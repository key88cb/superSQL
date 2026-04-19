package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Secondary-side WAL handler.
 *
 * <p>Receives WAL entries from the primary via {@link #syncLog}, stores them in an in-memory
 * ordered map, and applies them to the local MiniSQL engine on {@link #commitLog}.
 *
 * <p>The SQL payload is carried in {@link WalEntry#getAfterRow()} as UTF-8 bytes.
 */
public class ReplicaSyncServiceImpl implements ReplicaSyncService.Iface {

    private static final Logger log = LoggerFactory.getLogger(ReplicaSyncServiceImpl.class);
    private static final long DEFAULT_PREPARE_DECISION_TIMEOUT_MS =
            readLongEnv("RS_PREPARE_DECISION_TIMEOUT_MS", TimeUnit.MINUTES.toMillis(5));
    private static final long MIN_EPOCH_TXN_ID = 1_000_000_000_000L;

    // ── per-table in-memory WAL ────────────────────────────────────────────────
    static final Map<String, ConcurrentSkipListMap<Long, WalEntry>> WAL_BY_TABLE =
            new ConcurrentHashMap<>();
    static final Map<String, Set<Long>> COMMITTED_LSNS = new ConcurrentHashMap<>();
        static final Map<String, ConcurrentHashMap<Long, DecisionRecord>> FINAL_DECISIONS_BY_TABLE =
            new ConcurrentHashMap<>();

        private record DecisionRecord(boolean committed, String decisionId, long decidedAtMs) {
        }

    private final MiniSqlProcess miniSql;
    private final WalManager walManager;
    private final long prepareDecisionTimeoutMs;
    private final AtomicLong prepareResolutionRuns = new AtomicLong(0L);
    private final AtomicLong prepareResolutionExamined = new AtomicLong(0L);
    private final AtomicLong prepareResolutionAutoAborted = new AtomicLong(0L);
    private final AtomicLong prepareResolutionLastRunAtMs = new AtomicLong(0L);
    private final AtomicLong prepareResolutionLastAbortAtMs = new AtomicLong(0L);
    private final AtomicLong prepareResolutionLastAbortLsn = new AtomicLong(-1L);
    private volatile String prepareResolutionLastAbortTable = "";
    private volatile String prepareResolutionLastError = "";

    /**
     * Production constructor — replays committed entries on local miniSQL engine.
     */
    public ReplicaSyncServiceImpl(MiniSqlProcess miniSql, WalManager walManager) {
        this(miniSql, walManager, DEFAULT_PREPARE_DECISION_TIMEOUT_MS);
    }

    ReplicaSyncServiceImpl(MiniSqlProcess miniSql, WalManager walManager, long prepareDecisionTimeoutMs) {
        this.miniSql = miniSql;
        this.walManager = walManager;
        this.prepareDecisionTimeoutMs = Math.max(1_000L, prepareDecisionTimeoutMs);
    }

    /**
     * Restores committed and uncommitted logs from WAL files into the in-memory map.
     */
    public void init() {
        log.info("ReplicaSyncServiceImpl: restoring pending logs from WAL...");
        WAL_BY_TABLE.clear();
        COMMITTED_LSNS.clear();
        FINAL_DECISIONS_BY_TABLE.clear();
        
        File dir = new File(walManager.getWalDir());
        File[] files = dir.listFiles((d, name) -> name.endsWith(".wal"));
        if (files == null) return;

        for (File f : files) {
            String tableName = f.getName().replace(".wal", "");
            try {
                List<WalEntry> committed = walManager.readEntriesAfter(tableName, 0L);
                if (!committed.isEmpty()) {
                    ConcurrentSkipListMap<Long, WalEntry> tableMap = tableWal(tableName);
                    Set<Long> committedLsnSet = COMMITTED_LSNS.computeIfAbsent(tableName,
                            k -> ConcurrentHashMap.newKeySet());
                    for (WalEntry entry : committed) {
                        tableMap.put(entry.getLsn(), entry);
                        committedLsnSet.add(entry.getLsn());
                    }
                    log.info("Restored {} committed logs for table={}", committed.size(), tableName);
                }

                List<WalEntry> uncommitted = walManager.readUncommittedEntries(tableName);
                if (!uncommitted.isEmpty()) {
                    ConcurrentSkipListMap<Long, WalEntry> tableMap = tableWal(tableName);
                    for (WalEntry entry : uncommitted) {
                        tableMap.putIfAbsent(entry.getLsn(), entry);
                    }
                    log.info("Restored {} pending logs for table={}", uncommitted.size(), tableName);
                }

                Map<Long, Byte> statuses = walManager.readEntryStatuses(tableName);
                if (!statuses.isEmpty()) {
                    ConcurrentHashMap<Long, DecisionRecord> decisionMap = tableDecisions(tableName);
                    for (Map.Entry<Long, Byte> statusEntry : statuses.entrySet()) {
                        long lsn = statusEntry.getKey();
                        byte status = statusEntry.getValue();
                        if (status == 1) {
                            decisionMap.put(lsn,
                                    new DecisionRecord(true, "recovered-commit-" + tableName + "-" + lsn, System.currentTimeMillis()));
                        } else if (status == 2) {
                            decisionMap.put(lsn,
                                    new DecisionRecord(false, "recovered-abort-" + tableName + "-" + lsn, System.currentTimeMillis()));
                        }
                    }
                }
            } catch (IOException e) {
                log.error("Failed to restore logs for table={}: {}", tableName, e.getMessage());
            }
        }

        resolveTimedOutPreparesBestEffort();
    }

    /** Test-only: clears all in-memory state. */
    public static void resetForTests() {
        WAL_BY_TABLE.clear();
        COMMITTED_LSNS.clear();
        FINAL_DECISIONS_BY_TABLE.clear();
    }

    public void resolveTimedOutPreparesBestEffort() {
        resolveTimedOutPrepares(System.currentTimeMillis(), prepareDecisionTimeoutMs);
    }

    public Map<String, Object> getPrepareResolutionStats() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("timeoutMs", prepareDecisionTimeoutMs);
        stats.put("runs", prepareResolutionRuns.get());
        stats.put("examined", prepareResolutionExamined.get());
        stats.put("autoAborted", prepareResolutionAutoAborted.get());
        stats.put("lastRunAtMs", prepareResolutionLastRunAtMs.get());
        stats.put("lastAbortAtMs", prepareResolutionLastAbortAtMs.get());
        stats.put("lastAbortTable", prepareResolutionLastAbortTable);
        stats.put("lastAbortLsn", prepareResolutionLastAbortLsn.get());
        stats.put("lastError", prepareResolutionLastError);
        return stats;
    }

    void resolveTimedOutPrepares(long nowMs, long timeoutMs) {
        if (timeoutMs <= 0L) {
            return;
        }
        long cutoff = nowMs - timeoutMs;
        prepareResolutionRuns.incrementAndGet();
        prepareResolutionLastRunAtMs.set(nowMs);

        for (Map.Entry<String, ConcurrentSkipListMap<Long, WalEntry>> tableEntry : WAL_BY_TABLE.entrySet()) {
            String tableName = tableEntry.getKey();
            ConcurrentSkipListMap<Long, WalEntry> wal = tableEntry.getValue();
            if (wal == null || wal.isEmpty()) {
                continue;
            }

            Set<Long> committed = COMMITTED_LSNS.get(tableName);
            for (Map.Entry<Long, WalEntry> walEntry : wal.entrySet()) {
                Long lsn = walEntry.getKey();
                WalEntry entry = walEntry.getValue();
                if (lsn == null || entry == null) {
                    continue;
                }
                if (committed != null && committed.contains(lsn)) {
                    continue;
                }

                prepareResolutionExamined.incrementAndGet();
                long txnId = entry.getTxnId();
                if (txnId < MIN_EPOCH_TXN_ID || txnId > cutoff) {
                    continue;
                }

                try {
                    if (walManager != null) {
                        walManager.abort(tableName, lsn);
                    }
                    if (wal.remove(lsn, entry)) {
                        tableDecisions(tableName).put(lsn,
                                new DecisionRecord(false,
                                        "auto-timeout-abort-" + tableName + "-" + lsn,
                                        System.currentTimeMillis()));
                        prepareResolutionAutoAborted.incrementAndGet();
                        prepareResolutionLastAbortAtMs.set(System.currentTimeMillis());
                        prepareResolutionLastAbortLsn.set(lsn);
                        prepareResolutionLastAbortTable = tableName;
                    }
                } catch (Exception e) {
                    prepareResolutionLastError = e.getMessage() == null ? "unknown" : e.getMessage();
                    log.warn("resolveTimedOutPrepares failed table={} lsn={}: {}",
                            tableName, lsn, e.getMessage());
                }
            }
        }
    }

    // ─────────────────────── Thrift interface ─────────────────────────────────

    @Override
    public Response syncLog(WalEntry entry) throws TException {
        if (entry == null || !entry.isSetTableName() || !entry.isSetLsn()) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Invalid wal entry");
            return r;
        }

        DecisionRecord existingDecision = tableDecisions(entry.getTableName()).get(entry.getLsn());
        if (existingDecision != null) {
            if (!existingDecision.committed()) {
                if (isOverrideableAbortDecision(existingDecision)) {
                    tableDecisions(entry.getTableName()).remove(entry.getLsn(), existingDecision);
                    existingDecision = null;
                } else {
                    Response r = new Response(StatusCode.ERROR);
                    r.setMessage("Entry already finalized as ABORT for table="
                            + entry.getTableName() + " lsn=" + entry.getLsn());
                    return r;
                }
            }
            if (existingDecision != null) {
                Response r = new Response(StatusCode.OK);
                r.setMessage("ACK_ALREADY_DECIDED_COMMIT lsn=" + entry.getLsn());
                return r;
            }
        }

        // S4-04: Persist to disk before ACK
        if (walManager != null) {
            try {
                String sql = entry.isSetAfterRow() ? new String(entry.getAfterRow(), StandardCharsets.UTF_8) : "";
                walManager.appendEntry(entry.getTableName(), entry.getLsn(), entry.getTxnId(), entry.getOpType(), sql);
            } catch (IOException e) {
                log.error("syncLog: failed to persist lsn={} table={}: {}", entry.getLsn(), entry.getTableName(), e.getMessage());
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Persistence failed: " + e.getMessage());
                return r;
            }
        }

        tableWal(entry.getTableName()).put(entry.getLsn(), cloneEntry(entry));
        log.debug("syncLog: table={} lsn={}", entry.getTableName(), entry.getLsn());

        Response r = new Response(StatusCode.OK);
        r.setMessage("ACK lsn=" + entry.getLsn());
        return r;
    }

    @Override
    public List<WalEntry> pullLog(String tableName, long startLsn) throws TException {
        resolveTimedOutPreparesBestEffort();
        if (tableName == null || tableName.isBlank()) {
            return Collections.emptyList();
        }
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        Set<Long> committed = COMMITTED_LSNS.get(tableName);
        if (committed == null || committed.isEmpty()) {
            return Collections.emptyList();
        }
        if (wal == null || wal.isEmpty()) {
            return Collections.emptyList();
        }

        List<WalEntry> result = new ArrayList<>();
        for (WalEntry e : wal.tailMap(startLsn).values()) {
            if (!committed.contains(e.getLsn())) {
                continue;
            }
            result.add(cloneEntry(e));
        }
        result.sort(Comparator.comparingLong(WalEntry::getLsn));
        return result;
    }

    @Override
    public long getMaxLsn(String tableName) throws TException {
        resolveTimedOutPreparesBestEffort();
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        Set<Long> committed = COMMITTED_LSNS.get(tableName);
        if (committed == null || committed.isEmpty()) {
            return -1L;
        }
        if (wal == null || wal.isEmpty()) {
            return -1L;
        }
        for (Map.Entry<Long, WalEntry> entry : wal.descendingMap().entrySet()) {
            if (committed.contains(entry.getKey())) {
                return entry.getKey();
            }
        }
        return -1L;
    }

    @Override
    public Response commitLog(String tableName, long lsn) throws TException {
        LogDecisionState state = getLogDecisionState(tableName, lsn);
        if (state.isDecided() && state.isSetCommitted() && !state.isCommitted()) {
            String decisionId = state.getDecisionId();
            if (decisionId != null && decisionId.startsWith("auto-timeout-abort-")) {
                Response r = new Response(StatusCode.TABLE_NOT_FOUND);
                r.setMessage("No wal entry found for table=" + tableName + " lsn=" + lsn);
                return r;
            }
        }
        Response finalize = finalizeLogDecision(
                tableName,
                lsn,
                true,
                "legacy-commit-" + tableName + "-" + lsn,
                System.currentTimeMillis());
        if (finalize.getCode() == StatusCode.OK
                && finalize.getMessage() != null
                && finalize.getMessage().startsWith("DECIDED_COMMIT")) {
            finalize.setMessage("COMMITTED lsn=" + lsn);
        }
        return finalize;
    }

    @Override
    public Response finalizeLogDecision(String tableName,
                                        long lsn,
                                        boolean committed,
                                        String decisionId,
                                        long decidedAtMs) throws TException {
        if (tableName == null || tableName.isBlank()) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Invalid table name");
            return r;
        }
        String normalizedDecisionId =
                (decisionId == null || decisionId.isBlank())
                        ? "decision-" + tableName + "-" + lsn + "-" + (committed ? "commit" : "abort")
                        : decisionId;
        long decisionTs = decidedAtMs > 0L ? decidedAtMs : System.currentTimeMillis();

        ConcurrentHashMap<Long, DecisionRecord> decisions = tableDecisions(tableName);
        DecisionRecord existingDecision = decisions.get(lsn);
        if (existingDecision != null) {
            if (!existingDecision.committed() && committed && isOverrideableAbortDecision(existingDecision)) {
                decisions.remove(lsn, existingDecision);
                existingDecision = null;
            }
        }
        if (existingDecision != null) {
            if (existingDecision.committed() == committed) {
                Response r = new Response(StatusCode.OK);
                r.setMessage(committed
                        ? "ALREADY_COMMITTED lsn=" + lsn
                        : "ALREADY_ABORTED lsn=" + lsn);
                return r;
            }
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Decision conflict for table=" + tableName + " lsn=" + lsn
                    + ": existingCommitted=" + existingDecision.committed());
            return r;
        }

        Set<Long> committedLsns = COMMITTED_LSNS.computeIfAbsent(tableName, k -> ConcurrentHashMap.newKeySet());
        if (committed && committedLsns.contains(lsn)) {
            decisions.put(lsn, new DecisionRecord(true, normalizedDecisionId, decisionTs));
            Response r = new Response(StatusCode.OK);
            r.setMessage("ALREADY_COMMITTED lsn=" + lsn);
            return r;
        }

        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        WalEntry entry = wal == null ? null : wal.get(lsn);

        if (committed) {
            if (entry == null) {
                Response r = new Response(StatusCode.TABLE_NOT_FOUND);
                r.setMessage("No wal entry found for table=" + tableName + " lsn=" + lsn);
                return r;
            }

            if (!committedLsns.contains(lsn)) {
                if (miniSql != null && entry.isSetAfterRow()) {
                    String sql = new String(entry.getAfterRow(), StandardCharsets.UTF_8);
                    try {
                        miniSql.execute(sql);
                        log.debug("finalizeLogDecision(commit): replayed lsn={} sql={}", lsn, sql);
                    } catch (Exception e) {
                        log.error("finalizeLogDecision(commit): failed to replay lsn={} sql={}: {}", lsn, sql, e.getMessage());
                        Response r = new Response(StatusCode.ERROR);
                        r.setMessage("Replay failed: " + e.getMessage());
                        return r;
                    }
                }

                if (walManager != null) {
                    walManager.commit(tableName, lsn);
                }
                committedLsns.add(lsn);
            }

            decisions.put(lsn, new DecisionRecord(true, normalizedDecisionId, decisionTs));
            Response r = new Response(StatusCode.OK);
            r.setMessage("DECIDED_COMMIT lsn=" + lsn + " decisionId=" + normalizedDecisionId);
            return r;
        }

        if (committedLsns.contains(lsn)) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Cannot ABORT already committed log table=" + tableName + " lsn=" + lsn);
            return r;
        }

        if (entry != null && walManager != null) {
            try {
                walManager.abort(tableName, lsn);
            } catch (Exception e) {
                log.warn("finalizeLogDecision(abort): failed to persist ABORT table={} lsn={}: {}",
                        tableName, lsn, e.getMessage());
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Abort persistence failed: " + e.getMessage());
                return r;
            }
        }
        if (wal != null) {
            wal.remove(lsn);
        }
        decisions.put(lsn, new DecisionRecord(false, normalizedDecisionId, decisionTs));
        Response r = new Response(StatusCode.OK);
        r.setMessage("DECIDED_ABORT lsn=" + lsn + " decisionId=" + normalizedDecisionId);
        return r;
    }

    @Override
    public LogDecisionState getLogDecisionState(String tableName, long lsn) throws TException {
        LogDecisionState state = new LogDecisionState(false);
        if (tableName == null || tableName.isBlank()) {
            return state;
        }

        DecisionRecord decision = tableDecisions(tableName).get(lsn);
        if (decision != null) {
            state.setDecided(true);
            state.setCommitted(decision.committed());
            state.setDecisionId(decision.decisionId());
            state.setDecidedAtMs(decision.decidedAtMs());
            return state;
        }

        Set<Long> committed = COMMITTED_LSNS.get(tableName);
        if (committed != null && committed.contains(lsn)) {
            state.setDecided(true);
            state.setCommitted(true);
            state.setDecisionId("legacy-commit-" + tableName + "-" + lsn);
            state.setDecidedAtMs(0L);
            return state;
        }

        return state;
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private static ConcurrentSkipListMap<Long, WalEntry> tableWal(String tableName) {
        return WAL_BY_TABLE.computeIfAbsent(tableName, k -> new ConcurrentSkipListMap<>());
    }

    private static ConcurrentHashMap<Long, DecisionRecord> tableDecisions(String tableName) {
        return FINAL_DECISIONS_BY_TABLE.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>());
    }

    private static long readLongEnv(String key, long fallback) {
        String raw = System.getenv(key);
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static WalEntry cloneEntry(WalEntry entry) {
        return new WalEntry(entry);
    }

    private static boolean isOverrideableAbortDecision(DecisionRecord decision) {
        if (decision == null || decision.committed()) {
            return false;
        }
        String id = decision.decisionId();
        if (id == null || id.isBlank()) {
            return false;
        }
        return id.startsWith("auto-timeout-abort-") || id.startsWith("recovered-abort-");
    }
}
