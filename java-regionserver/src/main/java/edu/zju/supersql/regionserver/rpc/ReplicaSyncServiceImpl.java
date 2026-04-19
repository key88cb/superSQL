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
        resolveTimedOutPreparesBestEffort();
        Set<Long> committed = COMMITTED_LSNS.computeIfAbsent(tableName, k -> ConcurrentHashMap.newKeySet());
        if (committed.contains(lsn)) {
            Response r = new Response(StatusCode.OK);
            r.setMessage("ALREADY_COMMITTED lsn=" + lsn);
            return r;
        }

        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        if (wal == null || !wal.containsKey(lsn)) {
            Response r = new Response(StatusCode.TABLE_NOT_FOUND);
            r.setMessage("No wal entry found for table=" + tableName + " lsn=" + lsn);
            return r;
        }

        WalEntry entry = wal.get(lsn);

        // Replay SQL on local engine (write-through to secondary's miniSQL)
        if (miniSql != null && entry.isSetAfterRow()) {
            String sql = new String(entry.getAfterRow(), StandardCharsets.UTF_8);
            try {
                miniSql.execute(sql);
                log.debug("commitLog: replayed lsn={} sql={}", lsn, sql);
            } catch (Exception e) {
                log.error("commitLog: failed to replay lsn={} sql={}: {}", lsn, sql, e.getMessage());
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Replay failed: " + e.getMessage());
                return r;
            }
        }

        // S4-04: Mark as COMMITTED on disk
        if (walManager != null) {
            walManager.commit(tableName, lsn);
        }

        committed.add(lsn);

        Response r = new Response(StatusCode.OK);
        r.setMessage("COMMITTED lsn=" + lsn);
        return r;
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private static ConcurrentSkipListMap<Long, WalEntry> tableWal(String tableName) {
        return WAL_BY_TABLE.computeIfAbsent(tableName, k -> new ConcurrentSkipListMap<>());
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
}
