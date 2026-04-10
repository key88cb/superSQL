package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

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

    // ── per-table in-memory WAL ────────────────────────────────────────────────
    static final Map<String, ConcurrentSkipListMap<Long, WalEntry>> WAL_BY_TABLE =
            new ConcurrentHashMap<>();
    static final Map<String, Set<Long>> COMMITTED_LSNS = new ConcurrentHashMap<>();

    private final MiniSqlProcess miniSql;

    /**
     * Production constructor — replays committed entries on local miniSQL engine.
     */
    public ReplicaSyncServiceImpl(MiniSqlProcess miniSql) {
        this.miniSql = miniSql;
    }

    /** Test-only: clears all in-memory state. */
    public static void resetForTests() {
        WAL_BY_TABLE.clear();
        COMMITTED_LSNS.clear();
    }

    // ─────────────────────── Thrift interface ─────────────────────────────────

    @Override
    public Response syncLog(WalEntry entry) throws TException {
        if (entry == null || !entry.isSetTableName() || !entry.isSetLsn()) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Invalid wal entry");
            return r;
        }

        tableWal(entry.getTableName()).put(entry.getLsn(), cloneEntry(entry));
        log.debug("syncLog: table={} lsn={}", entry.getTableName(), entry.getLsn());

        Response r = new Response(StatusCode.OK);
        r.setMessage("ACK lsn=" + entry.getLsn());
        return r;
    }

    @Override
    public List<WalEntry> pullLog(String tableName, long startLsn) throws TException {
        if (tableName == null || tableName.isBlank()) {
            return Collections.emptyList();
        }
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        if (wal == null || wal.isEmpty()) {
            return Collections.emptyList();
        }

        List<WalEntry> result = new ArrayList<>();
        for (WalEntry e : wal.tailMap(startLsn).values()) {
            result.add(cloneEntry(e));
        }
        result.sort(Comparator.comparingLong(WalEntry::getLsn));
        return result;
    }

    @Override
    public long getMaxLsn(String tableName) throws TException {
        ConcurrentSkipListMap<Long, WalEntry> wal = WAL_BY_TABLE.get(tableName);
        if (wal == null || wal.isEmpty()) {
            return -1L;
        }
        return wal.lastKey();
    }

    @Override
    public Response commitLog(String tableName, long lsn) throws TException {
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

        COMMITTED_LSNS.computeIfAbsent(tableName, k -> ConcurrentHashMap.newKeySet()).add(lsn);

        Response r = new Response(StatusCode.OK);
        r.setMessage("COMMITTED lsn=" + lsn);
        return r;
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private static ConcurrentSkipListMap<Long, WalEntry> tableWal(String tableName) {
        return WAL_BY_TABLE.computeIfAbsent(tableName, k -> new ConcurrentSkipListMap<>());
    }

    private static WalEntry cloneEntry(WalEntry entry) {
        return new WalEntry(entry);
    }
}
