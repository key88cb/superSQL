package edu.zju.supersql.regionserver.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.OutputParser;
import edu.zju.supersql.regionserver.ReplicaManager;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.rpc.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Primary-side SQL execution service.
 *
 * <p>Write path (INSERT / UPDATE / DELETE / CREATE / DROP):
 * <ol>
 *   <li>Check {@link WriteGuard}: return MOVING if table is paused.</li>
 *   <li>Append WAL entry (binary file) via {@link WalManager}.</li>
 *   <li>Sync entry to replica RegionServers via {@link ReplicaManager} (requires configured minimum ACKs).</li>
 *   <li>Execute SQL on local MiniSQL engine.</li>
 *   <li>Commit on replicas asynchronously.</li>
 * </ol>
 *
 * <p>Read path (SELECT): execute directly on local engine, no WAL.
 */
public class RegionServiceImpl implements RegionService.Iface {

    private static final Logger log = LoggerFactory.getLogger(RegionServiceImpl.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final AtomicLong EXECUTED_SQL_COUNT = new AtomicLong(0L);

    private final MiniSqlProcess miniSql;
    private final WalManager walManager;
    private final ReplicaManager replicaManager;
    private final WriteGuard writeGuard;
    private final CuratorFramework zkClient;
    /** This RS's own address (host:port) — excluded from replica list to avoid self-sync. */
    private final String selfAddress;
    private final int minReplicaAcks;
    /**
     * Engine data root (typically `/data/db`). Used by the BUG-17 post-DROP
     * cleanup path to remove `database/data/<table>` and matching index files
     * after the C++ engine drops the table — without this, replica RSs that
     * the C++ `remove(./database/data/<name>)` failed silently on (e.g. dirty
     * buffer-pool page held an fd) accumulate orphan data files that cause
     * `Table has existed!` on the next CREATE of the same name.
     * May be {@code null} in unit tests that wire RegionServiceImpl directly.
     */
    private final String dataRootDir;

    public RegionServiceImpl(MiniSqlProcess miniSql,
                             WalManager walManager,
                             ReplicaManager replicaManager,
                             WriteGuard writeGuard,
                             CuratorFramework zkClient,
                             String selfAddress) {
        this(miniSql, walManager, replicaManager, writeGuard, zkClient, selfAddress, 1, null);
    }

    public RegionServiceImpl(MiniSqlProcess miniSql,
                             WalManager walManager,
                             ReplicaManager replicaManager,
                             WriteGuard writeGuard,
                             CuratorFramework zkClient,
                             String selfAddress,
                             int minReplicaAcks) {
        this(miniSql, walManager, replicaManager, writeGuard, zkClient, selfAddress, minReplicaAcks, null);
    }

    public RegionServiceImpl(MiniSqlProcess miniSql,
                             WalManager walManager,
                             ReplicaManager replicaManager,
                             WriteGuard writeGuard,
                             CuratorFramework zkClient,
                             String selfAddress,
                             int minReplicaAcks,
                             String dataRootDir) {
        this.miniSql        = miniSql;
        this.walManager     = walManager;
        this.replicaManager = replicaManager;
        this.writeGuard     = writeGuard;
        this.zkClient       = zkClient;
        this.selfAddress    = selfAddress;
        this.minReplicaAcks = Math.max(0, minReplicaAcks);
        this.dataRootDir    = dataRootDir;
    }

    // ─────────────────────── RegionService.Iface ──────────────────────────────

    @Override
    public QueryResult execute(String tableName, String sql) throws TException {
        log.info("execute: table={} sql={}", tableName, sql);
        EXECUTED_SQL_COUNT.incrementAndGet();
        boolean isWrite = isWriteOperation(sql);

        // Check migration guard
        if (isWrite && writeGuard != null && writeGuard.isPaused(tableName)) {
            log.warn("execute: table {} is paused (migration in progress)", tableName);
            Response r = new Response(StatusCode.MOVING);
            r.setMessage("Table " + tableName + " is currently being migrated. Retry shortly.");
            return new QueryResult(r);
        }

        if (isWrite) {
            return executeWrite(tableName, sql);
        } else {
            return executeRead(sql);
        }
    }

    @Override
    public QueryResult executeBatch(String tableName, List<String> sqls) throws TException {
        if (sqls == null || sqls.isEmpty()) {
            Response r = new Response(StatusCode.OK);
            r.setMessage("No statements");
            return new QueryResult(r);
        }
        long totalAffected = 0L;
        QueryResult lastResult = null;
        for (String sql : sqls) {
            lastResult = execute(tableName, sql);
            if (lastResult.getStatus().getCode() != StatusCode.OK) {
                log.warn("executeBatch: statement failed: {} -> {}", sql,
                        lastResult.getStatus().getMessage());
                return lastResult;
            }
            if (lastResult.isSetAffectedRows()) {
                totalAffected += lastResult.getAffectedRows();
            }
        }
        if (lastResult == null) {
            lastResult = new QueryResult(new Response(StatusCode.OK));
        }
        lastResult.setAffectedRows(totalAffected);
        return lastResult;
    }

    @Override
    public Response createIndex(String tableName, String ddl) throws TException {
        log.info("createIndex: table={} ddl={}", tableName, ddl);
        try {
            QueryResult qr = execute(tableName, ddl);
            return qr.getStatus();
        } catch (Exception e) {
            log.error("createIndex failed", e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("createIndex failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response dropIndex(String tableName, String indexName) throws TException {
        log.info("dropIndex: table={} index={}", tableName, indexName);
        try {
            QueryResult qr = execute(tableName, "drop index " + indexName + ";");
            return qr.getStatus();
        } catch (Exception e) {
            log.error("dropIndex failed", e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("dropIndex failed: " + e.getMessage());
            return r;
        }
    }

    @Override
    public Response ping() throws TException {
        Response r = new Response(StatusCode.OK);
        r.setMessage("pong");
        return r;
    }

    // ─────────────────────── write path ───────────────────────────────────────

    private QueryResult executeWrite(String tableName, String sql) {
        try {
            // 1. WAL append
            long lsn   = walManager.nextLsn();
            long txnId = System.currentTimeMillis();
            WalOpType opType = classifyOpType(sql);
            walManager.appendEntry(tableName, lsn, txnId, opType, sql);

            // 2. Build WalEntry for replica sync (afterRow carries the SQL)
            WalEntry entry = new WalEntry(lsn, txnId, tableName, opType,
                    System.currentTimeMillis());
            entry.setAfterRow(sql.getBytes(StandardCharsets.UTF_8));

            // 3. Sync to replicas.
            //    CREATE TABLE / DROP TABLE are master-orchestrated: the master
            //    fans the DDL out to every replica directly (MasterServiceImpl
            //    iterates all selected replicas), so the per-RS replica-sync
            //    step is redundant and would deadlock on CREATE because
            //    /assignments/<table> is only written after the fan-out.
            boolean masterOrchestratedDdl =
                    opType == WalOpType.CREATE_TABLE || opType == WalOpType.DROP_TABLE;
            List<String> replicas = masterOrchestratedDdl
                    ? Collections.emptyList()
                    : getReplicaAddresses(tableName);
            int requiredAcks = masterOrchestratedDdl ? 0 : minReplicaAcks;
            if (replicas.size() < requiredAcks) {
                log.warn("executeWrite: insufficient replica targets lsn={} table={} required={} available={}",
                        lsn, tableName, requiredAcks, replicas.size());
                walManager.abort(tableName, lsn);
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Insufficient replica targets: required=" + requiredAcks
                        + ", available=" + replicas.size());
                return new QueryResult(r);
            }
            int acks = masterOrchestratedDdl
                    ? 0
                    : replicaManager.syncToReplicas(entry, replicas, requiredAcks);
            if (acks < requiredAcks) {
                log.warn("executeWrite: insufficient replica ACKs lsn={} table={} required={} actual={} replicas={}",
                        lsn, tableName, requiredAcks, acks, replicas.size());
                walManager.abort(tableName, lsn);
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("Insufficient replica ACKs: required=" + requiredAcks + ", actual=" + acks);
                return new QueryResult(r);
            }

            // 3.5. Execute on local engine first
            String raw = miniSql.execute(sql);
            QueryResult result = OutputParser.parse(raw);

            if (!result.isSetStatus() || result.getStatus().getCode() != StatusCode.OK) {
                walManager.abort(tableName, lsn);
                if (!replicas.isEmpty()) {
                    replicaManager.abortOnReplicas(tableName, lsn, replicas);
                }
                return result;
            }

            // 4. Commit WAL entry locally after local execution succeeds
            walManager.commit(tableName, lsn);

            // 4.5 BUG-17: belt-and-suspenders cleanup for DROP TABLE.
            // The C++ engine's record_manager calls `remove("./database/data/<name>")`
            // which can fail silently on a replica (dirty buffer-pool page holding
            // an fd, or stale catalog entry that short-circuits before the file
            // delete). Without this Java-side fallback the replica accumulates an
            // orphan data file; the next CREATE of the same name then fails with
            // "Table has existed!" on this replica, breaking the whole DDL fan-out.
            // We trigger a checkpoint first (flush catalog tombstone), then
            // best-effort delete `database/data/<table>` and any index file
            // whose name starts with the table name.
            if (opType == WalOpType.DROP_TABLE && dataRootDir != null) {
                try {
                    miniSql.checkpoint();
                } catch (Exception ce) {
                    log.warn("executeWrite: checkpoint after DROP TABLE failed table={} err={}",
                            tableName, ce.getMessage());
                }
                cleanupDroppedTableFiles(tableName);
            }

            // 5. Async commit on replicas
            if (!replicas.isEmpty()) {
                replicaManager.commitOnReplicas(tableName, lsn, replicas);
                replicaManager.reconcileReplicasAsync(tableName, lsn, replicas);
            }

            // 6. Checkpoint check
            if (walManager.incrementWriteCount()) {
                log.info("executeWrite: checkpoint threshold reached, triggering async checkpoint");
                new Thread(() -> walManager.performCheckpoint(miniSql),
                        "WAL-checkpoint").start();
            }

            return result;

        } catch (Exception e) {
            log.error("executeWrite failed for table={} sql={}", tableName, sql, e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Engine error: " + e.getMessage());
            return new QueryResult(r);
        }
    }

    private QueryResult executeRead(String sql) {
        try {
            return OutputParser.parse(miniSql.execute(sql));
        } catch (Exception e) {
            log.error("executeRead failed for sql={}", sql, e);
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Engine error: " + e.getMessage());
            return new QueryResult(r);
        }
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    /**
     * Returns the {@code host:port} addresses of the replicas for this table,
     * excluding this RS's own address to prevent self-sync.
     */
    private List<String> getReplicaAddresses(String tableName) {
        if (zkClient == null) {
            return Collections.emptyList();
        }
        try {
            String path = "/assignments/" + tableName;
            if (zkClient.checkExists().forPath(path) == null) {
                return Collections.emptyList();
            }
            byte[] bytes = zkClient.getData().forPath(path);
            if (bytes == null || bytes.length == 0) {
                return Collections.emptyList();
            }
            Map<?, ?> root = MAPPER.readValue(bytes, Map.class);
            List<?> replicasRaw = (List<?>) root.get("replicas");
            if (replicasRaw == null) {
                return Collections.emptyList();
            }
            List<String> addresses = new ArrayList<>();
            for (Object item : replicasRaw) {
                if (item instanceof Map<?, ?> rs) {
                    String host = String.valueOf(rs.get("host"));
                    Object portObj = rs.get("port");
                    if (portObj != null) {
                        String addr = host + ":" + portObj;
                        if (!addr.equals(selfAddress)) {
                            addresses.add(addr);
                        }
                    }
                }
            }
            return addresses;
        } catch (Exception e) {
            log.warn("getReplicaAddresses failed for table={}: {}", tableName, e.getMessage());
            return Collections.emptyList();
        }
    }

    private static boolean isWriteOperation(String sql) {
        if (sql == null) return false;
        String s = sql.trim().toLowerCase();
        return s.startsWith("insert") || s.startsWith("delete") || s.startsWith("update")
                || s.startsWith("create") || s.startsWith("drop")
                || s.startsWith("alter") || s.startsWith("truncate");
    }

    private static WalOpType classifyOpType(String sql) {
        String s = sql.trim().toLowerCase();
        if (s.startsWith("insert"))       return WalOpType.INSERT;
        if (s.startsWith("update"))       return WalOpType.UPDATE;
        if (s.startsWith("delete"))       return WalOpType.DELETE;
        if (s.startsWith("truncate"))     return WalOpType.DELETE;
        if (s.startsWith("alter"))        return WalOpType.UPDATE;
        if (s.startsWith("create table")) return WalOpType.CREATE_TABLE;
        if (s.startsWith("drop table"))   return WalOpType.DROP_TABLE;
        if (s.startsWith("create index")) return WalOpType.CREATE_INDEX;
        if (s.startsWith("drop index"))   return WalOpType.DROP_INDEX;
        return WalOpType.INSERT; // fallback
    }

    public static long getExecutedSqlCountForMetrics() {
        return EXECUTED_SQL_COUNT.get();
    }

    /**
     * BUG-17 fix: best-effort filesystem cleanup after a DROP TABLE.
     *
     * <p>Removes <code>database/data/&lt;table&gt;</code> and any index file in
     * <code>database/index/</code> whose name starts with <code>&lt;table&gt;</code>
     * (or <code>idx_&lt;table&gt;</code>). Idempotent — silent no-op if files
     * are already gone (the common case when the C++ engine successfully
     * deleted them).
     *
     * <p>Mirrors the file pattern used by
     * {@link RegionAdminServiceImpl#deleteLocalTable(String)} so admin-driven
     * cleanups and master-orchestrated DROPs converge on the same on-disk
     * state.
     */
    private void cleanupDroppedTableFiles(String tableName) {
        if (dataRootDir == null || tableName == null || tableName.isBlank()) {
            return;
        }
        try {
            File dataDir = new File(dataRootDir, "database/data");
            int dataDeleted = bestEffortDeleteMatching(dataDir, tableName);

            File indexDir = new File(dataRootDir, "database/index");
            int indexDeleted = bestEffortDeleteMatching(indexDir, tableName);

            if (dataDeleted + indexDeleted > 0) {
                log.info("executeWrite: cleaned up {} data file(s) + {} index file(s) after DROP TABLE table={}",
                        dataDeleted, indexDeleted, tableName);
            }
        } catch (Exception e) {
            // Cleanup must never break the DROP itself.
            log.warn("executeWrite: cleanup after DROP TABLE failed table={} err={}",
                    tableName, e.getMessage());
        }
    }

    private static int bestEffortDeleteMatching(File dir, String tableName) {
        if (dir == null || !dir.exists() || !dir.isDirectory()) {
            return 0;
        }
        File[] files = dir.listFiles(f -> {
            if (f == null || f.getName() == null) return false;
            String n = f.getName();
            // exact match (heap file) | "<table>_*" (per-engine secondary)
            // | "idx_<table>*" (CREATE INDEX default-style names)
            return n.equals(tableName)
                    || n.startsWith(tableName + "_")
                    || n.startsWith("idx_" + tableName);
        });
        if (files == null) return 0;
        int deleted = 0;
        for (File f : files) {
            if (f.delete()) {
                deleted++;
            }
        }
        return deleted;
    }
}
