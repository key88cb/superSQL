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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Primary-side SQL execution service.
 *
 * <p>Write path (INSERT / UPDATE / DELETE / CREATE / DROP):
 * <ol>
 *   <li>Check {@link WriteGuard}: return MOVING if table is paused.</li>
 *   <li>Append WAL entry (binary file) via {@link WalManager}.</li>
 *   <li>Sync entry to replica RegionServers via {@link ReplicaManager} (semi-sync: ≥1 ACK).</li>
 *   <li>Execute SQL on local MiniSQL engine.</li>
 *   <li>Commit on replicas asynchronously.</li>
 * </ol>
 *
 * <p>Read path (SELECT): execute directly on local engine, no WAL.
 */
public class RegionServiceImpl implements RegionService.Iface {

    private static final Logger log = LoggerFactory.getLogger(RegionServiceImpl.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final MiniSqlProcess miniSql;
    private final WalManager walManager;
    private final ReplicaManager replicaManager;
    private final WriteGuard writeGuard;
    private final CuratorFramework zkClient;
    /** This RS's own address (host:port) — excluded from replica list to avoid self-sync. */
    private final String selfAddress;

    public RegionServiceImpl(MiniSqlProcess miniSql,
                             WalManager walManager,
                             ReplicaManager replicaManager,
                             WriteGuard writeGuard,
                             CuratorFramework zkClient,
                             String selfAddress) {
        this.miniSql        = miniSql;
        this.walManager     = walManager;
        this.replicaManager = replicaManager;
        this.writeGuard     = writeGuard;
        this.zkClient       = zkClient;
        this.selfAddress    = selfAddress;
    }

    // ─────────────────────── RegionService.Iface ──────────────────────────────

    @Override
    public QueryResult execute(String tableName, String sql) throws TException {
        log.info("execute: table={} sql={}", tableName, sql);
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
            String raw = miniSql.execute(ddl);
            QueryResult qr = OutputParser.parse(raw);
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
            String raw = miniSql.execute("drop index " + indexName + ";");
            QueryResult qr = OutputParser.parse(raw);
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

            // 3. Sync to replicas (semi-sync: ≥1 ACK)
            List<String> replicas = getReplicaAddresses(tableName);
            int acks = replicaManager.syncToReplicas(entry, replicas);
            if (acks == 0 && !replicas.isEmpty()) {
                log.warn("executeWrite: no replica ACK for lsn={} table={} — proceeding anyway",
                        lsn, tableName);
            }

            // 4. Execute on local engine
            String raw = miniSql.execute(sql);
            QueryResult result = OutputParser.parse(raw);

            // 5. Async commit on replicas
            if (!replicas.isEmpty()) {
                replicaManager.commitOnReplicas(tableName, lsn, replicas);
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
    @SuppressWarnings("unchecked")
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
                || s.startsWith("create") || s.startsWith("drop");
    }

    private static WalOpType classifyOpType(String sql) {
        String s = sql.trim().toLowerCase();
        if (s.startsWith("insert"))       return WalOpType.INSERT;
        if (s.startsWith("update"))       return WalOpType.UPDATE;
        if (s.startsWith("delete"))       return WalOpType.DELETE;
        if (s.startsWith("create table")) return WalOpType.CREATE_TABLE;
        if (s.startsWith("drop table"))   return WalOpType.DROP_TABLE;
        if (s.startsWith("create index")) return WalOpType.CREATE_INDEX;
        if (s.startsWith("drop index"))   return WalOpType.DROP_INDEX;
        return WalOpType.INSERT; // fallback
    }
}
