package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.rpc.*;
import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.OutputParser;
import edu.zju.supersql.regionserver.WalManager;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * RegionService stub — all methods return ERROR until Sprint 2 implementation.
 */
public class RegionServiceImpl implements RegionService.Iface {

    private static final Logger log = LoggerFactory.getLogger(RegionServiceImpl.class);
    
    private final MiniSqlProcess miniSql;
    private final WalManager walManager;

    public RegionServiceImpl(MiniSqlProcess miniSql, WalManager walManager) {
        this.miniSql = miniSql;
        this.walManager = walManager;
    }

    private static QueryResult notImplemented(String method) {
        log.warn("RegionService.{} called — not yet implemented", method);
        Response status = new Response(StatusCode.ERROR);
        status.setMessage("Not implemented: " + method);
        return new QueryResult(status);
    }

    private static Response notImplementedResp(String method) {
        log.warn("RegionService.{} called — not yet implemented", method);
        Response r = new Response(StatusCode.ERROR);
        r.setMessage("Not implemented: " + method);
        return r;
    }

    @Override
    public QueryResult execute(String tableName, String sql) throws TException {
        log.info("RegionService.execute called for table {}: {}", tableName, sql);
        
        try {
            // 提交写操作计数（用于触发 S4-07 要求的 1000 条 Checkpoint）
            if (isWriteOperation(sql)) {
                log.debug("Detected write operation, incrementing log counter.");
                if (walManager.incrementWriteCount()) {
                    log.info("Log threshold reached. Triggering asynchronous checkpoint.");
                    // 异步触发一个 Checkpoint 任务
                    new Thread(() -> walManager.performCheckpoint(miniSql)).start();
                }
            }

            // 执行实际 SQL
            String result = miniSql.execute(sql);
            return OutputParser.parse(result);
        } catch (Exception e) {
            log.error("Internal error executing SQL on table {}: ", tableName, e);
            Response status = new Response(StatusCode.ERROR);
            status.setMessage("Engine error: " + e.getMessage());
            return new QueryResult(status);
        }
    }

    /**
     * 判断 SQL 是否为写操作 (INSERT/DELETE/UPDATE/CREATE/DROP)
     */
    private boolean isWriteOperation(String sql) {
        String s = sql.trim().toLowerCase();
        return s.startsWith("insert") || s.startsWith("delete") || s.startsWith("update") 
            || s.startsWith("create") || s.startsWith("drop") || s.startsWith("checkpoint");
    }

    @Override
    public QueryResult executeBatch(String tableName, List<String> sqls) throws TException {
        return notImplemented("executeBatch");
    }

    @Override
    public Response createIndex(String tableName, String ddl) throws TException {
        return notImplementedResp("createIndex");
    }

    @Override
    public Response dropIndex(String tableName, String indexName) throws TException {
        return notImplementedResp("dropIndex");
    }

    @Override
    public Response ping() throws TException {
        log.debug("RegionService.ping called");
        Response r = new Response(StatusCode.OK);
        r.setMessage("pong");
        return r;
    }
}
