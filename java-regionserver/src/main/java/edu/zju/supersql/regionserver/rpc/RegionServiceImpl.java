package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * RegionService stub — all methods return ERROR until Sprint 2 implementation.
 */
public class RegionServiceImpl implements RegionService.Iface {

    private static final Logger log = LoggerFactory.getLogger(RegionServiceImpl.class);

    private static QueryResult notImplemented(String method) {
        log.warn("RegionService.{} called — not yet implemented", method);
        Response status = new Response(StatusCode.ERROR).setMessage("Not implemented: " + method);
        return new QueryResult(status);
    }

    private static Response notImplementedResp(String method) {
        log.warn("RegionService.{} called — not yet implemented", method);
        return new Response(StatusCode.ERROR).setMessage("Not implemented: " + method);
    }

    @Override
    public QueryResult execute(String tableName, String sql) throws TException {
        return notImplemented("execute");
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
        return new Response(StatusCode.OK).setMessage("pong");
    }
}
