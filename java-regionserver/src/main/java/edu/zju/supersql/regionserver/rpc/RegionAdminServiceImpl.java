package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RegionAdminService stub — all methods return ERROR until Sprint 3 implementation.
 */
public class RegionAdminServiceImpl implements RegionAdminService.Iface {

    private static final Logger log = LoggerFactory.getLogger(RegionAdminServiceImpl.class);

    private static Response notImplemented(String method) {
        log.warn("RegionAdminService.{} called — not yet implemented", method);
        Response r = new Response(StatusCode.ERROR);
        r.setMessage("Not implemented: " + method);
        return r;
    }

    @Override
    public Response pauseTableWrite(String tableName) throws TException {
        return notImplemented("pauseTableWrite");
    }

    @Override
    public Response resumeTableWrite(String tableName) throws TException {
        return notImplemented("resumeTableWrite");
    }

    @Override
    public Response transferTable(String tableName, String targetHost, int targetPort) throws TException {
        return notImplemented("transferTable");
    }

    @Override
    public Response copyTableData(DataChunk chunk) throws TException {
        return notImplemented("copyTableData");
    }

    @Override
    public Response deleteLocalTable(String tableName) throws TException {
        return notImplemented("deleteLocalTable");
    }

    @Override
    public Response registerRegionServer(RegionServerInfo info) throws TException {
        log.info("RegionAdminService.registerRegionServer called by {} — not yet implemented", info.getId());
        Response r = new Response(StatusCode.OK);
        r.setMessage("registered (stub)");
        return r;
    }

    @Override
    public Response heartbeat(RegionServerInfo info) throws TException {
        log.debug("RegionAdminService.heartbeat from {}", info.getId());
        return new Response(StatusCode.OK);
    }

    @Override
    public Response invalidateClientCache(String tableName) throws TException {
        return notImplemented("invalidateClientCache");
    }
}
