package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * ReplicaSyncService stub — all methods return ERROR/defaults until Sprint 3 implementation.
 */
public class ReplicaSyncServiceImpl implements ReplicaSyncService.Iface {

    private static final Logger log = LoggerFactory.getLogger(ReplicaSyncServiceImpl.class);

    @Override
    public Response syncLog(WalEntry entry) throws TException {
        log.warn("ReplicaSyncService.syncLog called — not yet implemented");
        return new Response(StatusCode.ERROR).setMessage("Not implemented: syncLog");
    }

    @Override
    public List<WalEntry> pullLog(String tableName, long startLsn) throws TException {
        log.warn("ReplicaSyncService.pullLog called — not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public long getMaxLsn(String tableName) throws TException {
        log.warn("ReplicaSyncService.getMaxLsn called — not yet implemented");
        return -1L;
    }

    @Override
    public Response commitLog(String tableName, long lsn) throws TException {
        log.warn("ReplicaSyncService.commitLog called — not yet implemented");
        return new Response(StatusCode.ERROR).setMessage("Not implemented: commitLog");
    }
}
