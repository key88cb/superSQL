package edu.zju.supersql.master.rpc;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

/**
 * MasterService stub implementation.
 * All methods return an ERROR response until the real logic is implemented.
 */
public class MasterServiceImpl implements MasterService.Iface {

    private static final Logger log = LoggerFactory.getLogger(MasterServiceImpl.class);

    private static Response notImplemented(String method) {
        log.warn("MasterService.{} called — not yet implemented", method);
        Response r = new Response(StatusCode.ERROR);
        r.setMessage("Not implemented: " + method);
        return r;
    }

    @Override
    public TableLocation getTableLocation(String tableName) throws TException {
        // TODO Sprint 3: resolve table route from MetaManager and support NOT_LEADER redirect.
        log.warn("MasterService.getTableLocation called for '{}' — returning stub location", tableName);
        RegionServerInfo placeholderPrimary = new RegionServerInfo("stub-rs", "127.0.0.1", 9090);
        TableLocation location = new TableLocation(tableName, placeholderPrimary,
                Collections.singletonList(placeholderPrimary));
        location.setTableStatus("TODO_NOT_IMPLEMENTED");
        location.setVersion(0L);
        return location;
    }

    @Override
    public Response createTable(String ddl) throws TException {
        return notImplemented("createTable");
    }

    @Override
    public Response dropTable(String tableName) throws TException {
        return notImplemented("dropTable");
    }

    @Override
    public String getActiveMaster() throws TException {
        log.warn("MasterService.getActiveMaster called — not yet implemented");
        return "not-implemented:8080";
    }

    @Override
    public List<RegionServerInfo> listRegionServers() throws TException {
        log.warn("MasterService.listRegionServers called — not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public List<TableLocation> listTables() throws TException {
        log.warn("MasterService.listTables called — not yet implemented");
        return Collections.emptyList();
    }

    @Override
    public Response triggerRebalance() throws TException {
        return notImplemented("triggerRebalance");
    }
}
