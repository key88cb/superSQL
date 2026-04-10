package edu.zju.supersql.master.rpc;

import edu.zju.supersql.rpc.RegionAdminService;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;

/**
 * Thrift-backed RegionAdmin executor.
 */
public class ThriftRegionAdminExecutor implements RegionAdminExecutor {

    private final int timeoutMs;

    public ThriftRegionAdminExecutor(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) throws Exception {
        return withClient(regionServer, client -> client.pauseTableWrite(tableName));
    }

    @Override
    public Response resumeTableWrite(RegionServerInfo regionServer, String tableName) throws Exception {
        return withClient(regionServer, client -> client.resumeTableWrite(tableName));
    }

    @Override
    public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) throws Exception {
        return withClient(source, client -> client.transferTable(tableName, target.getHost(), target.getPort()));
    }

    @Override
    public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) throws Exception {
        return withClient(regionServer, client -> client.deleteLocalTable(tableName));
    }

    @Override
    public Response invalidateClientCache(RegionServerInfo regionServer, String tableName) throws Exception {
        return withClient(regionServer, client -> client.invalidateClientCache(tableName));
    }

    private Response withClient(RegionServerInfo regionServer, AdminCall call) throws Exception {
        try (TFramedTransport transport = new TFramedTransport(
                new TSocket(regionServer.getHost(), regionServer.getPort(), timeoutMs))) {
            transport.open();
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol mux = new TMultiplexedProtocol(protocol, "RegionAdminService");
            RegionAdminService.Client client = new RegionAdminService.Client(mux);
            return call.run(client);
        }
    }

    @FunctionalInterface
    private interface AdminCall {
        Response run(RegionAdminService.Client client) throws Exception;
    }
}
