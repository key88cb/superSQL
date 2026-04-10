package edu.zju.supersql.master.rpc;

import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.RegionService;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;

/**
 * Executes DDL against the RegionService endpoint of a RegionServer.
 */
public class RegionServiceDdlExecutor implements RegionDdlExecutor {

    private final int timeoutMs;

    public RegionServiceDdlExecutor(int timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public Response execute(RegionServerInfo regionServer, String tableName, String ddl) throws Exception {
        try (TFramedTransport transport = new TFramedTransport(
                new TSocket(regionServer.getHost(), regionServer.getPort(), timeoutMs))) {
            transport.open();
            TBinaryProtocol protocol = new TBinaryProtocol(transport);
            TMultiplexedProtocol mux = new TMultiplexedProtocol(protocol, "RegionService");
            RegionService.Client client = new RegionService.Client(mux);
            QueryResult result = client.execute(tableName, ddl);
            return result == null || !result.isSetStatus()
                    ? error("RegionService returned null status for ddl=" + ddl)
                    : result.getStatus();
        }
    }

    private static Response error(String message) {
        Response response = new Response(StatusCode.ERROR);
        response.setMessage(message);
        return response;
    }
}
