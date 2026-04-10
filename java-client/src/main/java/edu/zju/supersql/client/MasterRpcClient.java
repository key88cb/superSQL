package edu.zju.supersql.client;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.util.List;

/**
 * AutoCloseable Thrift client for MasterService.
 *
 * <p>Usage:
 * <pre>
 *   try (MasterRpcClient c = MasterRpcClient.fromAddress("master-1:8080", 5000)) {
 *       TableLocation loc = c.getTableLocation("orders");
 *   }
 * </pre>
 */
public class MasterRpcClient implements AutoCloseable {

    private final TFramedTransport transport;
    private final MasterService.Client client;

    public MasterRpcClient(String host, int port, int timeoutMs) throws Exception {
        TSocket socket = new TSocket(host, port, timeoutMs);
        transport = new TFramedTransport(socket);
        transport.open();
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol mux = new TMultiplexedProtocol(protocol, "MasterService");
        client = new MasterService.Client(mux);
    }

    /**
     * Parses {@code host:port} and creates a client.
     */
    public static MasterRpcClient fromAddress(String hostPort, int timeoutMs) throws Exception {
        String[] parts = hostPort.split(":", 2);
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new MasterRpcClient(host, port, timeoutMs);
    }

    public TableLocation getTableLocation(String tableName) throws Exception {
        return client.getTableLocation(tableName);
    }

    public Response createTable(String ddl) throws Exception {
        return client.createTable(ddl);
    }

    public Response dropTable(String tableName) throws Exception {
        return client.dropTable(tableName);
    }

    public List<TableLocation> listTables() throws Exception {
        return client.listTables();
    }

    public List<RegionServerInfo> listRegionServers() throws Exception {
        return client.listRegionServers();
    }

    public String getActiveMaster() throws Exception {
        return client.getActiveMaster();
    }

    @Override
    public void close() {
        if (transport != null && transport.isOpen()) {
            transport.close();
        }
    }
}
