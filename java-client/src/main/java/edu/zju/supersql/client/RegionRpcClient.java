package edu.zju.supersql.client;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.util.List;

/**
 * AutoCloseable Thrift client for RegionService.
 *
 * <p>Usage:
 * <pre>
 *   try (RegionRpcClient c = RegionRpcClient.fromInfo(rsInfo, 10000)) {
 *       QueryResult r = c.execute("orders", "select * from orders;");
 *   }
 * </pre>
 */
public class RegionRpcClient implements AutoCloseable {

    private final TFramedTransport transport;
    private final RegionService.Client client;

    public RegionRpcClient(String host, int port, int timeoutMs) throws Exception {
        TSocket socket = new TSocket(host, port, timeoutMs);
        transport = new TFramedTransport(socket);
        transport.open();
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        TMultiplexedProtocol mux = new TMultiplexedProtocol(protocol, "RegionService");
        client = new RegionService.Client(mux);
    }

    /**
     * Creates a client from a {@link RegionServerInfo} object.
     */
    public static RegionRpcClient fromInfo(RegionServerInfo info, int timeoutMs) throws Exception {
        return new RegionRpcClient(info.getHost(), info.getPort(), timeoutMs);
    }

    /**
     * Parses {@code host:port} and creates a client.
     */
    public static RegionRpcClient fromAddress(String hostPort, int timeoutMs) throws Exception {
        String[] parts = hostPort.split(":", 2);
        String host = parts[0];
        int port = Integer.parseInt(parts[1]);
        return new RegionRpcClient(host, port, timeoutMs);
    }

    public QueryResult execute(String tableName, String sql) throws Exception {
        return client.execute(tableName, sql);
    }

    public QueryResult executeBatch(String tableName, List<String> sqls) throws Exception {
        return client.executeBatch(tableName, sqls);
    }

    public Response createIndex(String tableName, String ddl) throws Exception {
        return client.createIndex(tableName, ddl);
    }

    public Response dropIndex(String tableName, String indexName) throws Exception {
        return client.dropIndex(tableName, indexName);
    }

    public Response ping() throws Exception {
        return client.ping();
    }

    @Override
    public void close() {
        if (transport != null && transport.isOpen()) {
            transport.close();
        }
    }
}
