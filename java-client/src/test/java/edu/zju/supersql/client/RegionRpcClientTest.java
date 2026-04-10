package edu.zju.supersql.client;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Integration test for RegionRpcClient using an embedded Thrift server.
 */
class RegionRpcClientTest {

    private TThreadPoolServer server;
    private ExecutorService serverPool;
    private int port;

    @BeforeEach
    void setUp() throws Exception {
        port = freePort();
        server = buildServer(port);
        serverPool = Executors.newSingleThreadExecutor();
        serverPool.submit(server::serve);
        Thread.sleep(200);
    }

    @AfterEach
    void tearDown() {
        if (server != null) server.stop();
        if (serverPool != null) serverPool.shutdownNow();
    }

    @Test
    void pingReturnsOk() throws Exception {
        try (RegionRpcClient client = new RegionRpcClient("127.0.0.1", port, 3000)) {
            Response r = client.ping();
            Assertions.assertEquals(StatusCode.OK, r.getCode());
            Assertions.assertEquals("pong", r.getMessage());
        }
    }

    @Test
    void executeReturnsQueryResult() throws Exception {
        try (RegionRpcClient client = RegionRpcClient.fromAddress("127.0.0.1:" + port, 3000)) {
            QueryResult result = client.execute("orders", "select * from orders;");
            Assertions.assertNotNull(result);
            Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        }
    }

    @Test
    void executeBatchReturnsOk() throws Exception {
        try (RegionRpcClient client = RegionRpcClient.fromAddress("127.0.0.1:" + port, 3000)) {
            QueryResult result = client.executeBatch("orders", List.of(
                    "insert into orders values(1);",
                    "insert into orders values(2);"
            ));
            Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        }
    }

    @Test
    void fromInfoCreatesClient() throws Exception {
        RegionServerInfo info = new RegionServerInfo("rs-1", "127.0.0.1", port);
        try (RegionRpcClient client = RegionRpcClient.fromInfo(info, 3000)) {
            Response r = client.ping();
            Assertions.assertEquals(StatusCode.OK, r.getCode());
        }
    }

    // ─────────────────────── embedded server ──────────────────────────────────

    private static TThreadPoolServer buildServer(int port) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionService", new RegionService.Processor<>(new StubRegionService()));
        TServerSocket transport = new TServerSocket(port);
        return new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** Minimal RegionService stub. */
    private static class StubRegionService implements RegionService.Iface {

        @Override
        public QueryResult execute(String tableName, String sql) throws TException {
            return new QueryResult(new Response(StatusCode.OK));
        }

        @Override
        public QueryResult executeBatch(String tableName, List<String> sqls) throws TException {
            return new QueryResult(new Response(StatusCode.OK));
        }

        @Override
        public Response createIndex(String tableName, String ddl) throws TException {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response dropIndex(String tableName, String indexName) throws TException {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response ping() throws TException {
            Response r = new Response(StatusCode.OK);
            r.setMessage("pong");
            return r;
        }
    }
}
