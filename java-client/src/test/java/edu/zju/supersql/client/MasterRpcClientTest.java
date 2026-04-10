package edu.zju.supersql.client;

import edu.zju.supersql.rpc.*;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TException;
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
 * Integration test for MasterRpcClient using an embedded Thrift server.
 */
class MasterRpcClientTest {

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
    void getTableLocationReturnsExpectedPrimaryRS() throws Exception {
        try (MasterRpcClient client = new MasterRpcClient("127.0.0.1", port, 3000)) {
            TableLocation loc = client.getTableLocation("orders");
            Assertions.assertNotNull(loc);
            Assertions.assertEquals("rs-1", loc.getPrimaryRS().getHost());
            Assertions.assertEquals(9090, loc.getPrimaryRS().getPort());
        }
    }

    @Test
    void createTableReturnsOk() throws Exception {
        try (MasterRpcClient client = MasterRpcClient.fromAddress("127.0.0.1:" + port, 3000)) {
            Response r = client.createTable("create table orders(id int, primary key(id));");
            Assertions.assertEquals(StatusCode.OK, r.getCode());
        }
    }

    @Test
    void dropTableReturnsOk() throws Exception {
        try (MasterRpcClient client = MasterRpcClient.fromAddress("127.0.0.1:" + port, 3000)) {
            Response r = client.dropTable("orders");
            Assertions.assertEquals(StatusCode.OK, r.getCode());
        }
    }

    @Test
    void listTablesReturnsNonNull() throws Exception {
        try (MasterRpcClient client = MasterRpcClient.fromAddress("127.0.0.1:" + port, 3000)) {
            List<TableLocation> tables = client.listTables();
            Assertions.assertNotNull(tables);
        }
    }

    @Test
    void listRegionServersReturnsNonNull() throws Exception {
        try (MasterRpcClient client = MasterRpcClient.fromAddress("127.0.0.1:" + port, 3000)) {
            List<RegionServerInfo> servers = client.listRegionServers();
            Assertions.assertNotNull(servers);
        }
    }

    // ─────────────────────── embedded server ──────────────────────────────────

    private static TThreadPoolServer buildServer(int port) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("MasterService", new MasterService.Processor<>(new StubMasterService()));
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

    /** Minimal MasterService stub that returns hardcoded responses. */
    private static class StubMasterService implements MasterService.Iface {

        @Override
        public TableLocation getTableLocation(String tableName) throws TException {
            RegionServerInfo rs = new RegionServerInfo("rs-1", "rs-1", 9090);
            TableLocation loc = new TableLocation(tableName, rs, List.of());
            loc.setVersion(1L);
            return loc;
        }

        @Override
        public Response createTable(String ddl) throws TException {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response dropTable(String tableName) throws TException {
            return new Response(StatusCode.OK);
        }

        @Override
        public String getActiveMaster() throws TException {
            return "master-1:8080";
        }

        @Override
        public List<RegionServerInfo> listRegionServers() throws TException {
            return List.of(new RegionServerInfo("rs-1", "rs-1", 9090));
        }

        @Override
        public List<TableLocation> listTables() throws TException {
            return List.of();
        }

        @Override
        public Response triggerRebalance() throws TException {
            return new Response(StatusCode.OK);
        }
    }
}
