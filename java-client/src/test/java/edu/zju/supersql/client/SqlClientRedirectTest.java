package edu.zju.supersql.client;

import edu.zju.supersql.rpc.MasterService;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class SqlClientRedirectTest {

    private TThreadPoolServer standbyServer;
    private TThreadPoolServer activeServer;
    private ExecutorService pool;
    private int standbyPort;
    private int activePort;
    private ClientConfig config;

    @BeforeEach
    void setUp() throws Exception {
        pool = Executors.newFixedThreadPool(2);

        activePort = startServer(new ActiveMasterService());
        standbyPort = startServer(new StandbyMasterService("127.0.0.1:" + activePort));
        config = ClientConfig.fromEnv(java.util.Map.of(
                "CLIENT_MASTER_RPC_TIMEOUT_MS", "3000",
                "CLIENT_RS_RPC_TIMEOUT_MS", "3000"
        ));
    }

    @AfterEach
    void tearDown() {
        if (standbyServer != null) standbyServer.stop();
        if (activeServer != null) activeServer.stop();
        if (pool != null) pool.shutdownNow();
    }

    @Test
    void invokeMasterResponseWithRedirectShouldFollowNotLeader() throws Exception {
        Response response = SqlClient.invokeMasterResponseWithRedirect(
                "127.0.0.1:" + standbyPort,
                config,
                master -> master.createTable("create table orders(id int, primary key(id));"));

        Assertions.assertEquals(StatusCode.OK, response.getCode());
        Assertions.assertEquals("created on leader", response.getMessage());
    }

    @Test
    void fetchTableLocationWithRedirectShouldFollowStandbyPlaceholder() throws Exception {
        TableLocation location = SqlClient.fetchTableLocationWithRedirect(
                "127.0.0.1:" + standbyPort,
                "orders",
                config);

        Assertions.assertEquals("orders", location.getTableName());
        Assertions.assertEquals("rs-1", location.getPrimaryRS().getId());
    }

    private int startServer(MasterService.Iface iface) throws Exception {
        ServerHandle handle = buildServer(iface);
        TThreadPoolServer server = handle.server();
        int port = handle.port();
        if (iface instanceof ActiveMasterService) {
            activeServer = server;
        } else {
            standbyServer = server;
        }
        pool.submit(server::serve);
        Thread.sleep(150);
        return port;
    }

    private static ServerHandle buildServer(MasterService.Iface iface) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("MasterService", new MasterService.Processor<>(iface));

        TTransportException last = null;
        for (int i = 0; i < 8; i++) {
            try {
                int port = freePort();
                TServerSocket transport = new TServerSocket(port);
                TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                        .processor(processor)
                        .transportFactory(new TFramedTransport.Factory())
                        .minWorkerThreads(1)
                        .maxWorkerThreads(4));
                return new ServerHandle(server, port);
            } catch (TTransportException e) {
                last = e;
            }
        }
        throw last == null ? new IllegalStateException("Failed to allocate test master port") : last;
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private record ServerHandle(TThreadPoolServer server, int port) {
    }

    private static class ActiveMasterService implements MasterService.Iface {

        @Override
        public TableLocation getTableLocation(String tableName) {
            RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
            return new TableLocation(tableName, primary, List.of(primary));
        }

        @Override
        public Response createTable(String ddl) {
            Response response = new Response(StatusCode.OK);
            response.setMessage("created on leader");
            return response;
        }

        @Override
        public Response dropTable(String tableName) {
            return new Response(StatusCode.OK);
        }

        @Override
        public String getActiveMaster() {
            return "leader";
        }

        @Override
        public List<RegionServerInfo> listRegionServers() {
            return List.of();
        }

        @Override
        public List<TableLocation> listTables() {
            return List.of();
        }

        @Override
        public Response triggerRebalance() {
            return new Response(StatusCode.OK);
        }
    }

    private static class StandbyMasterService implements MasterService.Iface {

        private final String redirectTo;

        private StandbyMasterService(String redirectTo) {
            this.redirectTo = redirectTo;
        }

        @Override
        public TableLocation getTableLocation(String tableName) {
            RegionServerInfo placeholder = new RegionServerInfo("redirect", redirectTo, 0);
            TableLocation location = new TableLocation(tableName, placeholder, List.of(placeholder));
            location.setTableStatus("NOT_LEADER");
            location.setVersion(-1L);
            return location;
        }

        @Override
        public Response createTable(String ddl) {
            Response response = new Response(StatusCode.NOT_LEADER);
            response.setRedirectTo(redirectTo);
            response.setMessage("not leader");
            return response;
        }

        @Override
        public Response dropTable(String tableName) {
            Response response = new Response(StatusCode.NOT_LEADER);
            response.setRedirectTo(redirectTo);
            return response;
        }

        @Override
        public String getActiveMaster() {
            return redirectTo;
        }

        @Override
        public List<RegionServerInfo> listRegionServers() {
            return List.of();
        }

        @Override
        public List<TableLocation> listTables() {
            return List.of();
        }

        @Override
        public Response triggerRebalance() {
            return new Response(StatusCode.NOT_LEADER);
        }
    }
}
