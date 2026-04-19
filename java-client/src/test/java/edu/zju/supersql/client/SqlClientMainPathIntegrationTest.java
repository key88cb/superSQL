package edu.zju.supersql.client;

import edu.zju.supersql.rpc.MasterService;
import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.RegionService;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.Row;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class SqlClientMainPathIntegrationTest {

    private final List<TThreadPoolServer> servers = new ArrayList<>();
    private final List<ExecutorService> pools = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (TThreadPoolServer server : servers) {
            server.stop();
        }
        for (ExecutorService pool : pools) {
            pool.shutdownNow();
        }
        SqlClient.resetRoutingMetricsForTests();
    }

    @Test
    void dmlRedirectShouldSelfHealByRefreshingRouteFromMaster() throws Exception {
        String table = "orders_redirect";

        int rs1Port = freePort();
        int rs2Port = freePort();
        RegionServerInfo rs1 = new RegionServerInfo("rs-1", "127.0.0.1", rs1Port);
        RegionServerInfo rs2 = new RegionServerInfo("rs-2", "127.0.0.1", rs2Port);

        AtomicReference<TableLocation> locationRef = new AtomicReference<>(
                tableLocation(table, rs1, List.of(rs1, rs2)));

        DynamicMasterService masterService = new DynamicMasterService(locationRef);
        int masterPort = startMasterServer(masterService);

        AtomicInteger rs1Calls = new AtomicInteger(0);
        AtomicInteger rs2Calls = new AtomicInteger(0);

        startRegionServer(rs1Port, new RegionService.Iface() {
            @Override
            public QueryResult execute(String tableName, String sql) {
                rs1Calls.incrementAndGet();
                locationRef.set(tableLocation(table, rs2, List.of(rs1, rs2)));
                return statusResult(StatusCode.REDIRECT, "redirect to new primary");
            }

            @Override
            public QueryResult executeBatch(String tableName, List<String> sqls) {
                return execute(tableName, sqls.get(0));
            }

            @Override
            public Response createIndex(String tableName, String ddl) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response dropIndex(String tableName, String indexName) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response ping() {
                return new Response(StatusCode.OK);
            }
        });

        startRegionServer(rs2Port, new RegionService.Iface() {
            @Override
            public QueryResult execute(String tableName, String sql) {
                rs2Calls.incrementAndGet();
                return statusResult(StatusCode.OK, "ok");
            }

            @Override
            public QueryResult executeBatch(String tableName, List<String> sqls) {
                return execute(tableName, sqls.get(0));
            }

            @Override
            public Response createIndex(String tableName, String ddl) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response dropIndex(String tableName, String indexName) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response ping() {
                return new Response(StatusCode.OK);
            }
        });

        RouteCache cache = new RouteCache(30_000);
        ClientConfig config = configFor("127.0.0.1:" + masterPort, 3, 10, 10, ClientConfig.ReadConsistency.STRONG);

        QueryResult result = SqlClient.executeDmlWithRetry(
                table,
                "insert into orders_redirect values(1);",
                "127.0.0.1:" + masterPort,
                cache,
                config,
                this::resolveViaMaster,
                this::openRealRegionSession,
                ms -> { }
        );

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(1, rs1Calls.get());
        Assertions.assertEquals(1, rs2Calls.get());
        Assertions.assertEquals("rs-2", cache.get(table).getPrimaryRS().getId());
    }

    @Test
    void dmlMovingShouldRetryAndEventuallySucceedOnSamePrimary() throws Exception {
        String table = "orders_moving";

        int rs1Port = freePort();
        RegionServerInfo rs1 = new RegionServerInfo("rs-1", "127.0.0.1", rs1Port);
        AtomicReference<TableLocation> locationRef = new AtomicReference<>(
                tableLocation(table, rs1, List.of(rs1)));

        int masterPort = startMasterServer(new DynamicMasterService(locationRef));

        AtomicInteger calls = new AtomicInteger(0);
        startRegionServer(rs1Port, new RegionService.Iface() {
            @Override
            public QueryResult execute(String tableName, String sql) {
                int call = calls.incrementAndGet();
                if (call <= 2) {
                    return statusResult(StatusCode.MOVING, "moving");
                }
                return statusResult(StatusCode.OK, "ok");
            }

            @Override
            public QueryResult executeBatch(String tableName, List<String> sqls) {
                return execute(tableName, sqls.get(0));
            }

            @Override
            public Response createIndex(String tableName, String ddl) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response dropIndex(String tableName, String indexName) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response ping() {
                return new Response(StatusCode.OK);
            }
        });

        RouteCache cache = new RouteCache(30_000);
        ClientConfig config = configFor("127.0.0.1:" + masterPort, 5, 100, 50, ClientConfig.ReadConsistency.STRONG);
        List<Long> sleeps = new CopyOnWriteArrayList<>();

        QueryResult result = SqlClient.executeDmlWithRetry(
                table,
                "insert into orders_moving values(1);",
                "127.0.0.1:" + masterPort,
                cache,
                config,
                this::resolveViaMaster,
                this::openRealRegionSession,
                sleeps::add
        );

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(3, calls.get());
        Assertions.assertEquals(List.of(100L, 150L), sleeps);
    }

    @Test
    void selectShouldFallbackToReplicaWhenPrimaryUnavailableInEventualMode() throws Exception {
        String table = "orders_read_fallback";

        int primaryDownPort = freePort();
        int replicaPort = freePort();
        RegionServerInfo primary = new RegionServerInfo("rs-down", "127.0.0.1", primaryDownPort);
        RegionServerInfo replica = new RegionServerInfo("rs-2", "127.0.0.1", replicaPort);
        AtomicReference<TableLocation> locationRef = new AtomicReference<>(
                tableLocation(table, primary, List.of(primary, replica)));

        int masterPort = startMasterServer(new DynamicMasterService(locationRef));

        startRegionServer(replicaPort, new RegionService.Iface() {
            @Override
            public QueryResult execute(String tableName, String sql) {
                QueryResult result = statusResult(StatusCode.OK, "ok");
                result.setColumnNames(List.of("id"));
                result.setRows(List.of(new Row(List.of("1"))));
                return result;
            }

            @Override
            public QueryResult executeBatch(String tableName, List<String> sqls) {
                return execute(tableName, sqls.get(0));
            }

            @Override
            public Response createIndex(String tableName, String ddl) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response dropIndex(String tableName, String indexName) {
                return new Response(StatusCode.OK);
            }

            @Override
            public Response ping() {
                return new Response(StatusCode.OK);
            }
        });

        RouteCache cache = new RouteCache(30_000);
        ClientConfig config = configFor("127.0.0.1:" + masterPort, 3, 10, 10, ClientConfig.ReadConsistency.EVENTUAL);

        QueryResult result = SqlClient.executeDmlWithRetry(
                table,
                "select * from orders_read_fallback where id = 1;",
                "127.0.0.1:" + masterPort,
                cache,
                config,
                this::resolveViaMaster,
                this::openRealRegionSession,
                ms -> { }
        );

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertTrue(result.isSetRows());
        Assertions.assertEquals("1", result.getRows().get(0).getValues().get(0));

        Map<String, ClientRoutingMetrics.MetricsSnapshot> metrics = SqlClient.snapshotRoutingMetrics();
        Assertions.assertTrue(metrics.containsKey(table));
        Assertions.assertTrue(metrics.get(table).readFallbackCount() >= 1);
    }

    private int startMasterServer(MasterService.Iface masterService) throws Exception {
        int port = freePort();
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("MasterService", new MasterService.Processor<>(masterService));
        TServerSocket transport = new TServerSocket(port);
        TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
        servers.add(server);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        pools.add(pool);
        pool.submit(server::serve);
        Thread.sleep(150);
        return port;
    }

    private void startRegionServer(int port, RegionService.Iface regionService) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionService", new RegionService.Processor<>(regionService));
        TServerSocket transport = new TServerSocket(port);
        TThreadPoolServer server = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
        servers.add(server);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        pools.add(pool);
        pool.submit(server::serve);
        Thread.sleep(150);
    }

    private TableLocation resolveViaMaster(String tableName,
                                           String activeMaster,
                                           RouteCache routeCache,
                                           ClientConfig config) throws Exception {
        TableLocation cached = routeCache.get(tableName);
        if (cached != null) {
            return cached;
        }
        try (MasterRpcClient master = MasterRpcClient.fromAddress(activeMaster, config.masterRpcTimeoutMs())) {
            TableLocation location = master.getTableLocation(tableName);
            routeCache.put(tableName, location);
            return location;
        }
    }

    private SqlClient.RegionClientSession openRealRegionSession(RegionServerInfo target,
                                                                 ClientConfig config) throws Exception {
        RegionRpcClient client = RegionRpcClient.fromInfo(target, config.regionRpcTimeoutMs());
        return new SqlClient.RegionClientSession() {
            @Override
            public QueryResult execute(String tableName, String sql) throws Exception {
                return client.execute(tableName, sql);
            }

            @Override
            public QueryResult executeBatch(String tableName, List<String> sqls) throws Exception {
                return client.executeBatch(tableName, sqls);
            }

            @Override
            public void close() {
                client.close();
            }
        };
    }

    private static QueryResult statusResult(StatusCode code, String message) {
        Response status = new Response(code);
        status.setMessage(message);
        return new QueryResult(status);
    }

    private static ClientConfig configFor(String masterAddress,
                                          int maxAttempts,
                                          int initialBackoffMs,
                                          int stepBackoffMs,
                                          ClientConfig.ReadConsistency readConsistency) {
        return new ClientConfig(
                "unused-zk",
                masterAddress,
                30_000,
                3_000,
                3_000,
                maxAttempts,
                initialBackoffMs,
                stepBackoffMs,
                readConsistency
        );
    }

    private static TableLocation tableLocation(String table,
                                               RegionServerInfo primary,
                                               List<RegionServerInfo> replicas) {
        TableLocation location = new TableLocation(table, primary, replicas);
        location.setVersion(System.currentTimeMillis());
        location.setTableStatus("ACTIVE");
        return location;
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class DynamicMasterService implements MasterService.Iface {
        private final AtomicReference<TableLocation> locationRef;

        private DynamicMasterService(AtomicReference<TableLocation> locationRef) {
            this.locationRef = locationRef;
        }

        @Override
        public TableLocation getTableLocation(String tableName) {
            TableLocation current = locationRef.get();
            if (current == null) {
                return null;
            }
            return tableLocation(
                    current.getTableName(),
                    current.getPrimaryRS(),
                    new ArrayList<>(current.getReplicas()));
        }

        @Override
        public Response createTable(String ddl) {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response dropTable(String tableName) {
            return new Response(StatusCode.OK);
        }

        @Override
        public String getActiveMaster() {
            return "master-1:8080";
        }

        @Override
        public List<RegionServerInfo> listRegionServers() {
            TableLocation current = locationRef.get();
            if (current == null || !current.isSetReplicas()) {
                return List.of();
            }
            return current.getReplicas();
        }

        @Override
        public List<TableLocation> listTables() {
            TableLocation current = locationRef.get();
            return current == null ? List.of() : List.of(current);
        }

        @Override
        public Response triggerRebalance() throws TException {
            return new Response(StatusCode.OK);
        }
    }
}
