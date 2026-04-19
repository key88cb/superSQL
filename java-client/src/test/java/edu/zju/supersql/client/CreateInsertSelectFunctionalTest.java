package edu.zju.supersql.client;

import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.master.balance.LoadBalancer;
import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.master.rpc.MasterServiceImpl;
import edu.zju.supersql.master.rpc.RegionAdminExecutor;
import edu.zju.supersql.master.rpc.RegionServiceDdlExecutor;
import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.ReplicaManager;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.regionserver.rpc.RegionServiceImpl;
import edu.zju.supersql.rpc.MasterService;
import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.RegionService;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.Row;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class CreateInsertSelectFunctionalTest {

    @TempDir
    Path tempDir;

    private EmbeddedZkServer embeddedZk;
    private CuratorFramework zkClient;
    private ExecutorService rpcPool;
    private TThreadPoolServer regionServer;
    private TThreadPoolServer masterServer;
    private int regionPort;
    private int masterPort;

    @BeforeEach
    void setUp() throws Exception {
        embeddedZk = EmbeddedZkServerFactory.create();
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(embeddedZk.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(200, 3))
                .sessionTimeoutMs(10_000)
                .connectionTimeoutMs(5_000)
                .namespace("supersql")
                .build();
        zkClient.start();
        zkClient.blockUntilConnected();

        createIfMissing("/region_servers");
        createIfMissing("/meta/tables");
        createIfMissing("/assignments");
        createIfMissing("/active-master");

        rpcPool = Executors.newCachedThreadPool();

        startRegionServer();
        registerRegionServer("rs-1", regionPort);

        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
        writeActiveMaster("master-1", "127.0.0.1:" + masterPortPlaceholder());
        startMasterServer();
        writeActiveMaster("master-1", "127.0.0.1:" + masterPort);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (masterServer != null) masterServer.stop();
        if (regionServer != null) regionServer.stop();
        if (rpcPool != null) rpcPool.shutdownNow();
        if (zkClient != null) zkClient.close();
        if (embeddedZk != null) embeddedZk.close();
    }

    @Test
    void createInsertSelectShouldWorkThroughRealMasterAndRegionRpc() throws Exception {
        try (MasterRpcClient master = MasterRpcClient.fromAddress("127.0.0.1:" + masterPort, 3_000)) {
            Response create = master.createTable("create table orders(id int, primary key(id));");
            Assertions.assertEquals(StatusCode.OK, create.getCode());

            TableLocation location = master.getTableLocation("orders");
            Assertions.assertEquals("orders", location.getTableName());
            Assertions.assertEquals("rs-1", location.getPrimaryRS().getId());

            try (RegionRpcClient region = RegionRpcClient.fromInfo(location.getPrimaryRS(), 3_000)) {
                QueryResult insert = region.execute("orders", "insert into orders values(1);");
                Assertions.assertEquals(StatusCode.OK, insert.getStatus().getCode());

                QueryResult select = region.execute("orders", "select * from orders where id = 1;");
                Assertions.assertEquals(StatusCode.OK, select.getStatus().getCode());
                Assertions.assertTrue(select.isSetRows());
                Assertions.assertTrue(select.getRowsSize() >= 1);
                Assertions.assertTrue(select.getRows().stream().anyMatch(row -> List.of("1").equals(row.getValues())));
            }
        }
    }

    private void startRegionServer() throws Exception {
        regionPort = freePort();
        RecordingMiniSqlProcess miniSql = new RecordingMiniSqlProcess();
        WalManager walManager = new WalManager(tempDir.resolve("wal").toString());
        walManager.init();
        RegionServiceImpl regionService = new RegionServiceImpl(
                miniSql,
                walManager,
                new ReplicaManager(),
                new WriteGuard(),
                zkClient,
            "127.0.0.1:" + regionPort,
            0
        );

        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionService", new RegionService.Processor<>(regionService));
        TServerSocket transport = new TServerSocket(regionPort);
        regionServer = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
        rpcPool.submit(regionServer::serve);
        Thread.sleep(150);
    }

    private void startMasterServer() throws Exception {
        masterPort = freePort();
        MasterServiceImpl masterService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                new RegionServiceDdlExecutor(3_000),
                new NoopRegionAdminExecutor()
        );

        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("MasterService", new MasterService.Processor<>(masterService));
        TServerSocket transport = new TServerSocket(masterPort);
        masterServer = new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
        rpcPool.submit(masterServer::serve);
        Thread.sleep(150);
    }

    private void createIfMissing(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }

    private void registerRegionServer(String id, int port) throws Exception {
        byte[] bytes = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(Map.of(
                        "id", id,
                        "host", "127.0.0.1",
                        "port", port,
                        "tableCount", 0,
                        "qps1min", 0.0,
                        "cpuUsage", 0.0,
                        "memUsage", 0.0,
                        "lastHeartbeat", System.currentTimeMillis()
                )).getBytes(StandardCharsets.UTF_8);
        zkClient.create().withMode(CreateMode.EPHEMERAL).forPath("/region_servers/" + id, bytes);
    }

    private void writeActiveMaster(String masterId, String address) throws Exception {
        byte[] bytes = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(Map.of(
                        "epoch", 1,
                        "masterId", masterId,
                        "address", address,
                        "ts", System.currentTimeMillis()
                )).getBytes(StandardCharsets.UTF_8);
        zkClient.setData().forPath("/active-master", bytes);
    }

    private int masterPortPlaceholder() {
        return 8080;
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static class RecordingMiniSqlProcess extends MiniSqlProcess {

        private final Set<String> createdTables = ConcurrentHashMap.newKeySet();
        private final Map<String, List<Row>> rowsByTable = new ConcurrentHashMap<>();

        RecordingMiniSqlProcess() {
            super("unused-bin", "unused-dir");
        }

        @Override
        public synchronized String execute(String sql) {
            String normalized = sql.trim().toLowerCase();
            if (normalized.startsWith("create table")) {
                String table = sql.replaceAll("(?i)^\\s*create\\s+table\\s+([a-zA-Z_][a-zA-Z0-9_]*).*", "$1");
                createdTables.add(table);
                rowsByTable.putIfAbsent(table, new java.util.ArrayList<>());
                return ">>> SUCCESS\n>>> \n";
            }
            if (normalized.startsWith("insert into")) {
                String table = sql.replaceAll("(?i)^\\s*insert\\s+into\\s+([a-zA-Z_][a-zA-Z0-9_]*).*", "$1");
                if (!createdTables.contains(table)) {
                    return ">>> Error: Table not exist\n>>> \n";
                }
                String value = sql.replaceAll(".*values\\(([^)]+)\\).*", "$1").trim();
                rowsByTable.computeIfAbsent(table, ignored -> new java.util.ArrayList<>())
                        .add(new Row(List.of(value)));
                return ">>> SUCCESS\n>>> \n";
            }
            if (normalized.startsWith("select")) {
                String table = sql.replaceAll("(?i)^.*from\\s+([a-zA-Z_][a-zA-Z0-9_]*).*", "$1");
                if (!createdTables.contains(table)) {
                    return ">>> Error: Table not exist\n>>> \n";
                }
                List<Row> rows = rowsByTable.getOrDefault(table, List.of());
                StringBuilder builder = new StringBuilder();
                builder.append("id\n");
                builder.append("--\n");
                for (Row row : rows) {
                    builder.append(String.join(" | ", row.getValues())).append('\n');
                }
                builder.append(">>> \n");
                return builder.toString();
            }
            if (normalized.startsWith("drop table")) {
                String table = sql.replaceAll("(?i)^\\s*drop\\s+table\\s+([a-zA-Z_][a-zA-Z0-9_]*).*", "$1");
                createdTables.remove(table);
                rowsByTable.remove(table);
                return ">>> SUCCESS\n>>> \n";
            }
            return ">>> SUCCESS\n>>> \n";
        }
    }

    private static class NoopRegionAdminExecutor implements RegionAdminExecutor {

        @Override
        public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response resumeTableWrite(RegionServerInfo regionServer, String tableName) {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) {
            return new Response(StatusCode.OK);
        }

        @Override
        public Response invalidateClientCache(RegionServerInfo regionServer, String tableName) {
            return new Response(StatusCode.OK);
        }
    }
}
