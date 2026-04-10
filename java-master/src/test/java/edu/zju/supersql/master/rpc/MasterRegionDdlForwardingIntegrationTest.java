package edu.zju.supersql.master.rpc;

import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.master.balance.LoadBalancer;
import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.rpc.*;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class MasterRegionDdlForwardingIntegrationTest {

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;
    private MasterServiceImpl service;

    private ExecutorService rpcPool;
    private final List<TThreadPoolServer> regionServers = new ArrayList<>();
    private final List<RecordingRegionService> regionHandlers = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        server = EmbeddedZkServerFactory.create();
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
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

        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
        writeActiveMaster("master-1", "master-1:8080");

        rpcPool = Executors.newCachedThreadPool();
        startRegionServer("rs-1", 1);
        startRegionServer("rs-2", 2);

        service = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                new RegionServiceDdlExecutor(3_000),
                new NoopRegionAdminExecutor());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (TThreadPoolServer regionServer : regionServers) {
            regionServer.stop();
        }
        if (rpcPool != null) {
            rpcPool.shutdownNow();
        }
        if (zkClient != null) {
            zkClient.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void createTableShouldForwardDdlToAllChosenRegionServers() throws Exception {
        Response response = service.createTable("create table orders(id int, primary key(id));");

        Assertions.assertEquals(StatusCode.OK, response.getCode());
        Assertions.assertTrue(regionHandlers.stream()
                .allMatch(handler -> handler.executedSql.contains("create table orders(id int, primary key(id));")));
    }

    @Test
    void dropTableShouldForwardDdlToChosenRegionServers() throws Exception {
        Response create = service.createTable("create table users(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Response drop = service.dropTable("users");

        Assertions.assertEquals(StatusCode.OK, drop.getCode());
        Assertions.assertTrue(regionHandlers.stream()
                .allMatch(handler -> handler.executedSql.stream().anyMatch(sql -> sql.startsWith("drop table users"))));
    }

    private void startRegionServer(String rsId, int tableCount) throws Exception {
        int port = freePort();
        RecordingRegionService handler = new RecordingRegionService();
        TThreadPoolServer server = buildRegionServer(port, handler);
        regionServers.add(server);
        regionHandlers.add(handler);
        rpcPool.submit(server::serve);
        Thread.sleep(150);
        registerRegionServer(rsId, "127.0.0.1", port, tableCount);
    }

    private static TThreadPoolServer buildRegionServer(int port, RegionService.Iface handler) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionService", new RegionService.Processor<>(handler));
        TServerSocket transport = new TServerSocket(port);
        return new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private void createIfMissing(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }

    private void writeActiveMaster(String masterId, String address) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("epoch", 1);
        payload.put("masterId", masterId);
        payload.put("address", address);
        payload.put("ts", System.currentTimeMillis());
        zkClient.setData().forPath("/active-master", new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));
    }

    private void registerRegionServer(String id, String host, int port, int tableCount) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", id);
        payload.put("host", host);
        payload.put("port", port);
        payload.put("tableCount", tableCount);
        payload.put("qps1min", 0.0);
        payload.put("cpuUsage", 0.0);
        payload.put("memUsage", 0.0);
        payload.put("lastHeartbeat", System.currentTimeMillis());

        byte[] bytes = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        String path = "/region_servers/" + id;
        zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
    }

    private static class RecordingRegionService implements RegionService.Iface {

        private final List<String> executedSql = new CopyOnWriteArrayList<>();

        @Override
        public QueryResult execute(String tableName, String sql) {
            executedSql.add(sql);
            return new QueryResult(new Response(StatusCode.OK));
        }

        @Override
        public QueryResult executeBatch(String tableName, List<String> sqls) {
            executedSql.addAll(sqls);
            return new QueryResult(new Response(StatusCode.OK));
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
