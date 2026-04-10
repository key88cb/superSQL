package edu.zju.supersql.master.rpc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.master.balance.LoadBalancer;
import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RebalanceLoggingConsistencyTest {

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;
    private MasterServiceImpl service;
    private RecordingRegionDdlExecutor ddlExecutor;
    private RecordingRegionAdminExecutor adminExecutor;

    private Logger logger;
    private ListAppender<ILoggingEvent> appender;

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

        createPathIfMissing("/region_servers");
        createPathIfMissing("/meta/tables");
        createPathIfMissing("/assignments");
        createPathIfMissing("/active-master");

        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
        writeActiveMaster("master-1", "master-1:8080");

        ddlExecutor = new RecordingRegionDdlExecutor();
        adminExecutor = new RecordingRegionAdminExecutor();
        service = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor);

        logger = (Logger) LoggerFactory.getLogger(MasterServiceImpl.class);
        appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        logger.setLevel(Level.INFO);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (logger != null && appender != null) {
            logger.detachAppender(appender);
            appender.stop();
        }
        if (zkClient != null) {
            zkClient.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void rebalanceLogsShouldMatchUpdatedMetadata() throws Exception {
        registerRegionServer("rs-1", 0);
        registerRegionServer("rs-2", 2);
        registerRegionServer("rs-3", 1);
        registerRegionServer("rs-4", 3);

        Response create = service.createTable("create table t_log(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", 0);
        registerRegionServer("rs-2", 10);
        registerRegionServer("rs-3", 1);
        registerRegionServer("rs-4", 2);

        Response rebalance = service.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());

        List<String> messages = appender.list.stream().map(ILoggingEvent::getFormattedMessage).toList();
        Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("triggerRebalance start")));
        Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("triggerRebalance executing table=t_log")));
        Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("triggerRebalance metadata updated table=t_log replicasAfter=[rs-1, rs-3, rs-4]")));
        Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("triggerRebalance success table=t_log source=rs-2 target=rs-4")));
        Assertions.assertTrue(messages.stream().anyMatch(m -> m.contains("triggerRebalance resume writes table=t_log primary=rs-1")));

        TableLocation location = service.getTableLocation("t_log");
        Assertions.assertEquals(List.of("rs-1", "rs-3", "rs-4"),
                location.getReplicas().stream().map(RegionServerInfo::getId).toList());
    }

    private void createPathIfMissing(String path) throws Exception {
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

    private void registerRegionServer(String id, int tableCount) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", id);
        payload.put("host", "127.0.0.1");
        payload.put("port", 9000 + tableCount);
        payload.put("tableCount", tableCount);
        payload.put("qps1min", 0.0);
        payload.put("cpuUsage", 0.0);
        payload.put("memUsage", 0.0);
        payload.put("lastHeartbeat", System.currentTimeMillis());

        byte[] bytes = new com.fasterxml.jackson.databind.ObjectMapper()
                .writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        String path = "/region_servers/" + id;
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }

    private static class RecordingRegionDdlExecutor implements RegionDdlExecutor {
        @Override
        public Response execute(RegionServerInfo regionServer, String tableName, String ddl) {
            return new Response(StatusCode.OK);
        }
    }

    private static class RecordingRegionAdminExecutor implements RegionAdminExecutor {
        private final List<String> ops = new ArrayList<>();

        @Override
        public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) {
            ops.add("pause:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response resumeTableWrite(RegionServerInfo regionServer, String tableName) {
            ops.add("resume:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
            ops.add("transfer:" + source.getId() + "->" + target.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) {
            ops.add("delete:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response invalidateClientCache(RegionServerInfo regionServer, String tableName) {
            ops.add("invalidate:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }
    }
}
