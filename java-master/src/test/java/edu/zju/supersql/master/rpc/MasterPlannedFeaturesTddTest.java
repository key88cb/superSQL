package edu.zju.supersql.master.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.rpc.TableLocation;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MasterPlannedFeaturesTddTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;
    private MasterServiceImpl service;

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

        service = new MasterServiceImpl(
            new edu.zju.supersql.master.meta.MetaManager(zkClient),
            new edu.zju.supersql.master.meta.AssignmentManager(zkClient),
            new edu.zju.supersql.master.balance.LoadBalancer(),
            new RecordingRegionDdlExecutor(),
            new RecordingRegionAdminExecutor());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (zkClient != null) {
            zkClient.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void triggerRebalanceShouldReturnOkWhenClusterAlreadyBalanced() throws Exception {
        registerRegionServer("rs-1", 1);
        registerRegionServer("rs-2", 1);
        registerRegionServer("rs-3", 1);

        Response response = service.triggerRebalance();

        Assertions.assertEquals(StatusCode.OK, response.getCode());
    }

    @Test
    void triggerRebalanceShouldMoveHotspotTableToLighterNode() throws Exception {
        registerRegionServer("rs-1", 10);
        registerRegionServer("rs-2", 1);
        registerRegionServer("rs-3", 1);

        service.createTable("create table hot_table(id int, primary key(id));");

        Response response = service.triggerRebalance();

        Assertions.assertEquals(StatusCode.OK, response.getCode());
        Map<?, ?> assignment = readJson("/assignments/hot_table");
        Assertions.assertEquals(3, ((java.util.List<?>) assignment.get("replicas")).size());
    }

    @Test
    void getTableLocationShouldDowngradeAndRecoverWhenReplicasOfflineThenOnline() throws Exception {
        registerRegionServer("rs-1", 0);
        registerRegionServer("rs-2", 1);
        registerRegionServer("rs-3", 2);

        Response create = service.createTable("create table route_heal(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");
        zkClient.delete().forPath("/region_servers/rs-2");
        zkClient.delete().forPath("/region_servers/rs-3");

        TableLocation unavailable = service.getTableLocation("route_heal");
        Assertions.assertEquals("UNAVAILABLE", unavailable.getTableStatus());

        registerRegionServer("rs-2", 1);
        TableLocation recovered = service.getTableLocation("route_heal");
        Assertions.assertEquals("ACTIVE", recovered.getTableStatus());
        Assertions.assertEquals("rs-2", recovered.getPrimaryRS().getId());
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
        zkClient.setData().forPath("/active-master", MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));
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

        zkClient.create().withMode(CreateMode.EPHEMERAL)
                .forPath("/region_servers/" + id, MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));
    }

    private Map<?, ?> readJson(String path) throws Exception {
        return MAPPER.readValue(new String(zkClient.getData().forPath(path), StandardCharsets.UTF_8), Map.class);
    }

    private static final class RecordingRegionDdlExecutor implements RegionDdlExecutor {
        @Override
        public Response execute(edu.zju.supersql.rpc.RegionServerInfo regionServer, String tableName, String ddl) {
            return new Response(StatusCode.OK);
        }
    }

    private static final class RecordingRegionAdminExecutor implements RegionAdminExecutor {
        private final List<String> ops = new ArrayList<>();

        @Override
        public Response pauseTableWrite(edu.zju.supersql.rpc.RegionServerInfo regionServer, String tableName) {
            ops.add("pause:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response resumeTableWrite(edu.zju.supersql.rpc.RegionServerInfo regionServer, String tableName) {
            ops.add("resume:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response transferTable(edu.zju.supersql.rpc.RegionServerInfo source, String tableName,
                                      edu.zju.supersql.rpc.RegionServerInfo target) {
            ops.add("transfer:" + source.getId() + "->" + target.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response deleteLocalTable(edu.zju.supersql.rpc.RegionServerInfo regionServer, String tableName) {
            ops.add("delete:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }

        @Override
        public Response invalidateClientCache(edu.zju.supersql.rpc.RegionServerInfo regionServer, String tableName) {
            ops.add("invalidate:" + regionServer.getId());
            return new Response(StatusCode.OK);
        }
    }
}
