package edu.zju.supersql.regionserver.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.regionserver.ZkPaths;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RegionAdminServiceAssignmentIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path dataDir;

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;

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

        createPathIfMissing(ZkPaths.REGION_SERVERS);
        createPathIfMissing(ZkPaths.ASSIGNMENTS);
        createPathIfMissing(ZkPaths.META_TABLES);
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
    void deleteLocalTableShouldRemoveOnlyCurrentRsFromAssignment() throws Exception {
        writeAssignment("orders", List.of(
                replica("rs-1", 9090),
                replica("rs-2", 9091),
                replica("rs-3", 9092)
        ));

        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-2");

        Assertions.assertEquals(StatusCode.OK, service.deleteLocalTable("orders").getCode());

        byte[] bytes = zkClient.getData().forPath(ZkPaths.assignment("orders"));
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
        List<?> replicas = (List<?>) payload.get("replicas");

        Assertions.assertEquals(2, replicas.size());
        Assertions.assertTrue(replicas.stream().noneMatch(item ->
                item instanceof Map<?, ?> map && "rs-2".equals(String.valueOf(map.get("id")))));
        Assertions.assertTrue(replicas.stream().anyMatch(item ->
                item instanceof Map<?, ?> map && "rs-1".equals(String.valueOf(map.get("id")))));
        Assertions.assertTrue(replicas.stream().anyMatch(item ->
                item instanceof Map<?, ?> map && "rs-3".equals(String.valueOf(map.get("id")))));
    }

    @Test
    void deleteLocalTableShouldDeleteAssignmentNodeWhenReplicaBecomesEmpty() throws Exception {
        writeAssignment("single_table", List.of(replica("rs-1", 9090)));

        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-1");

        Assertions.assertEquals(StatusCode.OK, service.deleteLocalTable("single_table").getCode());
        Assertions.assertNull(zkClient.checkExists().forPath(ZkPaths.assignment("single_table")));
    }

    @Test
    void deleteLocalTableShouldKeepAssignmentWhenCurrentRsNotPresent() throws Exception {
        writeAssignment("orders", List.of(
                replica("rs-1", 9090),
                replica("rs-3", 9092)
        ));

        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-9");

        Assertions.assertEquals(StatusCode.OK, service.deleteLocalTable("orders").getCode());

        byte[] bytes = zkClient.getData().forPath(ZkPaths.assignment("orders"));
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
        List<?> replicas = (List<?>) payload.get("replicas");
        Assertions.assertEquals(2, replicas.size());
    }

    @Test
    void invalidateClientCacheShouldBumpTableMetaVersion() throws Exception {
        writeTableMeta("orders", 5L);

        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-1");

        Assertions.assertEquals(StatusCode.OK, service.invalidateClientCache("orders").getCode());

        byte[] bytes = zkClient.getData().forPath(ZkPaths.tableMeta("orders"));
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
        Assertions.assertEquals(6L, ((Number) payload.get("version")).longValue());
        Assertions.assertTrue(payload.containsKey("cacheInvalidatedAt"));
    }

    @Test
    void invalidateClientCacheShouldReturnOkWhenTableMetaMissing() throws Exception {
        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-1");

        Assertions.assertEquals(StatusCode.OK, service.invalidateClientCache("missing_table").getCode());
    }

    @Test
    void registerRegionServerShouldReturnErrorAndNotCreateNode() throws Exception {
        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-1");

        edu.zju.supersql.rpc.RegionServerInfo info = new edu.zju.supersql.rpc.RegionServerInfo("rs-1", "127.0.0.1", 9090);
        info.setTableCount(4);

        Response response = service.registerRegionServer(info);
        Assertions.assertEquals(StatusCode.ERROR, response.getCode());
        Assertions.assertTrue(String.valueOf(response.getMessage()).contains("deprecated"));
        Assertions.assertNull(zkClient.checkExists().forPath(ZkPaths.regionServer("rs-1")));
    }

    @Test
    void heartbeatShouldReturnErrorAndNotCreateNode() throws Exception {
        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-1");

        edu.zju.supersql.rpc.RegionServerInfo heartbeat = new edu.zju.supersql.rpc.RegionServerInfo("rs-1", "127.0.0.1", 9090);
        heartbeat.setTableCount(6);
        heartbeat.setQps1min(12.5);
        Response response = service.heartbeat(heartbeat);
        Assertions.assertEquals(StatusCode.ERROR, response.getCode());
        Assertions.assertTrue(String.valueOf(response.getMessage()).contains("deprecated"));
        Assertions.assertNull(zkClient.checkExists().forPath(ZkPaths.regionServer("rs-1")));
    }

    @Test
    void heartbeatShouldNotMutateExistingNode() throws Exception {
        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(), zkClient, dataDir.toString(), "rs-1");

        Map<String, Object> original = new HashMap<>();
        original.put("id", "rs-1");
        original.put("host", "127.0.0.1");
        original.put("port", 9090);
        original.put("httpPort", 19090);
        original.put("manualInterventionRequired", true);
        original.put("terminalQueueCount", 7L);
        original.put("activeDecisionReadyCount", 3L);
        original.put("activeDecisionCandidateCount", 5L);
        original.put("tableCount", 1);
        original.put("qps1min", 1.5);
        original.put("lastHeartbeat", 123456L);
        zkClient.create().creatingParentsIfNeeded().withMode(org.apache.zookeeper.CreateMode.EPHEMERAL)
                .forPath(
                        ZkPaths.regionServer("rs-1"),
                        MAPPER.writeValueAsString(original).getBytes(StandardCharsets.UTF_8));

        byte[] beforeBytes = zkClient.getData().forPath(ZkPaths.regionServer("rs-1"));
        zkClient.setData().forPath(
                ZkPaths.regionServer("rs-1"),
                MAPPER.writeValueAsString(original).getBytes(StandardCharsets.UTF_8));

        edu.zju.supersql.rpc.RegionServerInfo heartbeat = new edu.zju.supersql.rpc.RegionServerInfo("rs-1", "127.0.0.1", 9090);
        heartbeat.setTableCount(9);
        heartbeat.setQps1min(22.0);
        Response response = service.heartbeat(heartbeat);
        Assertions.assertEquals(StatusCode.ERROR, response.getCode());

        byte[] afterBytes = zkClient.getData().forPath(ZkPaths.regionServer("rs-1"));
        Assertions.assertArrayEquals(beforeBytes, afterBytes);
    }

    private void createPathIfMissing(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }

    private void writeAssignment(String tableName, List<Map<String, Object>> replicas) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("tableName", tableName);
        payload.put("replicas", replicas);
        byte[] bytes = MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);

        String path = ZkPaths.assignment(tableName);
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }

    private void writeTableMeta(String tableName, long version) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("tableName", tableName);
        payload.put("tableStatus", "ACTIVE");
        payload.put("version", version);
        payload.put("primaryRS", replica("rs-1", 9090));
        payload.put("replicas", List.of(replica("rs-1", 9090), replica("rs-2", 9091)));

        byte[] bytes = MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        String path = ZkPaths.tableMeta(tableName);
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }

    private Map<String, Object> replica(String id, int port) {
        Map<String, Object> rs = new HashMap<>();
        rs.put("id", id);
        rs.put("host", "127.0.0.1");
        rs.put("port", port);
        return rs;
    }

    private Map<?, ?> readJson(String path) throws Exception {
        byte[] bytes = zkClient.getData().forPath(path);
        return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
    }
}
