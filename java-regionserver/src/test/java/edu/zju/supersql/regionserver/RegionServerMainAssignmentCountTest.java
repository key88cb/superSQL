package edu.zju.supersql.regionserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

class RegionServerMainAssignmentCountTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

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

        if (zkClient.checkExists().forPath("/assignments") == null) {
            zkClient.create().creatingParentsIfNeeded().forPath("/assignments", new byte[0]);
        }
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
    void shouldCountOnlyTablesAssignedToCurrentRegionServer() throws Exception {
        createAssignment("orders", List.of(
                Map.of("id", "rs-a", "host", "127.0.0.1", "port", 9090),
                Map.of("id", "rs-b", "host", "127.0.0.1", "port", 9091)));
        createAssignment("users", List.of(
                Map.of("id", "rs-b", "host", "127.0.0.1", "port", 9091),
                Map.of("id", "rs-c", "host", "127.0.0.1", "port", 9092)));
        createAssignment("products", List.of(
                Map.of("id", "rs-a", "host", "127.0.0.1", "port", 9090),
                Map.of("id", "rs-c", "host", "127.0.0.1", "port", 9092)));

        int count = RegionServerMain.countAssignedTablesForRegionServer(zkClient, "rs-b");

        Assertions.assertEquals(2, count);
    }

    @Test
    void shouldReturnZeroWhenAssignmentsMissing() throws Exception {
        zkClient.delete().forPath("/assignments");

        int count = RegionServerMain.countAssignedTablesForRegionServer(zkClient, "rs-b");

        Assertions.assertEquals(0, count);
    }

    @Test
    void computeRecentQpsShouldUseDeltaAndElapsedMillis() {
        double qps = RegionServerMain.computeRecentQps(210L, 160L, 10_000L);
        Assertions.assertEquals(5.0, qps, 0.0001);

        Assertions.assertEquals(0.0, RegionServerMain.computeRecentQps(100L, 120L, 1000L), 0.0001);
        Assertions.assertEquals(0.0, RegionServerMain.computeRecentQps(100L, 100L, 0L), 0.0001);
    }

    @Test
    void resourceUsageSamplingShouldStayWithinPercentRange() {
        double cpu = RegionServerMain.sampleProcessCpuUsagePercent();
        double mem = RegionServerMain.sampleHeapMemoryUsagePercent();

        Assertions.assertTrue(cpu >= 0.0 && cpu <= 100.0);
        Assertions.assertTrue(mem >= 0.0 && mem <= 100.0);
    }

    private void createAssignment(String tableName, List<Map<String, Object>> replicas) throws Exception {
        byte[] payload = MAPPER.writeValueAsString(Map.of(
                "tableName", tableName,
                "replicas", replicas)).getBytes(StandardCharsets.UTF_8);
        zkClient.create().forPath("/assignments/" + tableName, payload);
    }
}
