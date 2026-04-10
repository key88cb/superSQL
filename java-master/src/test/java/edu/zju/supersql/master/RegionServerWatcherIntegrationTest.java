package edu.zju.supersql.master;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;

class RegionServerWatcherIntegrationTest {

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
        createIfMissing(ZkPaths.REGION_SERVERS);
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
    void shouldReceiveUpUpdatedAndDownEvents() throws Exception {
        List<String> events = new CopyOnWriteArrayList<>();
        RegionServerWatcher watcher = new RegionServerWatcher(zkClient, new RegionServerWatcher.Listener() {
            @Override
            public void onRegionServerUp(String rsId) {
                events.add("UP:" + rsId);
            }

            @Override
            public void onRegionServerUpdated(String rsId) {
                events.add("UPDATED:" + rsId);
            }

            @Override
            public void onRegionServerDown(String rsId) {
                events.add("DOWN:" + rsId);
            }
        });

        try {
            watcher.start();
            upsertRegionServer("rs-1", 1);
            waitUntil(Duration.ofSeconds(3), () -> events.contains("UP:rs-1"));

            upsertRegionServer("rs-1", 2);
            waitUntil(Duration.ofSeconds(3), () -> events.contains("UPDATED:rs-1"));

            zkClient.delete().forPath(ZkPaths.regionServer("rs-1"));
            waitUntil(Duration.ofSeconds(3), () -> events.contains("DOWN:rs-1"));
        } finally {
            watcher.close();
        }
    }

    private void createIfMissing(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }

    private void upsertRegionServer(String id, int tableCount) throws Exception {
        byte[] bytes = MAPPER.writeValueAsString(Map.of(
                "id", id,
                "host", "127.0.0.1",
                "port", 9090,
                "tableCount", tableCount,
                "qps1min", 0.0,
                "cpuUsage", 0.0,
                "memUsage", 0.0,
                "lastHeartbeat", System.currentTimeMillis()
        )).getBytes(StandardCharsets.UTF_8);

        String path = ZkPaths.regionServer(id);
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        Assertions.fail("Condition not met within " + timeout.toMillis() + " ms");
    }
}
