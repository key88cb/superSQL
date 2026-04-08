package edu.zju.supersql.master.election;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

class LeaderElectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TestingServer server;
    private CuratorFramework zk1;
    private CuratorFramework zk2;

    @BeforeEach
    void setUp() throws Exception {
        server = EmbeddedZkServerFactory.create();
        zk1 = buildClient();
        zk2 = buildClient();

        createIfMissing(zk1, "/masters");
        createIfMissing(zk1, "/active-master");
        writeActiveMaster(zk1, 0L, "none", "none");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (zk1 != null) {
            zk1.close();
        }
        if (zk2 != null) {
            zk2.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldElectExactlyOneLeader() throws Exception {
        LeaderElector e1 = new LeaderElector(zk1, "master-1", "master-1:8080");
        LeaderElector e2 = new LeaderElector(zk2, "master-2", "master-2:8081");
        try {
            e1.start();
            e2.start();

            waitUntil(Duration.ofSeconds(5), () -> e1.isLeader() || e2.isLeader());
            Assertions.assertFalse(e1.isLeader() && e2.isLeader());

            // LeaderLatch state and /active-master write are asynchronous; wait for payload convergence.
            waitUntil(Duration.ofSeconds(5), () -> {
                try {
                    Map<?, ?> active = readActiveMaster(zk1);
                    Object id = active.get("masterId");
                    return "master-1".equals(String.valueOf(id)) || "master-2".equals(String.valueOf(id));
                } catch (Exception ignored) {
                    return false;
                }
            });

            Map<?, ?> active = readActiveMaster(zk1);
            String id = String.valueOf(active.get("masterId"));
            Assertions.assertTrue("master-1".equals(id) || "master-2".equals(id));
        } finally {
            e1.close();
            e2.close();
        }
    }

    @Test
    void shouldFailoverToStandbyWhenLeaderClosed() throws Exception {
        LeaderElector e1 = new LeaderElector(zk1, "master-1", "master-1:8080");
        LeaderElector e2 = new LeaderElector(zk2, "master-2", "master-2:8081");
        try {
            e1.start();
            e2.start();
            waitUntil(Duration.ofSeconds(5), () -> e1.isLeader() ^ e2.isLeader());

            LeaderElector leader = e1.isLeader() ? e1 : e2;
            LeaderElector standby = e1.isLeader() ? e2 : e1;

            leader.close();
            waitUntil(Duration.ofSeconds(8), standby::isLeader);
            Assertions.assertTrue(standby.isLeader());

            Map<?, ?> active = readActiveMaster(zk1);
            long epoch = ((Number) active.get("epoch")).longValue();
            Assertions.assertTrue(epoch >= 2L);
        } finally {
            e1.close();
            e2.close();
        }
    }

    @Test
    void shouldCarryForwardExistingEpochWhenElected() throws Exception {
        writeActiveMaster(zk1, 9L, "old-master", "old-master:8080");

        LeaderElector e1 = new LeaderElector(zk1, "master-1", "master-1:8080");
        try {
            e1.start();
            waitUntil(Duration.ofSeconds(5), e1::isLeader);

            Map<?, ?> active = readActiveMaster(zk1);
            long epoch = ((Number) active.get("epoch")).longValue();
            Assertions.assertEquals(10L, epoch);
            Assertions.assertEquals("master-1", active.get("masterId"));
        } finally {
            e1.close();
        }
    }

    private CuratorFramework buildClient() {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(200, 3))
                .sessionTimeoutMs(10_000)
                .connectionTimeoutMs(5_000)
                .namespace("supersql")
                .build();
        client.start();
        return client;
    }

    private static void createIfMissing(CuratorFramework client, String path) throws Exception {
        if (client.checkExists().forPath(path) == null) {
            client.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }

    private static void writeActiveMaster(CuratorFramework client, long epoch, String masterId, String address) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("epoch", epoch);
        payload.put("masterId", masterId);
        payload.put("address", address);
        payload.put("ts", System.currentTimeMillis());
        client.setData().forPath("/active-master", MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));
    }

    private static Map<?, ?> readActiveMaster(CuratorFramework client) throws Exception {
        byte[] bytes = client.getData().forPath("/active-master");
        return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
    }

    private static void waitUntil(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        Assertions.fail("Condition not met within " + timeout.toMillis() + " ms");
    }
}
