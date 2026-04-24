package edu.zju.supersql.master.election;

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
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BooleanSupplier;

class LeaderElectorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EmbeddedZkServer server;
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
                } catch (Exception e) {
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
            waitUntil(Duration.ofSeconds(5), () -> {
                try {
                    Map<?, ?> active = readActiveMaster(zk1);
                    return ((Number) active.get("epoch")).longValue() == 10L;
                } catch (Exception e) {
                    return false;
                }
            });

            Map<?, ?> active = readActiveMaster(zk1);
            long epoch = ((Number) active.get("epoch")).longValue();
            Assertions.assertEquals(10L, epoch);
            Assertions.assertEquals("master-1", active.get("masterId"));
        } finally {
            e1.close();
        }
    }

    @Test
    void shouldStartWhenEnsuredPathsAlreadyExist() throws Exception {
        // Regression for BUG-08: in production, /masters and /active-master are
        // pre-created by MasterServer's bootstrap loop before LeaderElector runs.
        // ensurePath previously did `checkExists() then create()`, which threw
        // KeeperException.NodeExistsException whenever a concurrent master
        // (or the bootstrap itself) had just created the same path. The
        // exception escaped start() and the master never registered into the
        // leader latch, leaving the cluster with a single participant.
        // ensurePath must now swallow NodeExistsException and proceed.
        Assertions.assertNotNull(zk1.checkExists().forPath("/masters"));
        Assertions.assertNotNull(zk1.checkExists().forPath("/active-master"));

        LeaderElector elector = new LeaderElector(zk1, "master-1", "master-1:8080");
        try {
            elector.start();
            waitUntil(Duration.ofSeconds(5), elector::isLeader);
            Assertions.assertTrue(elector.isLeader());
        } finally {
            elector.close();
        }
    }

    @Test
    void shouldKeepMonotonicEpochAndSingleLeaderUnderRepeatedFlapping() throws Exception {
        LeaderElector[] electors = new LeaderElector[] {
                new LeaderElector(zk1, "master-1", "master-1:8080"),
                new LeaderElector(zk2, "master-2", "master-2:8081")
        };
        try {
            electors[0].start();
            electors[1].start();

            waitUntil(Duration.ofSeconds(5), () -> electors[0].isLeader() ^ electors[1].isLeader());
            waitUntil(Duration.ofSeconds(5), () -> activeMasterMatchesLeader(zk1, electors[0], electors[1]));
            long previousEpoch = ((Number) readActiveMaster(zk1).get("epoch")).longValue();

            for (int round = 0; round < 4; round++) {
                boolean closeE1 = electors[0].isLeader();
                if (closeE1) {
                    electors[0].close();
                    electors[0] = new LeaderElector(zk1, "master-1", "master-1:8080");
                    electors[0].start();
                } else {
                    electors[1].close();
                    electors[1] = new LeaderElector(zk2, "master-2", "master-2:8081");
                    electors[1].start();
                }

                waitUntil(Duration.ofSeconds(8), () -> electors[0].isLeader() ^ electors[1].isLeader());
                waitUntil(Duration.ofSeconds(8), () -> activeMasterMatchesLeader(zk1, electors[0], electors[1]));

                Map<?, ?> active = readActiveMaster(zk1);
                long currentEpoch = ((Number) active.get("epoch")).longValue();
                Assertions.assertTrue(currentEpoch > previousEpoch,
                        "epoch should be monotonic under master flapping");
                previousEpoch = currentEpoch;

                String activeId = String.valueOf(active.get("masterId"));
                if (electors[0].isLeader()) {
                    Assertions.assertEquals("master-1", activeId);
                } else {
                    Assertions.assertEquals("master-2", activeId);
                }
            }
        } finally {
            electors[0].close();
            electors[1].close();
        }
    }

    private static boolean activeMasterMatchesLeader(CuratorFramework client,
                                                     LeaderElector e1,
                                                     LeaderElector e2) {
        try {
            Map<?, ?> active = readActiveMaster(client);
            String activeId = String.valueOf(active.get("masterId"));
            if (e1.isLeader() && !e2.isLeader()) {
                return "master-1".equals(activeId);
            }
            if (e2.isLeader() && !e1.isLeader()) {
                return "master-2".equals(activeId);
            }
            return false;
        } catch (Exception e) {
            return false;
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
