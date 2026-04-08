package edu.zju.supersql.master;

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
import java.util.HashMap;
import java.util.Map;

class MasterHeartbeatIntegrationTest {

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

        createIfMissing("/masters");
        createIfMissing("/masters/active-heartbeat");
        createIfMissing("/active-master");

        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
        writeActiveMaster("master-1", "master-1:8080");
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
    void updateActiveHeartbeatShouldWriteTimestamp() throws Exception {
        MasterRuntimeContext.updateActiveHeartbeat();

        byte[] bytes = zkClient.getData().forPath("/masters/active-heartbeat");
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);

        Assertions.assertEquals("master-1", payload.get("masterId"));
        Assertions.assertEquals("master-1:8080", payload.get("address"));
        Assertions.assertTrue(((Number) payload.get("ts")).longValue() > 0);
    }

    @Test
    void updateHeartbeatShouldNotOverwriteWhenNotLeader() throws Exception {
        zkClient.setData().forPath("/masters/active-heartbeat", "{}".getBytes(StandardCharsets.UTF_8));
        writeActiveMaster("master-2", "master-2:8081");

        MasterRuntimeContext.updateActiveHeartbeat();

        String payload = new String(zkClient.getData().forPath("/masters/active-heartbeat"), StandardCharsets.UTF_8);
        Assertions.assertEquals("{}", payload);
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
        zkClient.setData().forPath("/active-master", MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8));
    }

}
