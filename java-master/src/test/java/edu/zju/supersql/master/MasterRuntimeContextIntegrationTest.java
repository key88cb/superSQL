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
import java.util.Map;

class MasterRuntimeContextIntegrationTest {

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

        createIfMissing("/active-master");
        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
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
    void tryBootstrapActiveMasterShouldWriteInitialPayload() throws Exception {
        MasterRuntimeContext.tryBootstrapActiveMaster();

        Map<?, ?> payload = readActiveMaster();
        Assertions.assertEquals(1L, ((Number) payload.get("epoch")).longValue());
        Assertions.assertEquals("master-1", payload.get("masterId"));
        Assertions.assertEquals("master-1:8080", payload.get("address"));
    }

    @Test
    void tryBootstrapActiveMasterShouldRepairMissingMasterId() throws Exception {
        zkClient.setData().forPath("/active-master", "{\"epoch\":7}".getBytes(StandardCharsets.UTF_8));

        MasterRuntimeContext.tryBootstrapActiveMaster();

        Map<?, ?> payload = readActiveMaster();
        Assertions.assertEquals(8L, ((Number) payload.get("epoch")).longValue());
        Assertions.assertEquals("master-1", payload.get("masterId"));
    }

    @Test
    void readActiveMasterAddressShouldFallbackToMasterIdWhenAddressMissing() throws Exception {
        zkClient.setData().forPath("/active-master", "{\"epoch\":1,\"masterId\":\"master-9\"}".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("master-9", MasterRuntimeContext.readActiveMasterAddress());
    }

    @Test
    void isActiveMasterShouldReflectCurrentLeader() throws Exception {
        zkClient.setData().forPath("/active-master",
                "{\"epoch\":1,\"masterId\":\"master-1\",\"address\":\"master-1:8080\"}".getBytes(StandardCharsets.UTF_8));
        Assertions.assertTrue(MasterRuntimeContext.isActiveMaster());

        zkClient.setData().forPath("/active-master",
                "{\"epoch\":2,\"masterId\":\"master-2\",\"address\":\"master-2:8081\"}".getBytes(StandardCharsets.UTF_8));
        Assertions.assertFalse(MasterRuntimeContext.isActiveMaster());
    }

    @Test
    void isActiveMasterShouldFailClosedWhenPayloadIsInvalid() throws Exception {
        zkClient.setData().forPath("/active-master", "{invalid-json".getBytes(StandardCharsets.UTF_8));
        Assertions.assertFalse(MasterRuntimeContext.isActiveMaster());
    }

    private void createIfMissing(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }

    private Map<?, ?> readActiveMaster() throws Exception {
        byte[] bytes = zkClient.getData().forPath("/active-master");
        return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
    }
}
