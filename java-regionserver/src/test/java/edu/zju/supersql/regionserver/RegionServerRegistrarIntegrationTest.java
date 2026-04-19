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
import java.util.Map;

class RegionServerRegistrarIntegrationTest {

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

        if (zkClient.checkExists().forPath("/region_servers") == null) {
            zkClient.create().creatingParentsIfNeeded().forPath("/region_servers", new byte[0]);
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
    void registerShouldCreateNodeWithDefaultMetrics() throws Exception {
        RegionServerRegistrar registrar = new RegionServerRegistrar(zkClient, "rs-test");
        registrar.register("127.0.0.1", 9090, 9190);

        byte[] bytes = zkClient.getData().forPath("/region_servers/rs-test");
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);

        Assertions.assertEquals("rs-test", payload.get("id"));
        Assertions.assertEquals("127.0.0.1", payload.get("host"));
        Assertions.assertEquals(9090, ((Number) payload.get("port")).intValue());
        Assertions.assertEquals(9190, ((Number) payload.get("httpPort")).intValue());
        Assertions.assertEquals(0, ((Number) payload.get("tableCount")).intValue());
    }

    @Test
    void heartbeatShouldUpdateMetrics() throws Exception {
        RegionServerRegistrar registrar = new RegionServerRegistrar(zkClient, "rs-heartbeat");
        registrar.register("127.0.0.1", 9091, 9191);

        registrar.heartbeat("127.0.0.1", 9091, 9191, 3, 11.5, 40.1, 60.2);

        byte[] bytes = zkClient.getData().forPath("/region_servers/rs-heartbeat");
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);

        Assertions.assertEquals(3, ((Number) payload.get("tableCount")).intValue());
        Assertions.assertEquals(11.5, ((Number) payload.get("qps1min")).doubleValue(), 0.0001);
        Assertions.assertEquals(40.1, ((Number) payload.get("cpuUsage")).doubleValue(), 0.0001);
        Assertions.assertEquals(60.2, ((Number) payload.get("memUsage")).doubleValue(), 0.0001);
    }

    @Test
    void heartbeatShouldReRegisterWhenNodeMissing() throws Exception {
        RegionServerRegistrar registrar = new RegionServerRegistrar(zkClient, "rs-reregister");
        registrar.register("127.0.0.1", 9092);
        zkClient.delete().forPath("/region_servers/rs-reregister");

        registrar.heartbeat("127.0.0.1", 9092, 9, 1.0, 2.0, 3.0);

        byte[] bytes = zkClient.getData().forPath("/region_servers/rs-reregister");
        Map<?, ?> payload = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
        Assertions.assertEquals("rs-reregister", payload.get("id"));
        Assertions.assertEquals(0, ((Number) payload.get("tableCount")).intValue());
    }

}
