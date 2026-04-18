package edu.zju.supersql.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.TableLocation;
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
import java.util.List;
import java.util.Map;

class MasterServerBootstrapIntegrationTest {

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

        for (String path : ZkPaths.bootstrapPaths()) {
            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
            }
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
    void preloadMetadataFromZkShouldReturnTableCount() throws Exception {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("orders", primary, List.of(primary));

        Map<String, Object> root = new HashMap<>();
        root.put("tableName", location.getTableName());
        root.put("tableStatus", "ACTIVE");
        root.put("version", 1L);
        root.put("primaryRS", Map.of(
                "id", primary.getId(),
                "host", primary.getHost(),
                "port", primary.getPort()
        ));
        root.put("replicas", List.of(Map.of(
                "id", primary.getId(),
                "host", primary.getHost(),
                "port", primary.getPort()
        )));

        zkClient.create().creatingParentsIfNeeded().forPath(
                ZkPaths.tableMeta("orders"),
                MAPPER.writeValueAsString(root).getBytes(StandardCharsets.UTF_8)
        );

        int count = MasterServer.preloadMetadataFromZk(zkClient);
        Assertions.assertEquals(1, count);
    }

    @Test
    void preloadMetadataFromZkShouldHandleNullClient() {
        Assertions.assertEquals(0, MasterServer.preloadMetadataFromZk(null));
    }
}
