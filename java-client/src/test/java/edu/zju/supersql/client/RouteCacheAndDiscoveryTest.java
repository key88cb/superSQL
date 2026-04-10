package edu.zju.supersql.client;

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
import java.util.Collections;

class RouteCacheAndDiscoveryTest {

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
    void invalidateIfVersionMismatchShouldKeepEntryWhenVersionMatches() {
        RouteCache cache = new RouteCache(60_000);
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("t_keep", primary, Collections.singletonList(primary));
        location.setVersion(7L);

        cache.put("t_keep", location);
        cache.invalidateIfVersionMismatch("t_keep", 7L);

        Assertions.assertNotNull(cache.get("t_keep"));
    }

    @Test
    void readActiveMasterShouldFallbackToMasterIdWhenAddressMissing() throws Exception {
        zkClient.create().creatingParentsIfNeeded().forPath(
                "/active-master",
                "{\"masterId\":\"master-2\"}".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("master-2", SqlClient.readActiveMaster(zkClient, "fallback:8080"));
    }

    @Test
    void readActiveMasterShouldFallbackOnInvalidPayload() throws Exception {
        zkClient.create().creatingParentsIfNeeded().forPath(
                "/active-master",
                "not-json".getBytes(StandardCharsets.UTF_8));

        Assertions.assertEquals("fallback:8080", SqlClient.readActiveMaster(zkClient, "fallback:8080"));
    }
}
