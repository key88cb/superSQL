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
import java.util.List;

class RouteInvalidationWatcherIntegrationTest {

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;
    private RouteInvalidationWatcher watcher;

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

        if (zkClient.checkExists().forPath(ZkPaths.META_TABLES) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(ZkPaths.META_TABLES, new byte[0]);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (watcher != null) {
            watcher.close();
        }
        if (zkClient != null) {
            zkClient.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void shouldInvalidateRouteCacheWhenTableMetaChanged() throws Exception {
        RouteCache routeCache = new RouteCache(60_000);
        RegionServerInfo rs = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        routeCache.put("orders", new TableLocation("orders", rs, List.of(rs)));

        watcher = RouteInvalidationWatcher.start(zkClient, routeCache);

        String path = ZkPaths.tableMeta("orders");
        zkClient.create().creatingParentsIfNeeded().forPath(path, "v1".getBytes(StandardCharsets.UTF_8));

        waitUntilInvalidated(routeCache, "orders");
        Assertions.assertNull(routeCache.get("orders"));
    }

    @Test
    void shouldExtractTableNameFromMetaPath() {
        Assertions.assertEquals("orders", RouteInvalidationWatcher.extractTableNameFromMetaPath("/meta/tables/orders"));
        Assertions.assertNull(RouteInvalidationWatcher.extractTableNameFromMetaPath("/meta/tables"));
        Assertions.assertNull(RouteInvalidationWatcher.extractTableNameFromMetaPath("/meta/tables/orders/child"));
        Assertions.assertNull(RouteInvalidationWatcher.extractTableNameFromMetaPath("/other/orders"));
    }

    private static void waitUntilInvalidated(RouteCache routeCache, String tableName) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3_000;
        while (System.currentTimeMillis() < deadline) {
            if (routeCache.get(tableName) == null) {
                return;
            }
            Thread.sleep(50);
        }
        Assertions.fail("route cache was not invalidated in time for table " + tableName);
    }
}
