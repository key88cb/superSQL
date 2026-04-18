package edu.zju.supersql.client;

import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

class SqlClientRoutingTest {

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
    void classifySqlShouldDetectDdlAndDml() {
        Assertions.assertEquals(SqlClient.SqlKind.DDL, SqlClient.classifySql("create table t1(id int);"));
        Assertions.assertEquals(SqlClient.SqlKind.DML, SqlClient.classifySql("insert into t1 values (1);"));
        Assertions.assertEquals(SqlClient.SqlKind.SHOW_TABLES, SqlClient.classifySql("show tables;"));
        Assertions.assertEquals(SqlClient.SqlKind.SHOW_ROUTING_METRICS,
                SqlClient.classifySql("show routing metrics;"));
    }

    @Test
    void extractTableNameShouldWorkForInsertAndSelect() {
        Assertions.assertEquals("t_order", SqlClient.extractTableName("insert into t_order values (1);"));
        Assertions.assertEquals("t_order", SqlClient.extractTableName("select * from t_order where id = 1;"));
        Assertions.assertEquals("t_order", SqlClient.extractTableName("update t_order set id = 2 where id = 1;"));
    }

    @Test
    void routeCacheShouldRespectVersionInvalidation() {
        RouteCache cache = new RouteCache(60_000);
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("t_order", primary, Collections.singletonList(primary));
        location.setVersion(1L);

        cache.put("t_order", location);
        Assertions.assertNotNull(cache.get("t_order"));

        cache.invalidateIfVersionMismatch("t_order", 2L);
        Assertions.assertNull(cache.get("t_order"));
    }

    @Test
    void routeCacheShouldExpireByTtl() throws InterruptedException {
        RouteCache cache = new RouteCache(10);
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("t_expire", primary, Collections.singletonList(primary));

        cache.put("t_expire", location);
        Thread.sleep(20);

        Assertions.assertNull(cache.get("t_expire"));
    }

    @Test
    void readActiveMasterShouldReturnAddressWhenPresent() throws Exception {
        zkClient.create().creatingParentsIfNeeded().forPath(
                "/active-master",
                "{\"masterId\":\"master-1\",\"address\":\"master-1:8080\"}".getBytes(StandardCharsets.UTF_8));

        String master = SqlClient.readActiveMaster(zkClient, "fallback:8080");
        Assertions.assertEquals("master-1:8080", master);
    }

    @Test
    void readActiveMasterShouldFallbackWhenNodeMissing() {
        String master = SqlClient.readActiveMaster(zkClient, "fallback:8080");
        Assertions.assertEquals("fallback:8080", master);
    }

}
