package edu.zju.supersql.client;

import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.TableLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class SqlClientRoutingTest {

    @Test
    void classifySqlShouldDetectDdlAndDml() {
        Assertions.assertEquals(SqlClient.SqlKind.DDL, SqlClient.classifySql("create table t1(id int);"));
        Assertions.assertEquals(SqlClient.SqlKind.DML, SqlClient.classifySql("insert into t1 values (1);"));
        Assertions.assertEquals(SqlClient.SqlKind.UNKNOWN, SqlClient.classifySql("show tables;"));
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
}
