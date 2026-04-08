package edu.zju.supersql.master.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MasterServiceMetadataIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private TestingServer server;
    private CuratorFramework zkClient;
    private MasterServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        server = new TestingServer(true);
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(200, 3))
                .sessionTimeoutMs(10_000)
                .connectionTimeoutMs(5_000)
                .namespace("supersql")
                .build();
        zkClient.start();
        zkClient.blockUntilConnected();

        createPathIfMissing("/region_servers");
        createPathIfMissing("/meta/tables");
        createPathIfMissing("/assignments");
        createPathIfMissing("/active-master");

        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
        writeActiveMaster("master-1", "master-1:8080");

        service = new MasterServiceImpl();
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
    void createGetListDropRoundTripShouldWork() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 5);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);

        Response createResp = service.createTable("create table orders(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, createResp.getCode());

        TableLocation location = service.getTableLocation("orders");
        Assertions.assertEquals("orders", location.getTableName());
        Assertions.assertEquals("rs-2", location.getPrimaryRS().getId());
        Assertions.assertEquals(2, location.getReplicasSize());

        List<TableLocation> tables = service.listTables();
        Assertions.assertEquals(1, tables.size());

        Response dropResp = service.dropTable("orders");
        Assertions.assertEquals(StatusCode.OK, dropResp.getCode());

        TableLocation dropped = service.getTableLocation("orders");
        Assertions.assertEquals("TABLE_NOT_FOUND", dropped.getTableStatus());
    }

    @Test
    void createExistingTableShouldReturnTableExists() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);

        Response first = service.createTable("create table t_exist(id int, primary key(id));");
        Response second = service.createTable("create table t_exist(id int, primary key(id));");

        Assertions.assertEquals(StatusCode.OK, first.getCode());
        Assertions.assertEquals(StatusCode.TABLE_EXISTS, second.getCode());
    }

    @Test
    void createTableOnStandbyShouldReturnNotLeader() throws Exception {
        writeActiveMaster("master-2", "master-2:8081");

        Response response = service.createTable("create table t_standby(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.NOT_LEADER, response.getCode());
        Assertions.assertEquals("master-2:8081", response.getRedirectTo());
    }

    @Test
    void listMethodsShouldReturnEmptyWhenNoData() throws Exception {
        List<TableLocation> tables = service.listTables();
        Assertions.assertTrue(tables.isEmpty());

        Assertions.assertTrue(service.listRegionServers().isEmpty());
    }

    @Test
    void createTableShouldReturnRsNotFoundWithoutRegionServer() throws Exception {
        Response response = service.createTable("create table t_no_rs(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.RS_NOT_FOUND, response.getCode());
    }

    private void createPathIfMissing(String path) throws Exception {
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

    private void registerRegionServer(String id, String host, int port, int tableCount) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("id", id);
        payload.put("host", host);
        payload.put("port", port);
        payload.put("tableCount", tableCount);
        payload.put("qps1min", 0.0);
        payload.put("cpuUsage", 0.0);
        payload.put("memUsage", 0.0);
        payload.put("lastHeartbeat", System.currentTimeMillis());

        String path = "/region_servers/" + id;
        byte[] bytes = MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().withMode(CreateMode.EPHEMERAL).forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }
}
