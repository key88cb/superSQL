package edu.zju.supersql.master.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.master.balance.LoadBalancer;
import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.testutil.EmbeddedZkServer;
import edu.zju.supersql.testutil.EmbeddedZkServerFactory;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class MasterServiceMetadataIntegrationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private EmbeddedZkServer server;
    private CuratorFramework zkClient;
    private MasterServiceImpl service;
    private RecordingRegionDdlExecutor ddlExecutor;
    private RecordingRegionAdminExecutor adminExecutor;

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

        createPathIfMissing("/region_servers");
        createPathIfMissing("/meta/tables");
        createPathIfMissing("/assignments");
        createPathIfMissing("/active-master");

        MasterRuntimeContext.initialize(zkClient, "master-1", 8080);
        writeActiveMaster("master-1", "master-1:8080");

        ddlExecutor = new RecordingRegionDdlExecutor();
        adminExecutor = new RecordingRegionAdminExecutor();
        service = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor);
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
        Assertions.assertEquals(2, ddlExecutor.commands.size());

        List<TableLocation> tables = service.listTables();
        Assertions.assertEquals(1, tables.size());

        Response dropResp = service.dropTable("orders");
        Assertions.assertEquals(StatusCode.OK, dropResp.getCode());
        Assertions.assertTrue(ddlExecutor.commands.stream().anyMatch(cmd -> cmd.sql.startsWith("drop table orders")));

        TableLocation dropped = service.getTableLocation("orders");
        Assertions.assertEquals("TABLE_NOT_FOUND", dropped.getTableStatus());
    }

    @Test
    void createTableShouldSelectUpToThreeLeastLoadedReplicasAndPersistAssignment() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 9);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 3);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        Response response = service.createTable("create table t_assign(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, response.getCode());

        TableLocation location = service.getTableLocation("t_assign");
        Assertions.assertEquals("rs-2", location.getPrimaryRS().getId());
        Assertions.assertEquals(3, location.getReplicasSize());
        Assertions.assertEquals(List.of("rs-2", "rs-4", "rs-3"),
                location.getReplicas().stream().map(rs -> rs.getId()).toList());

        Map<?, ?> assignment = readJson("/assignments/t_assign");
        Assertions.assertEquals("t_assign", assignment.get("tableName"));
        List<?> replicas = (List<?>) assignment.get("replicas");
        Assertions.assertEquals(3, replicas.size());

        Map<?, ?> tableMeta = readJson("/meta/tables/t_assign");
        Assertions.assertTrue(toLong(tableMeta.get("statusUpdatedAt")) > 0L);
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
    void getTableLocationOnStandbyShouldReturnRedirectPlaceholder() throws Exception {
        writeActiveMaster("master-2", "master-2:8081");

        TableLocation location = service.getTableLocation("t_standby");

        Assertions.assertEquals("NOT_LEADER", location.getTableStatus());
        Assertions.assertEquals("master-2:8081", location.getPrimaryRS().getHost());
        Assertions.assertEquals(-1L, location.getVersion());
    }

    @Test
    void listMethodsShouldReturnEmptyWhenNoData() throws Exception {
        List<TableLocation> tables = service.listTables();
        Assertions.assertTrue(tables.isEmpty());

        Assertions.assertTrue(service.listRegionServers().isEmpty());
    }

    @Test
    void dropMissingTableShouldReturnTableNotFound() throws Exception {
        Response response = service.dropTable("missing_table");
        Assertions.assertEquals(StatusCode.TABLE_NOT_FOUND, response.getCode());
    }

    @Test
    void createTableShouldReturnRsNotFoundWithoutRegionServer() throws Exception {
        Response response = service.createTable("create table t_no_rs(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.RS_NOT_FOUND, response.getCode());
    }

    @Test
    void createTableShouldNotPersistMetadataWhenRemoteDdlFails() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        ddlExecutor.failOnReplica("rs-1", StatusCode.ERROR, "engine rejected ddl");

        Response response = service.createTable("create table t_fail(id int, primary key(id));");

        Assertions.assertEquals(StatusCode.ERROR, response.getCode());
        Assertions.assertTrue(response.getMessage().contains("rs-1"));
        Assertions.assertEquals("TABLE_NOT_FOUND", service.getTableLocation("t_fail").getTableStatus());
        Assertions.assertTrue(zkClient.checkExists().forPath("/meta/tables/t_fail") == null);
        Assertions.assertTrue(zkClient.checkExists().forPath("/assignments/t_fail") == null);
    }

    @Test
    void dropTableShouldKeepMetadataWhenRemoteDropFails() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);

        Response create = service.createTable("create table t_drop_fail(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        ddlExecutor.failOnSqlPrefix("drop table t_drop_fail", StatusCode.ERROR, "drop failed");
        Response drop = service.dropTable("t_drop_fail");

        Assertions.assertEquals(StatusCode.ERROR, drop.getCode());
        Assertions.assertNotNull(service.getTableLocation("t_drop_fail"));
        Assertions.assertNotNull(zkClient.checkExists().forPath("/meta/tables/t_drop_fail"));
    }

    @Test
    void getActiveMasterShouldReturnAddressFromZooKeeper() throws Exception {
        writeActiveMaster("master-9", "master-9:8090");
        Assertions.assertEquals("master-9:8090", service.getActiveMaster());
    }

    @Test
    void getTableLocationShouldPromoteOnlineReplicaWhenPrimaryOffline() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_failover_primary(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        TableLocation before = service.getTableLocation("t_failover_primary");
        Assertions.assertEquals("rs-1", before.getPrimaryRS().getId());

        zkClient.delete().forPath("/region_servers/rs-1");

        TableLocation after = service.getTableLocation("t_failover_primary");
        Assertions.assertEquals("rs-2", after.getPrimaryRS().getId());
        Assertions.assertEquals("ACTIVE", after.getTableStatus());

        Map<?, ?> meta = readJson("/meta/tables/t_failover_primary");
        Map<?, ?> primary = (Map<?, ?>) meta.get("primaryRS");
        Assertions.assertEquals("rs-2", String.valueOf(primary.get("id")));
        Assertions.assertTrue(toLong(meta.get("statusUpdatedAt")) > 0L);
    }

    @Test
    void listTablesShouldPromoteOnlineReplicaWhenPrimaryOffline() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_failover_list(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");

        List<TableLocation> tables = service.listTables();
        TableLocation target = tables.stream()
                .filter(t -> "t_failover_list".equals(t.getTableName()))
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("rs-2", target.getPrimaryRS().getId());
        Assertions.assertEquals("ACTIVE", target.getTableStatus());

        Map<?, ?> meta = readJson("/meta/tables/t_failover_list");
        Map<?, ?> primary = (Map<?, ?>) meta.get("primaryRS");
        Assertions.assertEquals("rs-2", String.valueOf(primary.get("id")));
    }

    @Test
    void triggerRebalanceShouldMoveNonPrimaryReplicaToLeastLoadedNode() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 10);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        Response rebalance = service.triggerRebalance();

        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());

        TableLocation location = service.getTableLocation("t_rebalance");
        Assertions.assertEquals(List.of("rs-1", "rs-3", "rs-4"),
                location.getReplicas().stream().map(RegionServerInfo::getId).toList());
        Assertions.assertTrue(adminExecutor.operations.stream().anyMatch(op -> op.method.equals("pause")));
        Assertions.assertTrue(adminExecutor.operations.stream().anyMatch(op -> op.method.equals("transfer")));
        Assertions.assertTrue(adminExecutor.operations.stream().anyMatch(op -> op.method.equals("delete")));
        Assertions.assertTrue(adminExecutor.operations.stream().anyMatch(op -> op.method.equals("resume")));
    }

    @Test
    void triggerRebalanceShouldExposeMovingStatusDuringTransfer() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance_state(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 9);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        AtomicReference<String> observedStatus = new AtomicReference<>();
        RecordingRegionAdminExecutor observingAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    observedStatus.set(String.valueOf(meta.get("tableStatus")));
                } catch (Exception e) {
                    observedStatus.set("READ_ERROR");
                }
                return super.transferTable(source, tableName, target);
            }
        };

        MasterServiceImpl observingService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                observingAdmin);

        Response rebalance = observingService.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());
        Assertions.assertEquals("MOVING", observedStatus.get());

        TableLocation location = observingService.getTableLocation("t_rebalance_state");
        Assertions.assertEquals("ACTIVE", location.getTableStatus());
    }

    @Test
    void triggerRebalanceShouldExposePreparingStatusBeforePause() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance_prepare(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 9);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        AtomicReference<String> observedStatus = new AtomicReference<>();
        RecordingRegionAdminExecutor observingAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    observedStatus.set(String.valueOf(meta.get("tableStatus")));
                } catch (Exception e) {
                    observedStatus.set("READ_ERROR");
                }
                return super.pauseTableWrite(regionServer, tableName);
            }
        };

        MasterServiceImpl observingService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                observingAdmin);

        Response rebalance = observingService.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());
        Assertions.assertEquals("PREPARING", observedStatus.get());

        TableLocation location = observingService.getTableLocation("t_rebalance_prepare");
        Assertions.assertEquals("ACTIVE", location.getTableStatus());
    }

    @Test
    void triggerRebalanceShouldRefreshStatusUpdatedAtAcrossTransitions() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance_status_ts(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 9);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        AtomicReference<String> preparingStatus = new AtomicReference<>();
        AtomicReference<String> movingStatus = new AtomicReference<>();
        AtomicLong preparingTs = new AtomicLong(0L);
        AtomicLong movingTs = new AtomicLong(0L);

        RecordingRegionAdminExecutor observingAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    preparingStatus.set(String.valueOf(meta.get("tableStatus")));
                    preparingTs.set(toLong(meta.get("statusUpdatedAt")));
                } catch (Exception e) {
                    preparingStatus.set("READ_ERROR");
                }
                return super.pauseTableWrite(regionServer, tableName);
            }

            @Override
            public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    movingStatus.set(String.valueOf(meta.get("tableStatus")));
                    movingTs.set(toLong(meta.get("statusUpdatedAt")));
                } catch (Exception e) {
                    movingStatus.set("READ_ERROR");
                }
                return super.transferTable(source, tableName, target);
            }
        };

        MasterServiceImpl observingService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                observingAdmin);

        Response rebalance = observingService.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_rebalance_status_ts");
        long finalTs = toLong(finalMeta.get("statusUpdatedAt"));

        Assertions.assertEquals("PREPARING", preparingStatus.get());
        Assertions.assertEquals("MOVING", movingStatus.get());
        Assertions.assertEquals("ACTIVE", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertTrue(preparingTs.get() > 0L);
        Assertions.assertTrue(movingTs.get() >= preparingTs.get());
        Assertions.assertTrue(finalTs >= movingTs.get());
    }

    @Test
    void triggerRebalanceShouldExposeAndClearMigrationAttemptId() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance_attempt(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 9);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        AtomicReference<String> preparingAttempt = new AtomicReference<>();
        AtomicReference<String> movingAttempt = new AtomicReference<>();

        RecordingRegionAdminExecutor observingAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    preparingAttempt.set(String.valueOf(meta.get("migrationAttemptId")));
                } catch (Exception e) {
                    preparingAttempt.set("READ_ERROR");
                }
                return super.pauseTableWrite(regionServer, tableName);
            }

            @Override
            public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    movingAttempt.set(String.valueOf(meta.get("migrationAttemptId")));
                } catch (Exception e) {
                    movingAttempt.set("READ_ERROR");
                }
                return super.transferTable(source, tableName, target);
            }
        };

        MasterServiceImpl observingService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                observingAdmin);

        Response rebalance = observingService.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());
        Assertions.assertNotNull(preparingAttempt.get());
        Assertions.assertFalse(preparingAttempt.get().isBlank());
        Assertions.assertEquals(preparingAttempt.get(), movingAttempt.get());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_rebalance_attempt");
        Assertions.assertFalse(finalMeta.containsKey("migrationAttemptId"));
    }

    @Test
    void triggerRebalanceShouldClearMigrationAttemptIdWhenTransferFails() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance_attempt_fail(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 9);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        RecordingRegionAdminExecutor failingAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
                Response response = new Response(StatusCode.ERROR);
                response.setMessage("simulated transfer failure");
                return response;
            }
        };

        MasterServiceImpl failingService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                failingAdmin);

        Response rebalance = failingService.triggerRebalance();
        Assertions.assertEquals(StatusCode.ERROR, rebalance.getCode());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_rebalance_attempt_fail");
        Assertions.assertEquals("ACTIVE", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertFalse(finalMeta.containsKey("migrationAttemptId"));
    }

    @Test
    void triggerRebalanceShouldSkipTableWhenStatusIsNotActive() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_non_active(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_non_active").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "MOVING");
        zkClient.setData().forPath("/meta/tables/t_non_active",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 10);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        Response rebalance = service.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());
        Assertions.assertTrue(rebalance.getMessage().startsWith("Rebalance skipped:"));
        Assertions.assertTrue(adminExecutor.operations.isEmpty());
    }

    private static long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
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

    private Map<?, ?> readJson(String path) throws Exception {
        byte[] bytes = zkClient.getData().forPath(path);
        return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
    }

    private static class RecordingRegionDdlExecutor implements RegionDdlExecutor {

        private final List<Command> commands = new ArrayList<>();
        private final Map<String, Response> byReplica = new HashMap<>();
        private final Map<String, Response> bySqlPrefix = new HashMap<>();

        @Override
        public Response execute(RegionServerInfo regionServer, String tableName, String ddl) {
            commands.add(new Command(regionServer.getId(), tableName, ddl));
            Response replicaResponse = byReplica.get(regionServer.getId());
            if (replicaResponse != null) {
                return replicaResponse;
            }
            for (Map.Entry<String, Response> entry : bySqlPrefix.entrySet()) {
                if (ddl.toLowerCase().startsWith(entry.getKey().toLowerCase())) {
                    return entry.getValue();
                }
            }
            return new Response(StatusCode.OK);
        }

        void failOnReplica(String replicaId, StatusCode code, String message) {
            Response response = new Response(code);
            response.setMessage(message);
            byReplica.put(replicaId, response);
        }

        void failOnSqlPrefix(String sqlPrefix, StatusCode code, String message) {
            Response response = new Response(code);
            response.setMessage(message);
            bySqlPrefix.put(sqlPrefix, response);
        }
    }

    private record Command(String replicaId, String tableName, String sql) {
    }

    private static class RecordingRegionAdminExecutor implements RegionAdminExecutor {

        private final List<AdminOperation> operations = new ArrayList<>();

        @Override
        public Response pauseTableWrite(RegionServerInfo regionServer, String tableName) {
            operations.add(new AdminOperation("pause", regionServer.getId(), tableName, null));
            return new Response(StatusCode.OK);
        }

        @Override
        public Response resumeTableWrite(RegionServerInfo regionServer, String tableName) {
            operations.add(new AdminOperation("resume", regionServer.getId(), tableName, null));
            return new Response(StatusCode.OK);
        }

        @Override
        public Response transferTable(RegionServerInfo source, String tableName, RegionServerInfo target) {
            operations.add(new AdminOperation("transfer", source.getId(), tableName, target.getId()));
            return new Response(StatusCode.OK);
        }

        @Override
        public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) {
            operations.add(new AdminOperation("delete", regionServer.getId(), tableName, null));
            return new Response(StatusCode.OK);
        }

        @Override
        public Response invalidateClientCache(RegionServerInfo regionServer, String tableName) {
            operations.add(new AdminOperation("invalidate", regionServer.getId(), tableName, null));
            return new Response(StatusCode.OK);
        }
    }

    private record AdminOperation(String method, String replicaId, String tableName, String targetReplicaId) {
    }
}
