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
    void getTableLocationShouldRefillReplicasAfterPrimaryOffline() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_failover_refill(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");

        TableLocation after = service.getTableLocation("t_failover_refill");
        List<String> replicaIds = after.getReplicas().stream().map(RegionServerInfo::getId).toList();
        Assertions.assertTrue(List.of("rs-2", "rs-3", "rs-4").contains(after.getPrimaryRS().getId()));
        Assertions.assertEquals(3, after.getReplicasSize());
        Assertions.assertTrue(replicaIds.contains("rs-2"));
        Assertions.assertTrue(replicaIds.contains("rs-3"));
        Assertions.assertTrue(replicaIds.contains("rs-4"));
        Assertions.assertTrue(adminExecutor.operations.stream().anyMatch(op ->
            "transfer".equals(op.method)
                && "t_failover_refill".equals(op.tableName)
                && op.targetReplicaId != null));
    }

    @Test
    void getTableLocationShouldKeepReducedReplicasWhenTransferFails() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_refill_transfer_fail(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        adminExecutor.failAnyTransfer();
        zkClient.delete().forPath("/region_servers/rs-1");

        TableLocation after = service.getTableLocation("t_refill_transfer_fail");
        List<String> replicaIds = after.getReplicas().stream().map(RegionServerInfo::getId).toList();

        Assertions.assertEquals("ACTIVE", after.getTableStatus());
        Assertions.assertEquals(2, after.getReplicasSize());
        Assertions.assertFalse(replicaIds.contains("rs-1"));
        Assertions.assertTrue(replicaIds.contains(after.getPrimaryRS().getId()));
        Assertions.assertTrue(replicaIds.stream().allMatch(id -> List.of("rs-2", "rs-3", "rs-4").contains(id)));
        Assertions.assertTrue(adminExecutor.operations.stream().anyMatch(op ->
            "transfer".equals(op.method)
                && "t_refill_transfer_fail".equals(op.tableName)));

        Map<?, ?> meta = readJson("/meta/tables/t_refill_transfer_fail");
        List<?> persistedReplicas = (List<?>) meta.get("replicas");
        Assertions.assertEquals(2, persistedReplicas.size());
    }

    @Test
    void listTablesShouldRefillReplicasAfterPrimaryOffline() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_failover_list_refill(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");

        TableLocation table = service.listTables().stream()
                .filter(t -> "t_failover_list_refill".equals(t.getTableName()))
                .findFirst()
                .orElseThrow();
        List<String> replicaIds = table.getReplicas().stream().map(RegionServerInfo::getId).toList();

        Assertions.assertTrue(List.of("rs-2", "rs-3", "rs-4").contains(table.getPrimaryRS().getId()));
        Assertions.assertEquals(3, table.getReplicasSize());
        Assertions.assertTrue(replicaIds.contains("rs-2"));
        Assertions.assertTrue(replicaIds.contains("rs-3"));
        Assertions.assertTrue(replicaIds.contains("rs-4"));

        Map<?, ?> assignment = readJson("/assignments/t_failover_list_refill");
        List<?> replicas = (List<?>) assignment.get("replicas");
        Assertions.assertEquals(3, replicas.size());
    }

    @Test
    void getTableLocationShouldMarkUnavailableWhenAllReplicasOffline() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_unavailable(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");
        zkClient.delete().forPath("/region_servers/rs-2");
        zkClient.delete().forPath("/region_servers/rs-3");

        TableLocation location = service.getTableLocation("t_unavailable");
        Assertions.assertEquals("UNAVAILABLE", location.getTableStatus());

        Map<?, ?> meta = readJson("/meta/tables/t_unavailable");
        Assertions.assertEquals("UNAVAILABLE", String.valueOf(meta.get("tableStatus")));
    }

    @Test
    void getTableLocationShouldRecoverToActiveWhenReplicaBackOnline() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_unavailable_recover(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");
        zkClient.delete().forPath("/region_servers/rs-2");
        zkClient.delete().forPath("/region_servers/rs-3");
        Assertions.assertEquals("UNAVAILABLE", service.getTableLocation("t_unavailable_recover").getTableStatus());

        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);

        TableLocation recovered = service.getTableLocation("t_unavailable_recover");
        Assertions.assertEquals("ACTIVE", recovered.getTableStatus());
        Assertions.assertEquals("rs-2", recovered.getPrimaryRS().getId());

        Map<?, ?> meta = readJson("/meta/tables/t_unavailable_recover");
        Assertions.assertEquals("ACTIVE", String.valueOf(meta.get("tableStatus")));
    }

    @Test
    void getTableLocationShouldThrottleRepeatedHealWrites() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_heal_throttle(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        AtomicLong clock = new AtomicLong(1_000L);
        MasterServiceImpl throttledService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor,
                clock::get,
                10_000L);

        zkClient.delete().forPath("/region_servers/rs-1");
        TableLocation firstHeal = throttledService.getTableLocation("t_heal_throttle");
        Assertions.assertEquals("rs-2", firstHeal.getPrimaryRS().getId());

        forcePrimaryInMeta("t_heal_throttle", "rs-1", 9090);

        clock.set(2_000L);
        TableLocation secondHeal = throttledService.getTableLocation("t_heal_throttle");
        Assertions.assertEquals("rs-2", secondHeal.getPrimaryRS().getId());

        Map<?, ?> throttledMeta = readJson("/meta/tables/t_heal_throttle");
        Map<?, ?> throttledPrimary = (Map<?, ?>) throttledMeta.get("primaryRS");
        Assertions.assertEquals("rs-1", String.valueOf(throttledPrimary.get("id")));

        clock.set(20_000L);
        TableLocation thirdHeal = throttledService.getTableLocation("t_heal_throttle");
        Assertions.assertEquals("rs-2", thirdHeal.getPrimaryRS().getId());

        Map<?, ?> persistedMeta = readJson("/meta/tables/t_heal_throttle");
        Map<?, ?> persistedPrimary = (Map<?, ?>) persistedMeta.get("primaryRS");
        Assertions.assertEquals("rs-2", String.valueOf(persistedPrimary.get("id")));
    }

    @Test
    void repairTableRoutesBestEffortShouldHealOfflinePrimaryWithoutReadPath() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_background_repair(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        zkClient.delete().forPath("/region_servers/rs-1");
        int repaired = service.repairTableRoutesBestEffort();

        Assertions.assertTrue(repaired >= 1);

        Map<?, ?> meta = readJson("/meta/tables/t_background_repair");
        Map<?, ?> primary = (Map<?, ?>) meta.get("primaryRS");
        Assertions.assertEquals("rs-2", String.valueOf(primary.get("id")));
        Assertions.assertEquals("ACTIVE", String.valueOf(meta.get("tableStatus")));

        MasterServiceImpl.RouteRepairSnapshot snapshot = service.routeRepairSnapshot();
        Assertions.assertTrue(snapshot.runCount() >= 1);
        Assertions.assertTrue(snapshot.totalRepairedTables() >= 1);
        Assertions.assertTrue(snapshot.lastRunAtMs() > 0L);
        Assertions.assertTrue(snapshot.lastRunRepairedCount() >= 1);
        Assertions.assertTrue(snapshot.lastRunTotalTables() >= 1);
        Assertions.assertTrue(snapshot.lastRunCandidateTables() >= 1);
        Assertions.assertNull(snapshot.lastRunFilterRegionServerId());
        Assertions.assertEquals("t_background_repair", snapshot.lastRepairedTable());
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
    void triggerRebalanceShouldExposeFinalizingStatusBeforeSourceCleanup() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 2);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 3);

        Response create = service.createTable("create table t_rebalance_finalizing(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 9);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 1);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 2);

        AtomicReference<String> observedStatus = new AtomicReference<>();
        RecordingRegionAdminExecutor observingAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) {
                try {
                    Map<?, ?> meta = readJson("/meta/tables/" + tableName);
                    observedStatus.set(String.valueOf(meta.get("tableStatus")));
                } catch (Exception e) {
                    observedStatus.set("READ_ERROR");
                }
                return super.deleteLocalTable(regionServer, tableName);
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
        Assertions.assertEquals("FINALIZING", observedStatus.get());

        TableLocation location = observingService.getTableLocation("t_rebalance_finalizing");
        Assertions.assertEquals("ACTIVE", location.getTableStatus());
    }

        @Test
        void getTableLocationShouldRecoverStuckFinalizingAndCleanupSourceReplica() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_stuck_finalizing_recover(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_finalizing_recover").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "FINALIZING");
        meta.put("migrationAttemptId", "attempt-finalizing");
        meta.put("migrationSourceReplicaId", "rs-2");
        meta.put("migrationTargetReplicaId", "rs-4");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_finalizing_recover",
            MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        RecordingRegionAdminExecutor recoveringAdmin = new RecordingRegionAdminExecutor();
        MasterServiceImpl recoveringService = new MasterServiceImpl(
            new MetaManager(zkClient),
            new AssignmentManager(zkClient),
            new LoadBalancer(),
            ddlExecutor,
            recoveringAdmin,
            clock::get,
            1_000L,
            10,
            5_000L);

        TableLocation recovered = recoveringService.getTableLocation("t_stuck_finalizing_recover");
        Assertions.assertEquals("ACTIVE", recovered.getTableStatus());
        Assertions.assertTrue(recoveringAdmin.operations.stream().anyMatch(op ->
            "delete".equals(op.method())
                && "rs-2".equals(op.replicaId())
                && "t_stuck_finalizing_recover".equals(op.tableName())));

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_finalizing_recover");
        Assertions.assertEquals("ACTIVE", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertFalse(finalMeta.containsKey("migrationAttemptId"));
        Assertions.assertFalse(finalMeta.containsKey("migrationSourceReplicaId"));
        Assertions.assertFalse(finalMeta.containsKey("migrationTargetReplicaId"));
        }

        @Test
        void getTableLocationShouldRecoverStuckMovingAndCleanupTargetReplica() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_stuck_moving_recover(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_moving_recover").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "MOVING");
        meta.put("migrationAttemptId", "attempt-moving");
        meta.put("migrationSourceReplicaId", "rs-2");
        meta.put("migrationTargetReplicaId", "rs-4");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_moving_recover",
            MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        RecordingRegionAdminExecutor recoveringAdmin = new RecordingRegionAdminExecutor();
        MasterServiceImpl recoveringService = new MasterServiceImpl(
            new MetaManager(zkClient),
            new AssignmentManager(zkClient),
            new LoadBalancer(),
            ddlExecutor,
            recoveringAdmin,
            clock::get,
            1_000L,
            10,
            5_000L);

        TableLocation recovered = recoveringService.getTableLocation("t_stuck_moving_recover");
        Assertions.assertEquals("ACTIVE", recovered.getTableStatus());
        Assertions.assertTrue(recoveringAdmin.operations.stream().anyMatch(op ->
            "delete".equals(op.method())
                && "rs-4".equals(op.replicaId())
                && "t_stuck_moving_recover".equals(op.tableName())));

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_moving_recover");
        Assertions.assertEquals("ACTIVE", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertFalse(finalMeta.containsKey("migrationAttemptId"));
        Assertions.assertFalse(finalMeta.containsKey("migrationSourceReplicaId"));
        Assertions.assertFalse(finalMeta.containsKey("migrationTargetReplicaId"));
        }

    @Test
    void getTableLocationShouldNotRecoverStuckFinalizingWhenSourceCleanupFails() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_stuck_finalizing_blocked(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_finalizing_blocked").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "FINALIZING");
        meta.put("migrationAttemptId", "attempt-finalizing-blocked");
        meta.put("migrationSourceReplicaId", "rs-2");
        meta.put("migrationTargetReplicaId", "rs-4");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_finalizing_blocked",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        RecordingRegionAdminExecutor blockedAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) {
                Response fail = new Response(StatusCode.ERROR);
                fail.setMessage("simulated source cleanup failure");
                return fail;
            }
        };

        MasterServiceImpl blockedService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                blockedAdmin,
                clock::get,
                1_000L,
                10,
                5_000L);

        TableLocation recovered = blockedService.getTableLocation("t_stuck_finalizing_blocked");
        Assertions.assertEquals("FINALIZING", recovered.getTableStatus());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_finalizing_blocked");
        Assertions.assertEquals("FINALIZING", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertEquals("attempt-finalizing-blocked", String.valueOf(finalMeta.get("migrationAttemptId")));
    }

    @Test
    void getTableLocationShouldNotRecoverStuckMovingWhenTargetCleanupFails() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);
        registerRegionServer("rs-4", "127.0.0.1", 9093, 0);

        Response create = service.createTable("create table t_stuck_moving_blocked(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_moving_blocked").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "MOVING");
        meta.put("migrationAttemptId", "attempt-moving-blocked");
        meta.put("migrationSourceReplicaId", "rs-2");
        meta.put("migrationTargetReplicaId", "rs-4");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_moving_blocked",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        RecordingRegionAdminExecutor blockedAdmin = new RecordingRegionAdminExecutor() {
            @Override
            public Response deleteLocalTable(RegionServerInfo regionServer, String tableName) {
                if ("rs-4".equals(regionServer.getId())) {
                    Response fail = new Response(StatusCode.ERROR);
                    fail.setMessage("simulated target cleanup failure");
                    return fail;
                }
                return super.deleteLocalTable(regionServer, tableName);
            }
        };

        MasterServiceImpl blockedService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                blockedAdmin,
                clock::get,
                1_000L,
                10,
                5_000L);

        TableLocation recovered = blockedService.getTableLocation("t_stuck_moving_blocked");
        Assertions.assertEquals("MOVING", recovered.getTableStatus());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_moving_blocked");
        Assertions.assertEquals("MOVING", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertEquals("attempt-moving-blocked", String.valueOf(finalMeta.get("migrationAttemptId")));
    }

    @Test
    void getTableLocationShouldNotRecoverStuckFinalizingWhenSourceReplicaMissing() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_stuck_finalizing_missing_source(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_finalizing_missing_source").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "FINALIZING");
        meta.put("migrationAttemptId", "attempt-finalizing-missing-source");
        meta.put("migrationSourceReplicaId", "rs-missing");
        meta.put("migrationTargetReplicaId", "rs-4");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_finalizing_missing_source",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        MasterServiceImpl blockedService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor,
                clock::get,
                1_000L,
                10,
                5_000L);

        TableLocation recovered = blockedService.getTableLocation("t_stuck_finalizing_missing_source");
        Assertions.assertEquals("FINALIZING", recovered.getTableStatus());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_finalizing_missing_source");
        Assertions.assertEquals("FINALIZING", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertEquals("attempt-finalizing-missing-source", String.valueOf(finalMeta.get("migrationAttemptId")));
    }

    @Test
    void getTableLocationShouldNotRecoverStuckMovingWhenTargetReplicaMissing() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);
        registerRegionServer("rs-3", "127.0.0.1", 9092, 2);

        Response create = service.createTable("create table t_stuck_moving_missing_target(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_moving_missing_target").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "MOVING");
        meta.put("migrationAttemptId", "attempt-moving-missing-target");
        meta.put("migrationSourceReplicaId", "rs-2");
        meta.put("migrationTargetReplicaId", "rs-missing");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_moving_missing_target",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        MasterServiceImpl blockedService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor,
                clock::get,
                1_000L,
                10,
                5_000L);

        TableLocation recovered = blockedService.getTableLocation("t_stuck_moving_missing_target");
        Assertions.assertEquals("MOVING", recovered.getTableStatus());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_moving_missing_target");
        Assertions.assertEquals("MOVING", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertEquals("attempt-moving-missing-target", String.valueOf(finalMeta.get("migrationAttemptId")));
    }

    @Test
    void getTableLocationShouldRecoverStuckMovingStatusToActive() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 0);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);

        Response create = service.createTable("create table t_stuck_recover(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_recover").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "MOVING");
        meta.put("migrationAttemptId", "attempt-stuck");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_recover",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        MasterServiceImpl recoveringService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor,
                clock::get,
                1_000L,
                10,
                5_000L);

        TableLocation recovered = recoveringService.getTableLocation("t_stuck_recover");
        Assertions.assertEquals("ACTIVE", recovered.getTableStatus());

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_recover");
        Assertions.assertEquals("ACTIVE", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertFalse(finalMeta.containsKey("migrationAttemptId"));
    }

    @Test
    void triggerRebalanceShouldProactivelyRecoverStuckMovingStatus() throws Exception {
        registerRegionServer("rs-1", "127.0.0.1", 9090, 1);
        registerRegionServer("rs-2", "127.0.0.1", 9091, 1);

        Response create = service.createTable("create table t_stuck_recover_trigger(id int, primary key(id));");
        Assertions.assertEquals(StatusCode.OK, create.getCode());

        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson("/meta/tables/t_stuck_recover_trigger").entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }
        meta.put("tableStatus", "MOVING");
        meta.put("migrationAttemptId", "attempt-stuck-trigger");
        meta.put("version", 1L);
        zkClient.setData().forPath("/meta/tables/t_stuck_recover_trigger",
                MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));

        AtomicLong clock = new AtomicLong(20_000L);
        MasterServiceImpl recoveringService = new MasterServiceImpl(
                new MetaManager(zkClient),
                new AssignmentManager(zkClient),
                new LoadBalancer(),
                ddlExecutor,
                adminExecutor,
                clock::get,
                1_000L,
                10,
                5_000L);

        Response rebalance = recoveringService.triggerRebalance();
        Assertions.assertEquals(StatusCode.OK, rebalance.getCode());
        Assertions.assertTrue(rebalance.getMessage().contains("recovered 1 stuck migration(s)"));

        Map<?, ?> finalMeta = readJson("/meta/tables/t_stuck_recover_trigger");
        Assertions.assertEquals("ACTIVE", String.valueOf(finalMeta.get("tableStatus")));
        Assertions.assertFalse(finalMeta.containsKey("migrationAttemptId"));
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

    private void forcePrimaryInMeta(String tableName, String primaryId, int port) throws Exception {
        String path = "/meta/tables/" + tableName;
        Map<String, Object> meta = new HashMap<>();
        for (Map.Entry<?, ?> entry : readJson(path).entrySet()) {
            meta.put(String.valueOf(entry.getKey()), entry.getValue());
        }

        Map<String, Object> forcedPrimary = new HashMap<>();
        forcedPrimary.put("id", primaryId);
        forcedPrimary.put("host", "127.0.0.1");
        forcedPrimary.put("port", port);
        meta.put("primaryRS", forcedPrimary);
        meta.put("tableStatus", "ACTIVE");

        zkClient.setData().forPath(path, MAPPER.writeValueAsString(meta).getBytes(StandardCharsets.UTF_8));
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
        private String failTransferTargetId;
        private boolean failAnyTransfer;

        void failTransferTo(String targetReplicaId) {
            this.failTransferTargetId = targetReplicaId;
        }

        void failAnyTransfer() {
            this.failAnyTransfer = true;
        }

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
            if (failAnyTransfer || target.getId().equals(failTransferTargetId)) {
                Response fail = new Response(StatusCode.ERROR);
                fail.setMessage("simulated transfer failure to target " + target.getId());
                return fail;
            }
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
