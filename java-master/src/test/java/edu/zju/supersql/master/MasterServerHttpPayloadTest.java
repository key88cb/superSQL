package edu.zju.supersql.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

class MasterServerHttpPayloadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void healthPayloadShouldContainStatusAndRole() throws Exception {
        byte[] payload = MasterServer.buildHealthPayload();
        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);

        Assertions.assertEquals("ok", json.get("status"));
        Assertions.assertTrue("ACTIVE".equals(json.get("role")) || "STANDBY".equals(json.get("role")));
    }

    @Test
    void statusPayloadShouldContainCoreFields() throws Exception {
        byte[] payload = MasterServer.buildStatusPayload(8080, 8880, "zk1:2181");
        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);

        Assertions.assertEquals("ok", json.get("status"));
        Assertions.assertEquals(8080, ((Number) json.get("thriftPort")).intValue());
        Assertions.assertEquals(8880, ((Number) json.get("httpPort")).intValue());
        Assertions.assertEquals("zk1:2181", json.get("zkConnect"));
        Assertions.assertTrue(json.containsKey("timestamp"));
        Assertions.assertTrue(json.containsKey("zkReady"));
        Assertions.assertTrue(json.containsKey("rebalanceScheduler"));
        Assertions.assertTrue(json.containsKey("routeRepair"));
        Assertions.assertTrue(json.containsKey("migration"));
        Assertions.assertTrue(json.containsKey("replicaDecision"));
        Map<?, ?> scheduler = (Map<?, ?>) json.get("rebalanceScheduler");
        Assertions.assertEquals(Boolean.FALSE, scheduler.get("available"));
        Map<?, ?> routeRepair = (Map<?, ?>) json.get("routeRepair");
        Assertions.assertEquals(Boolean.FALSE, routeRepair.get("available"));
        Map<?, ?> migration = (Map<?, ?>) json.get("migration");
        Assertions.assertEquals(Boolean.FALSE, migration.get("available"));
        Map<?, ?> replicaDecision = (Map<?, ?>) json.get("replicaDecision");
        Assertions.assertEquals(Boolean.FALSE, replicaDecision.get("available"));
    }

    @Test
    void statusPayloadShouldContainSchedulerLastTriggerReasonWhenAvailable() throws Exception {
        edu.zju.supersql.master.balance.RebalanceScheduler scheduler =
                new edu.zju.supersql.master.balance.RebalanceScheduler(
                        true,
                        30_000L,
                        0L,
                () -> new edu.zju.supersql.rpc.Response(edu.zju.supersql.rpc.StatusCode.OK)
                );

        scheduler.requestTrigger("rs_down:rs-2");

        byte[] payload = MasterServer.buildStatusPayload(8080, 8880, "zk1:2181", scheduler);
        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);
        Map<?, ?> schedulerJson = (Map<?, ?>) json.get("rebalanceScheduler");
        Assertions.assertEquals(Boolean.TRUE, schedulerJson.get("available"));
        Assertions.assertEquals("rs_down:rs-2", schedulerJson.get("lastTriggerReason"));
    }

    @Test
    void statusPayloadShouldContainRouteRepairSnapshotWhenAvailable() throws Exception {
        edu.zju.supersql.master.rpc.MasterServiceImpl.RouteRepairSnapshot snapshot =
                new edu.zju.supersql.master.rpc.MasterServiceImpl.RouteRepairSnapshot(
                        3L,
                        7L,
                        1_234L,
                        2L,
                12L,
                5L,
                "rs-2",
                "orders",
                "none",
                10L,
                4L,
                0.75,
                1.5);

        byte[] payload = MasterServer.buildStatusPayload(8080, 8880, "zk1:2181", null, snapshot);
        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);
        Map<?, ?> routeRepair = (Map<?, ?>) json.get("routeRepair");

        Assertions.assertEquals(Boolean.TRUE, routeRepair.get("available"));
        Assertions.assertEquals(3, ((Number) routeRepair.get("runCount")).intValue());
        Assertions.assertEquals(7, ((Number) routeRepair.get("totalRepairedTables")).intValue());
        Assertions.assertEquals(2, ((Number) routeRepair.get("lastRunRepairedCount")).intValue());
        Assertions.assertEquals(12, ((Number) routeRepair.get("lastRunTotalTables")).intValue());
        Assertions.assertEquals(5, ((Number) routeRepair.get("lastRunCandidateTables")).intValue());
        Assertions.assertEquals("rs-2", routeRepair.get("lastRunFilterRegionServerId"));
        Assertions.assertEquals("orders", routeRepair.get("lastRepairedTable"));
        Assertions.assertEquals("none", routeRepair.get("lastError"));
        Assertions.assertEquals(10, ((Number) routeRepair.get("recentWindowSize")).intValue());
        Assertions.assertEquals(4, ((Number) routeRepair.get("recentObservedRuns")).intValue());
        Assertions.assertEquals(0.75, ((Number) routeRepair.get("recentSuccessRate")).doubleValue());
        Assertions.assertEquals(1.5, ((Number) routeRepair.get("recentAvgRepairedCount")).doubleValue());
    }

    @Test
    void statusPayloadShouldContainMigrationSnapshotWhenAvailable() throws Exception {
        edu.zju.supersql.master.migration.RegionMigrator.MigrationSnapshot snapshot =
                new edu.zju.supersql.master.migration.RegionMigrator.MigrationSnapshot(
                        5L,
                        3L,
                        2L,
                4L,
                2L,
                2L,
                1L,
                1L,
                0L,
                        2_001L,
                        2_002L,
                        2_003L,
                "transfer failed",
                "rebalance transfer failed",
                null);

        byte[] payload = MasterServer.buildStatusPayload(8080, 8880, "zk1:2181", null, null, snapshot);
        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);
        Map<?, ?> migration = (Map<?, ?>) json.get("migration");

        Assertions.assertEquals(Boolean.TRUE, migration.get("available"));
        Assertions.assertEquals(5, ((Number) migration.get("attemptCount")).intValue());
        Assertions.assertEquals(3, ((Number) migration.get("successCount")).intValue());
        Assertions.assertEquals(2, ((Number) migration.get("failureCount")).intValue());
        Assertions.assertEquals(4, ((Number) migration.get("rebalanceAttemptCount")).intValue());
        Assertions.assertEquals(2, ((Number) migration.get("rebalanceSuccessCount")).intValue());
        Assertions.assertEquals(2, ((Number) migration.get("rebalanceFailureCount")).intValue());
        Assertions.assertEquals(1, ((Number) migration.get("recoveryAttemptCount")).intValue());
        Assertions.assertEquals(1, ((Number) migration.get("recoverySuccessCount")).intValue());
        Assertions.assertEquals(0, ((Number) migration.get("recoveryFailureCount")).intValue());
        Assertions.assertEquals(2001L, ((Number) migration.get("lastAttemptAtMs")).longValue());
        Assertions.assertEquals(2002L, ((Number) migration.get("lastSuccessAtMs")).longValue());
        Assertions.assertEquals(2003L, ((Number) migration.get("lastFailureAtMs")).longValue());
        Assertions.assertEquals("transfer failed", migration.get("lastError"));
        Assertions.assertEquals("rebalance transfer failed", migration.get("lastRebalanceError"));
        Assertions.assertNull(migration.get("lastRecoveryError"));
    }

        @Test
        void statusPayloadShouldContainReplicaDecisionSnapshotWhenAvailable() throws Exception {
                edu.zju.supersql.master.rpc.MasterServiceImpl.ReplicaDecisionSnapshot snapshot =
                                new edu.zju.supersql.master.rpc.MasterServiceImpl.ReplicaDecisionSnapshot(
                                                5L,
                                                java.util.List.of("rs-2", "rs-4"),
                                                null);

                byte[] payload = MasterServer.buildStatusPayload(8080, 8880, "zk1:2181", null, null, null, snapshot);
                Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);
                Map<?, ?> replicaDecision = (Map<?, ?>) json.get("replicaDecision");

                Assertions.assertEquals(Boolean.TRUE, replicaDecision.get("available"));
                Assertions.assertEquals(5L, ((Number) replicaDecision.get("observedRegionServers")).longValue());
                Assertions.assertEquals(java.util.List.of("rs-2", "rs-4"), replicaDecision.get("affectedRegionServers"));
                Assertions.assertNull(replicaDecision.get("lastError"));
        }
}
