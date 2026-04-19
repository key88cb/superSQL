package edu.zju.supersql.regionserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

class RegionServerMainStatusPayloadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void statusPayloadShouldContainCoreRuntimeFields() throws Exception {
        byte[] payload = RegionServerMain.buildStatusPayload(
                "rs-9",
                "127.0.0.1",
                9090,
                9190,
                "zk:2181",
                "./data",
                "./wal",
                true);

        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);

        Assertions.assertEquals("ok", json.get("status"));
        Assertions.assertEquals("rs-9", json.get("rsId"));
        Assertions.assertEquals("127.0.0.1", json.get("rsHost"));
        Assertions.assertEquals(9090, ((Number) json.get("thriftPort")).intValue());
        Assertions.assertEquals(9190, ((Number) json.get("httpPort")).intValue());
        Assertions.assertEquals("zk:2181", json.get("zkConnect"));
        Assertions.assertEquals("./data", json.get("dataDir"));
        Assertions.assertEquals("./wal", json.get("walDir"));
        Assertions.assertEquals(Boolean.TRUE, json.get("miniSqlAlive"));
        Map<?, ?> transferManifestVerification = (Map<?, ?>) json.get("transferManifestVerification");
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("total")).longValue());
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("duplicateAcks")).longValue());
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("lastSuccessTs")).longValue());
        Assertions.assertEquals("", transferManifestVerification.get("lastFailureReason"));
        Assertions.assertEquals("", transferManifestVerification.get("lastFailureTable"));
        Map<?, ?> manifestFailureReasons = (Map<?, ?>) transferManifestVerification.get("failureReasons");
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("invalid_manifest")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("scope_violation")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("file_missing")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("size_mismatch")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("checksum_mismatch")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("other")).longValue());
        Assertions.assertTrue(((java.util.List<?>) transferManifestVerification.get("recentFailures")).isEmpty());
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("recentFailuresDropped")).longValue());
        Assertions.assertTrue(((Map<?, ?>) transferManifestVerification.get("duplicateAcksByTable")).isEmpty());
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("duplicateAcksByTableDropped")).longValue());
        Map<?, ?> transferTable = (Map<?, ?>) json.get("transferTable");
        Assertions.assertEquals(0L, ((Number) transferTable.get("total")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("lastSuccessTs")).longValue());
        Assertions.assertEquals("", transferTable.get("lastFailureTable"));
        Assertions.assertTrue(((java.util.List<?>) transferTable.get("recentFailures")).isEmpty());
        Assertions.assertEquals(0L, ((Number) transferTable.get("recentFailuresDropped")).longValue());
        Map<?, ?> prepareDecision = (Map<?, ?>) json.get("prepareDecision");
        Assertions.assertEquals(0L, ((Number) prepareDecision.get("timeoutMs")).longValue());
        Assertions.assertEquals(0L, ((Number) prepareDecision.get("runs")).longValue());
        Assertions.assertEquals(0L, ((Number) prepareDecision.get("examined")).longValue());
        Assertions.assertEquals(0L, ((Number) prepareDecision.get("autoAborted")).longValue());
        Assertions.assertEquals(0L, ((Number) prepareDecision.get("lastRunAtMs")).longValue());
        Assertions.assertEquals(0L, ((Number) prepareDecision.get("lastAbortAtMs")).longValue());
        Assertions.assertEquals("", prepareDecision.get("lastAbortTable"));
        Assertions.assertEquals(-1L, ((Number) prepareDecision.get("lastAbortLsn")).longValue());
        Assertions.assertEquals("", prepareDecision.get("lastError"));
        Map<?, ?> replicaCommitRetry = (Map<?, ?>) json.get("replicaCommitRetry");
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("pendingCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("enqueuedCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("recoveredCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("retryAttemptCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("droppedCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("throttledSkipCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("escalatedCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionCandidateCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("lastDecisionCandidateAtMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionCandidateCooldownAppliedCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionCandidateCooldownMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionReadyTransitionCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("lastDecisionReadyAtMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionReadyCooldownAppliedCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionReadyCooldownMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionReadyAttemptsThreshold")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("maxAgeMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("recoveredFromEscalationCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("lastRecoveredFromEscalationAtMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("repairTriggeredCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("repairSuccessCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("repairFailureCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("stalledCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("oldestPendingAgeMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("activeEscalatedCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("activeDecisionCandidateCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("activeDecisionReadyCount")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("decisionReadyOldestAgeMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("maxConsecutiveTransportFailures")).longValue());
        Assertions.assertTrue(((java.util.List<?>) replicaCommitRetry.get("decisionCandidatesPreview")).isEmpty());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("lastSuccessAtMs")).longValue());
        Assertions.assertEquals(0L, ((Number) replicaCommitRetry.get("lastFailureAtMs")).longValue());
        Assertions.assertEquals("", replicaCommitRetry.get("lastError"));
        Assertions.assertTrue(((Map<?, ?>) replicaCommitRetry.get("errorBreakdown")).isEmpty());
        Assertions.assertTrue(json.containsKey("timestamp"));
    }

    @Test
    void statusPayloadShouldIncludeProvidedTransferStats() throws Exception {
        Map<String, Object> manifestStats = new LinkedHashMap<>();
        manifestStats.put("total", 5L);
        manifestStats.put("success", 3L);
        manifestStats.put("failure", 2L);
        manifestStats.put("duplicateAcks", 1L);
        manifestStats.put("lastSuccessTs", 111L);
        manifestStats.put("lastFailureTs", 123L);
        Map<String, Object> manifestReasons = new LinkedHashMap<>();
        manifestReasons.put("invalid_manifest", 1L);
        manifestReasons.put("scope_violation", 0L);
        manifestReasons.put("file_missing", 1L);
        manifestReasons.put("size_mismatch", 0L);
        manifestReasons.put("checksum_mismatch", 0L);
        manifestReasons.put("other", 0L);
        manifestStats.put("failureReasons", manifestReasons);
        manifestStats.put("lastFailureReason", "file_missing");
        manifestStats.put("lastFailureTable", "orders");
        manifestStats.put("lastFailureMessage", "checksum mismatch");
        java.util.List<Map<String, Object>> manifestRecentFailures = new java.util.ArrayList<>();
        Map<String, Object> manifestEvent = new LinkedHashMap<>();
        manifestEvent.put("ts", 120L);
        manifestEvent.put("table", "orders");
        manifestEvent.put("reason", "file_missing");
        manifestEvent.put("message", "missing orders_data");
        manifestRecentFailures.add(manifestEvent);
        manifestStats.put("recentFailures", manifestRecentFailures);
        manifestStats.put("recentFailuresDropped", 3L);
        Map<String, Object> manifestDuplicateByTable = new LinkedHashMap<>();
        manifestDuplicateByTable.put("orders", 2L);
        manifestStats.put("duplicateAcksByTable", manifestDuplicateByTable);
        manifestStats.put("duplicateAcksByTableDropped", 5L);

        Map<String, Object> transferTableStats = new LinkedHashMap<>();
        transferTableStats.put("total", 7L);
        transferTableStats.put("success", 4L);
        transferTableStats.put("failure", 3L);
        transferTableStats.put("lastSuccessTs", 222L);
        Map<String, Object> reasons = new LinkedHashMap<>();
        reasons.put("table_not_found", 1L);
        reasons.put("target_reject", 2L);
        reasons.put("transport_error", 0L);
        reasons.put("source_io_error", 0L);
        reasons.put("other", 0L);
        transferTableStats.put("failureReasons", reasons);
        transferTableStats.put("lastFailureTs", 456L);
        transferTableStats.put("lastFailureReason", "target_reject");
        transferTableStats.put("lastFailureTable", "orders");
        transferTableStats.put("lastFailureMessage", "copyTableData rejected");
        java.util.List<Map<String, Object>> recentFailures = new java.util.ArrayList<>();
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("ts", 456L);
        event.put("table", "orders");
        event.put("reason", "target_reject");
        event.put("code", "ERROR");
        event.put("message", "copyTableData rejected");
        recentFailures.add(event);
        transferTableStats.put("recentFailures", recentFailures);
        transferTableStats.put("recentFailuresDropped", 4L);

        Map<String, Object> prepareDecision = new LinkedHashMap<>();
        prepareDecision.put("timeoutMs", 300000L);
        prepareDecision.put("runs", 12L);
        prepareDecision.put("examined", 21L);
        prepareDecision.put("autoAborted", 2L);
        prepareDecision.put("lastRunAtMs", 600L);
        prepareDecision.put("lastAbortAtMs", 550L);
        prepareDecision.put("lastAbortTable", "orders");
        prepareDecision.put("lastAbortLsn", 99L);
        prepareDecision.put("lastError", "");

        Map<String, Object> replicaCommitRetry = new LinkedHashMap<>();
        replicaCommitRetry.put("pendingCount", 2L);
        replicaCommitRetry.put("enqueuedCount", 10L);
        replicaCommitRetry.put("recoveredCount", 8L);
        replicaCommitRetry.put("retryAttemptCount", 21L);
        replicaCommitRetry.put("droppedCount", 1L);
        replicaCommitRetry.put("throttledSkipCount", 6L);
        replicaCommitRetry.put("escalatedCount", 2L);
        replicaCommitRetry.put("decisionCandidateCount", 1L);
        replicaCommitRetry.put("lastDecisionCandidateAtMs", 776L);
        replicaCommitRetry.put("decisionCandidateCooldownAppliedCount", 1L);
        replicaCommitRetry.put("decisionCandidateCooldownMs", 300000L);
        replicaCommitRetry.put("decisionReadyTransitionCount", 1L);
        replicaCommitRetry.put("lastDecisionReadyAtMs", 778L);
        replicaCommitRetry.put("decisionReadyCooldownAppliedCount", 2L);
        replicaCommitRetry.put("decisionReadyCooldownMs", 900000L);
        replicaCommitRetry.put("decisionReadyAttemptsThreshold", 24L);
        replicaCommitRetry.put("maxAgeMs", 1800000L);
        replicaCommitRetry.put("recoveredFromEscalationCount", 1L);
        replicaCommitRetry.put("lastRecoveredFromEscalationAtMs", 777L);
        replicaCommitRetry.put("repairTriggeredCount", 4L);
        replicaCommitRetry.put("repairSuccessCount", 3L);
        replicaCommitRetry.put("repairFailureCount", 1L);
        replicaCommitRetry.put("stalledCount", 2L);
        replicaCommitRetry.put("oldestPendingAgeMs", 120000L);
        replicaCommitRetry.put("activeEscalatedCount", 1L);
        replicaCommitRetry.put("activeDecisionCandidateCount", 1L);
        replicaCommitRetry.put("activeDecisionReadyCount", 1L);
        replicaCommitRetry.put("decisionReadyOldestAgeMs", 240000L);
        replicaCommitRetry.put("maxConsecutiveTransportFailures", 12L);
        java.util.List<Map<String, Object>> decisionCandidatesPreview = new java.util.ArrayList<>();
        Map<String, Object> candidate = new LinkedHashMap<>();
        candidate.put("table", "orders");
        candidate.put("lsn", 9191L);
        candidate.put("address", "127.0.0.1:1");
        candidate.put("attempts", 16L);
        candidate.put("consecutiveTransportFailures", 16L);
        candidate.put("ageMs", 120001L);
        candidate.put("nextRetryAtMs", 500000L);
        candidate.put("decisionReady", true);
        decisionCandidatesPreview.add(candidate);
        replicaCommitRetry.put("decisionCandidatesPreview", decisionCandidatesPreview);
        replicaCommitRetry.put("lastSuccessAtMs", 321L);
        replicaCommitRetry.put("lastFailureAtMs", 654L);
        replicaCommitRetry.put("lastError", "commit returned TABLE_NOT_FOUND");
        Map<String, Object> errorBreakdown = new LinkedHashMap<>();
        errorBreakdown.put("table_not_found", 5L);
        errorBreakdown.put("transport_error", 2L);
        replicaCommitRetry.put("errorBreakdown", errorBreakdown);

        byte[] payload = RegionServerMain.buildStatusPayload(
                "rs-9",
                "127.0.0.1",
                9090,
                9190,
                "zk:2181",
                "./data",
                "./wal",
                true,
                manifestStats,
                transferTableStats,
                prepareDecision,
                replicaCommitRetry);

        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);
        Map<?, ?> transferManifestVerification = (Map<?, ?>) json.get("transferManifestVerification");
            Map<?, ?> transferTable = (Map<?, ?>) json.get("transferTable");

        Assertions.assertEquals(5L, ((Number) transferManifestVerification.get("total")).longValue());
        Assertions.assertEquals(3L, ((Number) transferManifestVerification.get("success")).longValue());
        Assertions.assertEquals(2L, ((Number) transferManifestVerification.get("failure")).longValue());
        Assertions.assertEquals(1L, ((Number) transferManifestVerification.get("duplicateAcks")).longValue());
        Assertions.assertEquals(111L, ((Number) transferManifestVerification.get("lastSuccessTs")).longValue());
        Assertions.assertEquals(123L, ((Number) transferManifestVerification.get("lastFailureTs")).longValue());
        Assertions.assertEquals("file_missing", transferManifestVerification.get("lastFailureReason"));
        Assertions.assertEquals("orders", transferManifestVerification.get("lastFailureTable"));
        Assertions.assertEquals("checksum mismatch", transferManifestVerification.get("lastFailureMessage"));
        Map<?, ?> manifestFailureReasons = (Map<?, ?>) transferManifestVerification.get("failureReasons");
        Assertions.assertEquals(1L, ((Number) manifestFailureReasons.get("invalid_manifest")).longValue());
        Assertions.assertEquals(1L, ((Number) manifestFailureReasons.get("file_missing")).longValue());
        Assertions.assertEquals(1, ((java.util.List<?>) transferManifestVerification.get("recentFailures")).size());
        Assertions.assertEquals(3L, ((Number) transferManifestVerification.get("recentFailuresDropped")).longValue());
        Map<?, ?> duplicateAckTables = (Map<?, ?>) transferManifestVerification.get("duplicateAcksByTable");
        Assertions.assertEquals(2L, ((Number) duplicateAckTables.get("orders")).longValue());
        Assertions.assertEquals(5L, ((Number) transferManifestVerification.get("duplicateAcksByTableDropped")).longValue());

            Assertions.assertEquals(7L, ((Number) transferTable.get("total")).longValue());
            Assertions.assertEquals(4L, ((Number) transferTable.get("success")).longValue());
            Assertions.assertEquals(3L, ((Number) transferTable.get("failure")).longValue());
        Assertions.assertEquals(222L, ((Number) transferTable.get("lastSuccessTs")).longValue());
            Map<?, ?> failureReasons = (Map<?, ?>) transferTable.get("failureReasons");
            Assertions.assertEquals(1L, ((Number) failureReasons.get("table_not_found")).longValue());
            Assertions.assertEquals(2L, ((Number) failureReasons.get("target_reject")).longValue());
            Assertions.assertEquals(0L, ((Number) failureReasons.get("transport_error")).longValue());
            Assertions.assertEquals(0L, ((Number) failureReasons.get("source_io_error")).longValue());
            Assertions.assertEquals("target_reject", transferTable.get("lastFailureReason"));
            Assertions.assertEquals("orders", transferTable.get("lastFailureTable"));
            Assertions.assertEquals(1, ((java.util.List<?>) transferTable.get("recentFailures")).size());
            Assertions.assertEquals(4L, ((Number) transferTable.get("recentFailuresDropped")).longValue());
        Map<?, ?> prepareDecisionStats = (Map<?, ?>) json.get("prepareDecision");
        Assertions.assertEquals(300000L, ((Number) prepareDecisionStats.get("timeoutMs")).longValue());
        Assertions.assertEquals(12L, ((Number) prepareDecisionStats.get("runs")).longValue());
        Assertions.assertEquals(21L, ((Number) prepareDecisionStats.get("examined")).longValue());
        Assertions.assertEquals(2L, ((Number) prepareDecisionStats.get("autoAborted")).longValue());
        Assertions.assertEquals(600L, ((Number) prepareDecisionStats.get("lastRunAtMs")).longValue());
        Assertions.assertEquals(550L, ((Number) prepareDecisionStats.get("lastAbortAtMs")).longValue());
        Assertions.assertEquals("orders", prepareDecisionStats.get("lastAbortTable"));
        Assertions.assertEquals(99L, ((Number) prepareDecisionStats.get("lastAbortLsn")).longValue());
        Assertions.assertEquals("", prepareDecisionStats.get("lastError"));
        Map<?, ?> commitRetryStats = (Map<?, ?>) json.get("replicaCommitRetry");
        Assertions.assertEquals(2L, ((Number) commitRetryStats.get("pendingCount")).longValue());
        Assertions.assertEquals(10L, ((Number) commitRetryStats.get("enqueuedCount")).longValue());
        Assertions.assertEquals(8L, ((Number) commitRetryStats.get("recoveredCount")).longValue());
        Assertions.assertEquals(21L, ((Number) commitRetryStats.get("retryAttemptCount")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("droppedCount")).longValue());
        Assertions.assertEquals(6L, ((Number) commitRetryStats.get("throttledSkipCount")).longValue());
        Assertions.assertEquals(2L, ((Number) commitRetryStats.get("escalatedCount")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("decisionCandidateCount")).longValue());
        Assertions.assertEquals(776L, ((Number) commitRetryStats.get("lastDecisionCandidateAtMs")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("decisionCandidateCooldownAppliedCount")).longValue());
        Assertions.assertEquals(300000L, ((Number) commitRetryStats.get("decisionCandidateCooldownMs")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("decisionReadyTransitionCount")).longValue());
        Assertions.assertEquals(778L, ((Number) commitRetryStats.get("lastDecisionReadyAtMs")).longValue());
        Assertions.assertEquals(2L, ((Number) commitRetryStats.get("decisionReadyCooldownAppliedCount")).longValue());
        Assertions.assertEquals(900000L, ((Number) commitRetryStats.get("decisionReadyCooldownMs")).longValue());
        Assertions.assertEquals(24L, ((Number) commitRetryStats.get("decisionReadyAttemptsThreshold")).longValue());
        Assertions.assertEquals(1800000L, ((Number) commitRetryStats.get("maxAgeMs")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("recoveredFromEscalationCount")).longValue());
        Assertions.assertEquals(777L, ((Number) commitRetryStats.get("lastRecoveredFromEscalationAtMs")).longValue());
        Assertions.assertEquals(4L, ((Number) commitRetryStats.get("repairTriggeredCount")).longValue());
        Assertions.assertEquals(3L, ((Number) commitRetryStats.get("repairSuccessCount")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("repairFailureCount")).longValue());
        Assertions.assertEquals(2L, ((Number) commitRetryStats.get("stalledCount")).longValue());
        Assertions.assertEquals(120000L, ((Number) commitRetryStats.get("oldestPendingAgeMs")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("activeEscalatedCount")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("activeDecisionCandidateCount")).longValue());
        Assertions.assertEquals(1L, ((Number) commitRetryStats.get("activeDecisionReadyCount")).longValue());
        Assertions.assertEquals(240000L, ((Number) commitRetryStats.get("decisionReadyOldestAgeMs")).longValue());
        Assertions.assertEquals(12L, ((Number) commitRetryStats.get("maxConsecutiveTransportFailures")).longValue());
        java.util.List<?> preview = (java.util.List<?>) commitRetryStats.get("decisionCandidatesPreview");
        Assertions.assertEquals(1, preview.size());
        Map<?, ?> firstCandidate = (Map<?, ?>) preview.get(0);
        Assertions.assertEquals("orders", firstCandidate.get("table"));
        Assertions.assertEquals(9191L, ((Number) firstCandidate.get("lsn")).longValue());
        Assertions.assertEquals(500000L, ((Number) firstCandidate.get("nextRetryAtMs")).longValue());
        Assertions.assertEquals(Boolean.TRUE, firstCandidate.get("decisionReady"));
        Assertions.assertEquals(321L, ((Number) commitRetryStats.get("lastSuccessAtMs")).longValue());
        Assertions.assertEquals(654L, ((Number) commitRetryStats.get("lastFailureAtMs")).longValue());
        Assertions.assertEquals("commit returned TABLE_NOT_FOUND", commitRetryStats.get("lastError"));
        Map<?, ?> breakdown = (Map<?, ?>) commitRetryStats.get("errorBreakdown");
        Assertions.assertEquals(5L, ((Number) breakdown.get("table_not_found")).longValue());
        Assertions.assertEquals(2L, ((Number) breakdown.get("transport_error")).longValue());
    }
}
