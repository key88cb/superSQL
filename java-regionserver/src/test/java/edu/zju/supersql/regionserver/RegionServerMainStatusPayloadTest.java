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
        Map<?, ?> manifestFailureReasons = (Map<?, ?>) transferManifestVerification.get("failureReasons");
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("invalid_manifest")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("scope_violation")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("file_missing")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("size_mismatch")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("checksum_mismatch")).longValue());
        Assertions.assertEquals(0L, ((Number) manifestFailureReasons.get("other")).longValue());
        Map<?, ?> transferTable = (Map<?, ?>) json.get("transferTable");
        Assertions.assertEquals(0L, ((Number) transferTable.get("total")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("lastSuccessTs")).longValue());
        Assertions.assertTrue(((java.util.List<?>) transferTable.get("recentFailures")).isEmpty());
        Assertions.assertEquals(0L, ((Number) transferTable.get("recentFailuresDropped")).longValue());
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
        manifestStats.put("lastFailureMessage", "checksum mismatch");

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
        transferTableStats.put("lastFailureMessage", "copyTableData rejected");
        java.util.List<Map<String, Object>> recentFailures = new java.util.ArrayList<>();
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("ts", 456L);
        event.put("reason", "target_reject");
        event.put("code", "ERROR");
        event.put("message", "copyTableData rejected");
        recentFailures.add(event);
        transferTableStats.put("recentFailures", recentFailures);
        transferTableStats.put("recentFailuresDropped", 4L);

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
                transferTableStats);

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
        Assertions.assertEquals("checksum mismatch", transferManifestVerification.get("lastFailureMessage"));
        Map<?, ?> manifestFailureReasons = (Map<?, ?>) transferManifestVerification.get("failureReasons");
        Assertions.assertEquals(1L, ((Number) manifestFailureReasons.get("invalid_manifest")).longValue());
        Assertions.assertEquals(1L, ((Number) manifestFailureReasons.get("file_missing")).longValue());

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
            Assertions.assertEquals(1, ((java.util.List<?>) transferTable.get("recentFailures")).size());
            Assertions.assertEquals(4L, ((Number) transferTable.get("recentFailuresDropped")).longValue());
    }
}
