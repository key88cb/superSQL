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
        Assertions.assertEquals(0L, ((Number) transferManifestVerification.get("lastSuccessTs")).longValue());
        Map<?, ?> transferTable = (Map<?, ?>) json.get("transferTable");
        Assertions.assertEquals(0L, ((Number) transferTable.get("total")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) transferTable.get("lastSuccessTs")).longValue());
        Assertions.assertTrue(json.containsKey("timestamp"));
    }

    @Test
    void statusPayloadShouldIncludeProvidedTransferStats() throws Exception {
        Map<String, Object> manifestStats = new LinkedHashMap<>();
        manifestStats.put("total", 5L);
        manifestStats.put("success", 3L);
        manifestStats.put("failure", 2L);
        manifestStats.put("lastSuccessTs", 111L);
        manifestStats.put("lastFailureTs", 123L);
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
        reasons.put("other", 0L);
        transferTableStats.put("failureReasons", reasons);
        transferTableStats.put("lastFailureTs", 456L);
        transferTableStats.put("lastFailureReason", "target_reject");
        transferTableStats.put("lastFailureMessage", "copyTableData rejected");

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
        Assertions.assertEquals(111L, ((Number) transferManifestVerification.get("lastSuccessTs")).longValue());
        Assertions.assertEquals(123L, ((Number) transferManifestVerification.get("lastFailureTs")).longValue());
        Assertions.assertEquals("checksum mismatch", transferManifestVerification.get("lastFailureMessage"));

            Assertions.assertEquals(7L, ((Number) transferTable.get("total")).longValue());
            Assertions.assertEquals(4L, ((Number) transferTable.get("success")).longValue());
            Assertions.assertEquals(3L, ((Number) transferTable.get("failure")).longValue());
        Assertions.assertEquals(222L, ((Number) transferTable.get("lastSuccessTs")).longValue());
            Map<?, ?> failureReasons = (Map<?, ?>) transferTable.get("failureReasons");
            Assertions.assertEquals(1L, ((Number) failureReasons.get("table_not_found")).longValue());
            Assertions.assertEquals(2L, ((Number) failureReasons.get("target_reject")).longValue());
            Assertions.assertEquals(0L, ((Number) failureReasons.get("transport_error")).longValue());
            Assertions.assertEquals("target_reject", transferTable.get("lastFailureReason"));
    }
}
