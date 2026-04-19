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
        Assertions.assertTrue(json.containsKey("timestamp"));
    }

    @Test
    void statusPayloadShouldIncludeProvidedManifestStats() throws Exception {
        Map<String, Object> transferStats = new LinkedHashMap<>();
        transferStats.put("total", 5L);
        transferStats.put("success", 3L);
        transferStats.put("failure", 2L);
        transferStats.put("lastFailureTs", 123L);
        transferStats.put("lastFailureMessage", "checksum mismatch");

        byte[] payload = RegionServerMain.buildStatusPayload(
                "rs-9",
                "127.0.0.1",
                9090,
                9190,
                "zk:2181",
                "./data",
                "./wal",
                true,
                transferStats);

        Map<?, ?> json = MAPPER.readValue(new String(payload, StandardCharsets.UTF_8), Map.class);
        Map<?, ?> transferManifestVerification = (Map<?, ?>) json.get("transferManifestVerification");

        Assertions.assertEquals(5L, ((Number) transferManifestVerification.get("total")).longValue());
        Assertions.assertEquals(3L, ((Number) transferManifestVerification.get("success")).longValue());
        Assertions.assertEquals(2L, ((Number) transferManifestVerification.get("failure")).longValue());
        Assertions.assertEquals(123L, ((Number) transferManifestVerification.get("lastFailureTs")).longValue());
        Assertions.assertEquals("checksum mismatch", transferManifestVerification.get("lastFailureMessage"));
    }
}
