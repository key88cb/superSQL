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
    }
}
