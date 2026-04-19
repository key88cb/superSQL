package edu.zju.supersql.regionserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
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
        Assertions.assertTrue(json.containsKey("timestamp"));
    }
}
