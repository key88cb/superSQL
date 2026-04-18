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
        Map<?, ?> scheduler = (Map<?, ?>) json.get("rebalanceScheduler");
        Assertions.assertEquals(Boolean.FALSE, scheduler.get("available"));
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
}
