package edu.zju.supersql.master;

import edu.zju.supersql.master.balance.RebalanceScheduler;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class MasterServerSchedulerTriggerTest {

    @Test
    void composeScheduledTriggerShouldRunRouteRepairBeforeRebalance() throws Exception {
        AtomicInteger sequence = new AtomicInteger(0);
        AtomicInteger repairOrder = new AtomicInteger(0);
        AtomicInteger rebalanceOrder = new AtomicInteger(0);

        RebalanceScheduler.RebalanceTrigger trigger = MasterServer.composeScheduledTrigger(
                () -> {
                    repairOrder.set(sequence.incrementAndGet());
                    return 2;
                },
                () -> {
                    rebalanceOrder.set(sequence.incrementAndGet());
                    return ok("rebalance-ok");
                }
        );

        Response response = trigger.trigger();

        Assertions.assertEquals(StatusCode.OK, response.getCode());
        Assertions.assertEquals(1, repairOrder.get());
        Assertions.assertEquals(2, rebalanceOrder.get());
    }

    @Test
    void composeScheduledTriggerShouldKeepRebalanceWhenRouteRepairThrows() throws Exception {
        AtomicInteger rebalanceCalls = new AtomicInteger(0);

        RebalanceScheduler.RebalanceTrigger trigger = MasterServer.composeScheduledTrigger(
                () -> {
                    throw new RuntimeException("repair-boom");
                },
                () -> {
                    rebalanceCalls.incrementAndGet();
                    return ok("rebalance-after-repair-failure");
                }
        );

        Response response = trigger.trigger();

        Assertions.assertEquals(StatusCode.OK, response.getCode());
        Assertions.assertEquals(1, rebalanceCalls.get());
    }

    private static Response ok(String message) {
        Response response = new Response(StatusCode.OK);
        response.setMessage(message);
        return response;
    }
}
