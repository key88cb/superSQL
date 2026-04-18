package edu.zju.supersql.master.balance;

import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class RebalanceSchedulerTest {

    @Test
    void tickShouldTriggerWhenEnabled() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        AtomicInteger calls = new AtomicInteger(0);
        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                10_000L,
                () -> {
                    calls.incrementAndGet();
                    return ok();
                },
                clock::get
        );

        scheduler.tick();

        Assertions.assertEquals(1, calls.get());
    }

    @Test
    void tickShouldBeThrottledByMinGap() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        AtomicInteger calls = new AtomicInteger(0);
        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                10_000L,
                () -> {
                    calls.incrementAndGet();
                    return ok();
                },
                clock::get
        );

        scheduler.tick();
        clock.set(5_000L);
        scheduler.tick();
        clock.set(11_500L);
        scheduler.tick();

        Assertions.assertEquals(2, calls.get());
    }

    @Test
    void tickShouldNotTriggerWhenDisabled() throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        AtomicInteger calls = new AtomicInteger(0);
        RebalanceScheduler scheduler = new RebalanceScheduler(
                false,
                30_000L,
                10_000L,
                () -> {
                    calls.incrementAndGet();
                    return ok();
                },
                clock::get
        );

        scheduler.tick();

        Assertions.assertEquals(0, calls.get());
        Assertions.assertFalse(scheduler.isEnabled());
    }

    private static Response ok() {
        Response response = new Response(StatusCode.OK);
        response.setMessage("ok");
        return response;
    }
}
