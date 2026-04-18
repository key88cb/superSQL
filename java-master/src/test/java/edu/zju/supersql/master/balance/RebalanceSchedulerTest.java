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
        RebalanceScheduler.Snapshot snapshot = scheduler.snapshot();
        Assertions.assertEquals(1L, snapshot.tickCount());
        Assertions.assertEquals(1L, snapshot.triggerCount());
        Assertions.assertEquals(1L, snapshot.successCount());
        Assertions.assertEquals(0L, snapshot.failureCount());
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
        RebalanceScheduler.Snapshot snapshot = scheduler.snapshot();
        Assertions.assertEquals(3L, snapshot.tickCount());
        Assertions.assertEquals(2L, snapshot.triggerCount());
        Assertions.assertEquals(1L, snapshot.throttledCount());
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
        RebalanceScheduler.Snapshot snapshot = scheduler.snapshot();
        Assertions.assertEquals(0L, snapshot.tickCount());
        Assertions.assertEquals(0L, snapshot.triggerCount());
    }

    @Test
    void requestTriggerShouldInvokeTickAndIncreaseExternalCounter() {
        AtomicInteger calls = new AtomicInteger(0);
        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                0L,
                () -> {
                    calls.incrementAndGet();
                    return ok();
                }
        );

        scheduler.requestTrigger("rs_up:rs-1");

        Assertions.assertEquals(1, calls.get());
        RebalanceScheduler.Snapshot snapshot = scheduler.snapshot();
        Assertions.assertEquals(1L, snapshot.externalRequestCount());
        Assertions.assertEquals(1L, snapshot.tickCount());
        Assertions.assertEquals(1L, snapshot.triggerCount());
    }

    @Test
    void tickShouldRecordFailureWhenTriggerThrows() {
        AtomicLong clock = new AtomicLong(2_000L);
        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                10_000L,
                () -> {
                    throw new IllegalStateException("boom");
                },
                clock::get
        );

        Assertions.assertThrows(IllegalStateException.class, scheduler::tick);

        RebalanceScheduler.Snapshot snapshot = scheduler.snapshot();
        Assertions.assertEquals(1L, snapshot.tickCount());
        Assertions.assertEquals(1L, snapshot.triggerCount());
        Assertions.assertEquals(0L, snapshot.successCount());
        Assertions.assertEquals(1L, snapshot.failureCount());
        Assertions.assertEquals("boom", snapshot.lastError());
    }

    private static Response ok() {
        Response response = new Response(StatusCode.OK);
        response.setMessage("ok");
        return response;
    }
}
