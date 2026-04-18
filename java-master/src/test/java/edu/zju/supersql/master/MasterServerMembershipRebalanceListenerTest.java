package edu.zju.supersql.master;

import edu.zju.supersql.master.balance.RebalanceScheduler;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class MasterServerMembershipRebalanceListenerTest {

    @Test
    void membershipListenerShouldRequestRebalanceOnUpAndDown() {
        AtomicInteger calls = new AtomicInteger(0);
        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                0L,
                () -> {
                    calls.incrementAndGet();
                    Response response = new Response(StatusCode.OK);
                    response.setMessage("ok");
                    return response;
                }
        );

        RegionServerWatcher.Listener listener = MasterServer.buildMembershipRebalanceListener(scheduler);
        listener.onRegionServerUp("rs-1");
        listener.onRegionServerDown("rs-1");

        Assertions.assertEquals(2, calls.get());
        RebalanceScheduler.Snapshot snapshot = scheduler.snapshot();
        Assertions.assertEquals(2L, snapshot.externalRequestCount());
    }

    @Test
    void membershipListenerShouldTriggerRouteRepairOnUpAndDown() {
        AtomicInteger schedulerCalls = new AtomicInteger(0);
        AtomicInteger repairCalls = new AtomicInteger(0);

        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                0L,
                () -> {
                    schedulerCalls.incrementAndGet();
                    Response response = new Response(StatusCode.OK);
                    response.setMessage("ok");
                    return response;
                }
        );

        RegionServerWatcher.Listener listener = MasterServer.buildMembershipRebalanceListener(
                scheduler,
                () -> {
                    repairCalls.incrementAndGet();
                    return 1;
                });

        listener.onRegionServerUp("rs-1");
        listener.onRegionServerDown("rs-1");

        Assertions.assertEquals(2, schedulerCalls.get());
        Assertions.assertEquals(2, repairCalls.get());
    }

    @Test
    void membershipListenerShouldKeepRebalanceWhenRouteRepairThrows() {
        AtomicInteger schedulerCalls = new AtomicInteger(0);
        AtomicInteger repairCalls = new AtomicInteger(0);

        RebalanceScheduler scheduler = new RebalanceScheduler(
                true,
                30_000L,
                0L,
                () -> {
                    schedulerCalls.incrementAndGet();
                    Response response = new Response(StatusCode.OK);
                    response.setMessage("ok");
                    return response;
                }
        );

        RegionServerWatcher.Listener listener = MasterServer.buildMembershipRebalanceListener(
                scheduler,
                () -> {
                    repairCalls.incrementAndGet();
                    throw new RuntimeException("repair failed");
                });

        listener.onRegionServerUp("rs-1");
        listener.onRegionServerDown("rs-2");

        Assertions.assertEquals(2, schedulerCalls.get());
        Assertions.assertEquals(2, repairCalls.get());
        Assertions.assertEquals(2L, scheduler.snapshot().externalRequestCount());
    }
}
