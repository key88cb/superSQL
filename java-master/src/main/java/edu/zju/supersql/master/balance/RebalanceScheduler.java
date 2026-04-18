package edu.zju.supersql.master.balance;

import edu.zju.supersql.rpc.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Periodically triggers rebalance with a minimum gap throttle.
 */
public final class RebalanceScheduler implements AutoCloseable {

    @FunctionalInterface
    public interface RebalanceTrigger {
        Response trigger() throws Exception;
    }

    private static final Logger log = LoggerFactory.getLogger(RebalanceScheduler.class);

    private final boolean enabled;
    private final long intervalMs;
    private final long minGapMs;
    private final RebalanceTrigger trigger;
    private final LongSupplier clockMs;
    private final AtomicLong lastTriggerAtMs = new AtomicLong(Long.MIN_VALUE);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;

    public RebalanceScheduler(boolean enabled,
                              long intervalMs,
                              long minGapMs,
                              RebalanceTrigger trigger) {
        this(enabled, intervalMs, minGapMs, trigger, System::currentTimeMillis);
    }

    RebalanceScheduler(boolean enabled,
                       long intervalMs,
                       long minGapMs,
                       RebalanceTrigger trigger,
                       LongSupplier clockMs) {
        this.enabled = enabled;
        this.intervalMs = Math.max(1_000L, intervalMs);
        this.minGapMs = Math.max(0L, minGapMs);
        this.trigger = trigger;
        this.clockMs = clockMs;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void start() {
        if (!enabled) {
            log.info("Rebalance scheduler disabled");
            return;
        }
        if (!started.compareAndSet(false, true)) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Master-Rebalance-Scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::safeTick, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        log.info("Rebalance scheduler started: intervalMs={}, minGapMs={}", intervalMs, minGapMs);
    }

    void tick() throws Exception {
        if (!enabled) {
            return;
        }
        long now = clockMs.getAsLong();
        long last = lastTriggerAtMs.get();
        if (last != Long.MIN_VALUE && now - last < minGapMs) {
            log.debug("Rebalance scheduler throttled: sinceLast={}ms < minGap={}ms", now - last, minGapMs);
            return;
        }
        if (!lastTriggerAtMs.compareAndSet(last, now)) {
            return;
        }
        Response response = trigger.trigger();
        if (response != null) {
            log.info("Rebalance scheduler trigger result: code={} msg={}",
                    response.getCode(), response.isSetMessage() ? response.getMessage() : "");
        }
    }

    private void safeTick() {
        try {
            tick();
        } catch (Exception e) {
            log.warn("Rebalance scheduler tick failed: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
