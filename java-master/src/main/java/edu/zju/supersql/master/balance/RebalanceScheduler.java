package edu.zju.supersql.master.balance;

import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
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

    public static final class Snapshot {
        private final boolean enabled;
        private final long intervalMs;
        private final long minGapMs;
        private final boolean started;
        private final long tickCount;
        private final long triggerCount;
        private final long throttledCount;
        private final long successCount;
        private final long failureCount;
        private final long lastAttemptAtMs;
        private final long lastSuccessAtMs;
        private final long lastFailureAtMs;
        private final String lastError;

        Snapshot(boolean enabled,
                 long intervalMs,
                 long minGapMs,
                 boolean started,
                 long tickCount,
                 long triggerCount,
                 long throttledCount,
                 long successCount,
                 long failureCount,
                 long lastAttemptAtMs,
                 long lastSuccessAtMs,
                 long lastFailureAtMs,
                 String lastError) {
            this.enabled = enabled;
            this.intervalMs = intervalMs;
            this.minGapMs = minGapMs;
            this.started = started;
            this.tickCount = tickCount;
            this.triggerCount = triggerCount;
            this.throttledCount = throttledCount;
            this.successCount = successCount;
            this.failureCount = failureCount;
            this.lastAttemptAtMs = lastAttemptAtMs;
            this.lastSuccessAtMs = lastSuccessAtMs;
            this.lastFailureAtMs = lastFailureAtMs;
            this.lastError = lastError;
        }

        public boolean enabled() { return enabled; }
        public long intervalMs() { return intervalMs; }
        public long minGapMs() { return minGapMs; }
        public boolean started() { return started; }
        public long tickCount() { return tickCount; }
        public long triggerCount() { return triggerCount; }
        public long throttledCount() { return throttledCount; }
        public long successCount() { return successCount; }
        public long failureCount() { return failureCount; }
        public long lastAttemptAtMs() { return lastAttemptAtMs; }
        public long lastSuccessAtMs() { return lastSuccessAtMs; }
        public long lastFailureAtMs() { return lastFailureAtMs; }
        public String lastError() { return lastError; }
    }

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
    private final AtomicLong tickCount = new AtomicLong(0L);
    private final AtomicLong triggerCount = new AtomicLong(0L);
    private final AtomicLong throttledCount = new AtomicLong(0L);
    private final AtomicLong successCount = new AtomicLong(0L);
    private final AtomicLong failureCount = new AtomicLong(0L);
    private final AtomicLong lastAttemptAtMs = new AtomicLong(-1L);
    private final AtomicLong lastSuccessAtMs = new AtomicLong(-1L);
    private final AtomicLong lastFailureAtMs = new AtomicLong(-1L);
    private volatile String lastError;
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

    public Snapshot snapshot() {
        return new Snapshot(
                enabled,
                intervalMs,
                minGapMs,
                started.get(),
                tickCount.get(),
                triggerCount.get(),
                throttledCount.get(),
                successCount.get(),
                failureCount.get(),
                lastAttemptAtMs.get(),
                lastSuccessAtMs.get(),
                lastFailureAtMs.get(),
                lastError);
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
        tickCount.incrementAndGet();
        long now = clockMs.getAsLong();
        long last = lastTriggerAtMs.get();
        if (last != Long.MIN_VALUE && now - last < minGapMs) {
            throttledCount.incrementAndGet();
            log.debug("Rebalance scheduler throttled: sinceLast={}ms < minGap={}ms", now - last, minGapMs);
            return;
        }
        if (!lastTriggerAtMs.compareAndSet(last, now)) {
            return;
        }
        triggerCount.incrementAndGet();
        lastAttemptAtMs.set(now);
        try {
            Response response = trigger.trigger();
            if (response == null || response.getCode() == StatusCode.OK) {
                successCount.incrementAndGet();
                lastSuccessAtMs.set(now);
                lastError = null;
            } else {
                failureCount.incrementAndGet();
                lastFailureAtMs.set(now);
                lastError = "trigger returned " + response.getCode();
            }
            if (response != null) {
                log.info("Rebalance scheduler trigger result: code={} msg={}",
                        response.getCode(), response.isSetMessage() ? response.getMessage() : "");
            }
        } catch (Exception e) {
            failureCount.incrementAndGet();
            lastFailureAtMs.set(now);
            lastError = e.getMessage();
            throw e;
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
