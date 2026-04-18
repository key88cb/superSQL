package edu.zju.supersql.client;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory per-table routing metrics for client self-observability.
 */
final class ClientRoutingMetrics {

    static final class MetricsSnapshot {
        private final long redirectCount;
        private final long movingRetryCount;
        private final long retryOnExceptionCount;
        private final long locationFetchCount;
        private final long readFallbackCount;

        MetricsSnapshot(long redirectCount,
                        long movingRetryCount,
                        long retryOnExceptionCount,
                        long locationFetchCount,
                        long readFallbackCount) {
            this.redirectCount = redirectCount;
            this.movingRetryCount = movingRetryCount;
            this.retryOnExceptionCount = retryOnExceptionCount;
            this.locationFetchCount = locationFetchCount;
            this.readFallbackCount = readFallbackCount;
        }

        long redirectCount() {
            return redirectCount;
        }

        long movingRetryCount() {
            return movingRetryCount;
        }

        long retryOnExceptionCount() {
            return retryOnExceptionCount;
        }

        long locationFetchCount() {
            return locationFetchCount;
        }

        long readFallbackCount() {
            return readFallbackCount;
        }
    }

    private static final class MutableMetrics {
        private final AtomicLong redirectCount = new AtomicLong(0);
        private final AtomicLong movingRetryCount = new AtomicLong(0);
        private final AtomicLong retryOnExceptionCount = new AtomicLong(0);
        private final AtomicLong locationFetchCount = new AtomicLong(0);
        private final AtomicLong readFallbackCount = new AtomicLong(0);

        MetricsSnapshot snapshot() {
            return new MetricsSnapshot(
                    redirectCount.get(),
                    movingRetryCount.get(),
                    retryOnExceptionCount.get(),
                    locationFetchCount.get(),
                    readFallbackCount.get());
        }
    }

    private final ConcurrentHashMap<String, MutableMetrics> byTable = new ConcurrentHashMap<>();

    void recordRedirect(String tableName) {
        table(tableName).redirectCount.incrementAndGet();
    }

    void recordMovingRetry(String tableName) {
        table(tableName).movingRetryCount.incrementAndGet();
    }

    void recordRetryOnException(String tableName) {
        table(tableName).retryOnExceptionCount.incrementAndGet();
    }

    void recordLocationFetch(String tableName) {
        table(tableName).locationFetchCount.incrementAndGet();
    }

    void recordReadFallback(String tableName) {
        table(tableName).readFallbackCount.incrementAndGet();
    }

    Map<String, MetricsSnapshot> snapshot() {
        Map<String, MetricsSnapshot> copy = new LinkedHashMap<>();
        for (Map.Entry<String, MutableMetrics> entry : byTable.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().snapshot());
        }
        return copy;
    }

    void reset() {
        byTable.clear();
    }

    private MutableMetrics table(String tableName) {
        return byTable.computeIfAbsent(tableName, key -> new MutableMetrics());
    }
}
