package edu.zju.supersql.master;

import java.util.Map;

/**
 * Typed runtime configuration for the Master process.
 */
public record MasterConfig(
        String masterId,
        int thriftPort,
        int httpPort,
        String zkConnect,
        long heartbeatIntervalMs,
        long heartbeatTimeoutMs,
        long rebalanceIntervalMs,
    double rebalanceRatio,
    boolean rebalanceSchedulerEnabled,
        long rebalanceMinGapMs,
        long migrationStuckTimeoutMs
) {

    public static MasterConfig fromSystemEnv() {
        return fromEnv(System.getenv());
    }

    static MasterConfig fromEnv(Map<String, String> env) {
        return new MasterConfig(
                readString(env, "MASTER_ID", "master-1"),
                readInt(env, "MASTER_THRIFT_PORT", 8080),
                readInt(env, "MASTER_HTTP_PORT", 8880),
                readString(env, "ZK_CONNECT", readString(env, "MASTER_ZK_CONNECT",
                        "zk1:2181,zk2:2181,zk3:2181")),
                readLong(env, "MASTER_HEARTBEAT_INTERVAL_MS", 5_000L),
                readLong(env, "MASTER_HEARTBEAT_TIMEOUT_MS", 15_000L),
                readLong(env, "MASTER_REBALANCE_INTERVAL_MS", 30_000L),
                readDouble(env, "MASTER_REBALANCE_RATIO", 1.5),
                readBoolean(env, "MASTER_REBALANCE_SCHEDULER_ENABLED", true),
                readLong(env, "MASTER_REBALANCE_MIN_GAP_MS", 20_000L),
                readLong(env, "MASTER_MIGRATION_STUCK_TIMEOUT_MS", 60_000L)
        );
    }

    private static String readString(Map<String, String> env, String key, String fallback) {
        String value = env.get(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int readInt(Map<String, String> env, String key, int fallback) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long readLong(Map<String, String> env, String key, long fallback) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static double readDouble(Map<String, String> env, String key, double fallback) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static boolean readBoolean(Map<String, String> env, String key, boolean fallback) {
        String value = env.get(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Boolean.parseBoolean(value);
    }
}
