package edu.zju.supersql.client;

import java.util.Map;

/**
 * Typed runtime configuration for the client process.
 */
public record ClientConfig(
        String zkConnect,
        String masterFallback,
        long cacheTtlMs,
        int masterRpcTimeoutMs,
    int regionRpcTimeoutMs,
    int movingRetryMaxAttempts,
    int movingRetryInitialBackoffMs,
    int movingRetryBackoffStepMs
) {

    public static ClientConfig fromSystemEnv() {
        return fromEnv(System.getenv());
    }

    static ClientConfig fromEnv(Map<String, String> env) {
        return new ClientConfig(
                readString(env, "ZK_CONNECT", readString(env, "CLIENT_ZK_CONNECT",
                        "zk1:2181,zk2:2181,zk3:2181")),
                readString(env, "MASTER_ADDR", "master-1:8080"),
                readLong(env, "CLIENT_CACHE_TTL_MS", 30_000L),
                readInt(env, "CLIENT_MASTER_RPC_TIMEOUT_MS", 5_000),
            readInt(env, "CLIENT_RS_RPC_TIMEOUT_MS", 10_000),
            readInt(env, "CLIENT_MOVING_RETRY_MAX_ATTEMPTS", 5),
            readInt(env, "CLIENT_MOVING_RETRY_INITIAL_BACKOFF_MS", 300),
            readInt(env, "CLIENT_MOVING_RETRY_BACKOFF_STEP_MS", 200)
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
}
