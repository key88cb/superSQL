package edu.zju.supersql.regionserver;

import java.util.Map;

/**
 * Typed runtime configuration for the RegionServer process.
 */
public record RegionServerConfig(
        String rsId,
        String rsHost,
        int thriftPort,
        int httpPort,
        String zkConnect,
        String dataDir,
        String walDir,
        String miniSqlBin,
        long heartbeatIntervalMs,
        int minReplicaAcks
) {

    public static RegionServerConfig fromSystemEnv() {
        return fromEnv(System.getenv());
    }

    static RegionServerConfig fromEnv(Map<String, String> env) {
        return new RegionServerConfig(
                readString(env, "RS_ID", "rs-1"),
                readString(env, "RS_HOST", "rs-1"),
                readInt(env, "RS_THRIFT_PORT", 9090),
                readInt(env, "RS_HTTP_PORT", 9190),
                readString(env, "ZK_CONNECT", readString(env, "RS_ZK_CONNECT",
                        "zk1:2181,zk2:2181,zk3:2181")),
                readString(env, "RS_DATA_DIR", "/data/db"),
                readString(env, "RS_WAL_DIR", "/data/wal"),
                readString(env, "MINISQL_BIN", "/opt/minisql/main"),
                readLong(env, "RS_HEARTBEAT_INTERVAL_MS", 10_000L),
                Math.max(1, readInt(env, "RS_MIN_REPLICA_ACKS", 1))
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
