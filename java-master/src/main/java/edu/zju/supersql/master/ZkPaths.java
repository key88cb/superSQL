package edu.zju.supersql.master;

import java.util.List;

/**
 * Shared ZooKeeper path constants for the Master module.
 */
public final class ZkPaths {

    public static final String MASTERS = "/masters";
    public static final String ACTIVE_HEARTBEAT = "/masters/active-heartbeat";
    public static final String REGION_SERVERS = "/region_servers";
    public static final String META_TABLES = "/meta/tables";
    public static final String ASSIGNMENTS = "/assignments";
    public static final String ACTIVE_MASTER = "/active-master";

    private ZkPaths() {
    }

    public static String tableMeta(String tableName) {
        return META_TABLES + "/" + tableName;
    }

    public static String assignment(String tableName) {
        return ASSIGNMENTS + "/" + tableName;
    }

    public static String regionServer(String rsId) {
        return REGION_SERVERS + "/" + rsId;
    }

    public static List<String> bootstrapPaths() {
        return List.of(MASTERS, ACTIVE_HEARTBEAT, REGION_SERVERS, META_TABLES, ASSIGNMENTS, ACTIVE_MASTER);
    }
}
