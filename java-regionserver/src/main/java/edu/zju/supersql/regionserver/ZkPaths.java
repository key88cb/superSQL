package edu.zju.supersql.regionserver;

/**
 * Shared ZooKeeper path constants for the RegionServer module.
 */
public final class ZkPaths {

    public static final String REGION_SERVERS = "/region_servers";
    public static final String ASSIGNMENTS = "/assignments";
    public static final String ACTIVE_MASTER = "/active-master";
    public static final String META_TABLES = "/meta/tables";

    private ZkPaths() {
    }

    public static String regionServer(String rsId) {
        return REGION_SERVERS + "/" + rsId;
    }

    public static String assignment(String tableName) {
        return ASSIGNMENTS + "/" + tableName;
    }

    public static String tableMeta(String tableName) {
        return META_TABLES + "/" + tableName;
    }
}
