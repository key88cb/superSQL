package edu.zju.supersql.client;

/**
 * Shared ZooKeeper path constants for the client module.
 */
public final class ZkPaths {

    public static final String ACTIVE_MASTER = "/active-master";
    public static final String META_TABLES = "/meta/tables";

    private ZkPaths() {
    }

    public static String tableMeta(String tableName) {
        return META_TABLES + "/" + tableName;
    }
}
