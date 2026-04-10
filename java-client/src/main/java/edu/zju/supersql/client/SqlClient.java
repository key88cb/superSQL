package edu.zju.supersql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.rpc.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SuperSQL interactive REPL client.
 *
 * <p>Routing rules:
 * <ul>
 *   <li>DDL (CREATE TABLE / DROP TABLE) → MasterService</li>
 *   <li>DML (SELECT / INSERT / UPDATE / DELETE) → RegionService via RouteCache</li>
 *   <li>SHOW TABLES → MasterService.listTables()</li>
 * </ul>
 */
public class SqlClient {

    private static final Logger log = LoggerFactory.getLogger(SqlClient.class);
    private static final Pattern TABLE_NAME_PATTERN =
            Pattern.compile("(?i)\\b(from|into|table|update)\\s+([a-zA-Z_][a-zA-Z0-9_]*)");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    enum SqlKind { DDL, DML, SHOW_TABLES, UNKNOWN }

    static SqlKind classifySql(String sql) {
        if (sql == null || sql.isBlank()) return SqlKind.UNKNOWN;
        String s = sql.trim().toLowerCase();
        if (s.startsWith("show tables")) return SqlKind.SHOW_TABLES;
        if (s.startsWith("create") || s.startsWith("drop") || s.startsWith("alter")
                || s.startsWith("truncate")) return SqlKind.DDL;
        if (s.startsWith("select") || s.startsWith("insert")
                || s.startsWith("update") || s.startsWith("delete")) return SqlKind.DML;
        return SqlKind.UNKNOWN;
    }

    static String extractTableName(String sql) {
        if (sql == null) return null;
        Matcher m = TABLE_NAME_PATTERN.matcher(sql.trim());
        return m.find() ? m.group(2) : null;
    }

    static String readActiveMaster(CuratorFramework zkClient, String fallback) {
        if (zkClient == null) return fallback;
        try {
            if (zkClient.checkExists().forPath(ZkPaths.ACTIVE_MASTER) == null) return fallback;
            byte[] bytes = zkClient.getData().forPath(ZkPaths.ACTIVE_MASTER);
            if (bytes == null || bytes.length == 0) return fallback;
            Map<?, ?> node = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
            Object address = node.get("address");
            if (address != null && !String.valueOf(address).isBlank()) return String.valueOf(address);
            Object id = node.get("masterId");
            return id == null ? fallback : String.valueOf(id);
        } catch (Exception e) {
            log.warn("Failed to read /active-master: {}", e.getMessage());
            return fallback;
        }
    }

    public static void main(String[] args) throws IOException {
        ClientConfig config = ClientConfig.fromSystemEnv();
        String zkConnect = config.zkConnect();
        String masterFallback = config.masterFallback();
        log.info("SuperSQL Client starting, ZK={}", zkConnect);

        CuratorFramework zkClient = null;
        RouteCache routeCache = new RouteCache(config.cacheTtlMs());
        String activeMaster = masterFallback;

        try {
            RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkConnect)
                    .sessionTimeoutMs(30_000)
                    .connectionTimeoutMs(15_000)
                    .retryPolicy(retry)
                    .namespace("supersql")
                    .build();
            zkClient.start();
            boolean connected = zkClient.blockUntilConnected(20, java.util.concurrent.TimeUnit.SECONDS);
            if (connected) {
                log.info("ZooKeeper connected");
                activeMaster = readActiveMaster(zkClient, masterFallback);
                log.info("Discovered active master: {}", activeMaster);
            } else {
                log.warn("ZooKeeper connection timed out — using fallback master: {}", masterFallback);
            }
        } catch (Exception e) {
            log.warn("ZooKeeper init failed: {} — running in offline mode", e.getMessage());
        }

        boolean interactive = System.console() != null || System.in.available() > 0;
        if (!interactive) {
            log.info("No interactive terminal detected — running in daemon/standby mode");
            try { Thread.currentThread().join(); } catch (InterruptedException ignored) {}
            return;
        }

        System.out.println("SuperSQL Client connected. Type 'exit' to quit.");
        System.out.print("SuperSQL> ");
        System.out.flush();

        final String finalActiveMaster = activeMaster;
        final CuratorFramework finalZkClient = zkClient;

        try (Scanner sc = new Scanner(System.in)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim();
                if (line.isEmpty()) { printPrompt(); continue; }
                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    System.out.println("Bye.");
                    break;
                }

                try {
                    handleSql(line, finalActiveMaster, routeCache, config);
                } catch (Exception e) {
                    System.out.println("Error: " + e.getMessage());
                    log.debug("SQL execution error", e);
                }

                printPrompt();
            }
        }

        if (zkClient != null) zkClient.close();
    }

    // ─────────────────────── routing ──────────────────────────────────────────

    static void handleSql(String sql, String activeMaster, RouteCache routeCache, ClientConfig config) throws Exception {
        SqlKind kind = classifySql(sql);

        switch (kind) {
            case SHOW_TABLES -> handleShowTables(activeMaster, config);
            case DDL         -> handleDdl(sql, activeMaster, routeCache, config);
            case DML         -> handleDml(sql, activeMaster, routeCache, config);
            default          -> System.out.println("Unknown SQL: " + sql);
        }
    }

    private static void handleShowTables(String activeMaster, ClientConfig config) throws Exception {
        try (MasterRpcClient master = MasterRpcClient.fromAddress(activeMaster, config.masterRpcTimeoutMs())) {
            List<TableLocation> tables = master.listTables();
            if (tables == null || tables.isEmpty()) {
                System.out.println("(no tables)");
            } else {
                System.out.println("Tables:");
                for (TableLocation t : tables) {
                    System.out.printf("  %-30s  primary=%s:%d%n",
                            t.getTableName(),
                            t.getPrimaryRS().getHost(),
                            t.getPrimaryRS().getPort());
                }
            }
        }
    }

    private static void handleDdl(String sql, String activeMaster, RouteCache routeCache, ClientConfig config) throws Exception {
        String normalized = sql.trim().toLowerCase();
        Response r;
        if (normalized.startsWith("create table")) {
            r = invokeMasterResponseWithRedirect(activeMaster, config, master -> master.createTable(sql));
        } else if (normalized.startsWith("drop table")) {
            String tableName = extractTableName(sql);
            if (tableName != null) routeCache.invalidate(tableName);
            String target = tableName != null ? tableName : sql;
            r = invokeMasterResponseWithRedirect(activeMaster, config, master -> master.dropTable(target));
        } else {
            // Other DDL (CREATE INDEX, DROP INDEX) — forward to region if table name known
            String tableName = extractTableName(sql);
            if (tableName != null) {
                TableLocation loc = resolveLocation(tableName, activeMaster, routeCache, config);
                try (RegionRpcClient region = RegionRpcClient.fromInfo(loc.getPrimaryRS(), config.regionRpcTimeoutMs())) {
                    if (normalized.startsWith("drop index")) {
                        String indexName = extractIndexName(sql);
                        r = region.dropIndex(tableName, indexName != null ? indexName : sql);
                    } else {
                        r = region.createIndex(tableName, sql);
                    }
                }
            } else {
                r = invokeMasterResponseWithRedirect(activeMaster, config, master -> master.createTable(sql));
            }
        }
        printResponse(r);
    }

    private static void handleDml(String sql, String activeMaster, RouteCache routeCache, ClientConfig config)
            throws Exception {
        String tableName = extractTableName(sql);
        if (tableName == null) {
            System.out.println("Error: could not determine table name from: " + sql);
            return;
        }

        TableLocation loc = resolveLocation(tableName, activeMaster, routeCache, config);

        try (RegionRpcClient region = RegionRpcClient.fromInfo(loc.getPrimaryRS(), config.regionRpcTimeoutMs())) {
            QueryResult result = region.execute(tableName, sql);

            // Handle redirect: primary RS has moved
            if (result.getStatus().getCode() == StatusCode.REDIRECT) {
                log.info("REDIRECT received for table={}, invalidating cache", tableName);
                routeCache.invalidate(tableName);
                loc = resolveLocation(tableName, activeMaster, routeCache, config);
                try (RegionRpcClient retry = RegionRpcClient.fromInfo(loc.getPrimaryRS(), config.regionRpcTimeoutMs())) {
                    result = retry.execute(tableName, sql);
                }
            }

            // Handle MOVING: region migration in progress
            if (result.getStatus().getCode() == StatusCode.MOVING) {
                System.out.println("Table is being migrated, please retry shortly.");
                return;
            }

            printQueryResult(result);
        }
    }

    private static TableLocation resolveLocation(String tableName, String activeMaster,
                                                   RouteCache routeCache, ClientConfig config) throws Exception {
        TableLocation loc = routeCache.get(tableName);
        if (loc == null) {
            loc = fetchTableLocationWithRedirect(activeMaster, tableName, config);
            if (loc != null) {
                routeCache.put(tableName, loc);
            } else {
                throw new RuntimeException("Table not found: " + tableName);
            }
        }
        return loc;
    }

    // ─────────────────────── output ───────────────────────────────────────────

    static void printQueryResult(QueryResult result) {
        if (result == null) { System.out.println("(null result)"); return; }

        StatusCode code = result.getStatus().getCode();
        if (code != StatusCode.OK) {
            System.out.println("Error [" + code + "]: " + result.getStatus().getMessage());
            return;
        }

        boolean hasRows = result.isSetRows() && !result.getRows().isEmpty();
        boolean hasColumns = result.isSetColumnNames() && !result.getColumnNames().isEmpty();

        if (hasColumns) {
            // Print header
            List<String> cols = result.getColumnNames();
            String header = String.join(" | ", cols);
            System.out.println(header);
            System.out.println("-".repeat(header.length()));
        }

        if (hasRows) {
            for (Row row : result.getRows()) {
                System.out.println(String.join(" | ", row.getValues()));
            }
            System.out.printf("(%d row%s)%n", result.getRows().size(),
                    result.getRows().size() == 1 ? "" : "s");
        } else if (result.isSetAffectedRows()) {
            System.out.printf("(%d row%s affected)%n", result.getAffectedRows(),
                    result.getAffectedRows() == 1 ? "" : "s");
        } else {
            System.out.println("OK");
        }
    }

    private static void printResponse(Response r) {
        if (r == null) { System.out.println("(null response)"); return; }
        if (r.getCode() == StatusCode.OK) {
            String msg = r.isSetMessage() ? r.getMessage() : "OK";
            System.out.println(msg);
        } else {
            System.out.println("Error [" + r.getCode() + "]: " + r.getMessage());
        }
    }

    private static void printPrompt() {
        System.out.print("SuperSQL> ");
        System.out.flush();
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private static final Pattern INDEX_NAME_PATTERN =
            Pattern.compile("(?i)\\bdrop\\s+index\\s+([a-zA-Z_][a-zA-Z0-9_]*)");

    private static String extractIndexName(String sql) {
        Matcher m = INDEX_NAME_PATTERN.matcher(sql.trim());
        return m.find() ? m.group(1) : null;
    }

    @FunctionalInterface
    interface MasterResponseCall {
        Response call(MasterRpcClient master) throws Exception;
    }

    static Response invokeMasterResponseWithRedirect(String activeMaster,
                                                     ClientConfig config,
                                                     MasterResponseCall call) throws Exception {
        String currentMaster = activeMaster;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (MasterRpcClient master = MasterRpcClient.fromAddress(currentMaster, config.masterRpcTimeoutMs())) {
                Response response = call.call(master);
                if (response != null
                        && response.getCode() == StatusCode.NOT_LEADER
                        && response.isSetRedirectTo()
                        && !response.getRedirectTo().isBlank()) {
                    currentMaster = response.getRedirectTo();
                    continue;
                }
                return response;
            }
        }
        throw new IOException("Master redirect retries exhausted for " + activeMaster);
    }

    static TableLocation fetchTableLocationWithRedirect(String activeMaster,
                                                        String tableName,
                                                        ClientConfig config) throws Exception {
        String currentMaster = activeMaster;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (MasterRpcClient master = MasterRpcClient.fromAddress(currentMaster, config.masterRpcTimeoutMs())) {
                TableLocation location = master.getTableLocation(tableName);
                if (location != null && "NOT_LEADER".equals(location.getTableStatus())) {
                    String redirect = location.isSetPrimaryRS() ? location.getPrimaryRS().getHost() : null;
                    if (redirect != null && !redirect.isBlank()) {
                        currentMaster = redirect;
                        continue;
                    }
                }
                return location;
            }
        }
        throw new IOException("Table location redirect retries exhausted for " + tableName);
    }
}
