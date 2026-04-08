package edu.zju.supersql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SuperSQL interactive REPL client.
 *
 * Startup sequence:
 *   1. Connect to ZooKeeper; read /active-master to discover Active Master.
 *   2. REPL loop: DDL → MasterRpcClient, DML → RegionRpcClient via RouteCache.
 *
 * Sprint 3 will implement full routing; this skeleton keeps the process alive
 * and confirms ZK connectivity so the Docker environment can be validated.
 */
public class SqlClient {

    private static final Logger log = LoggerFactory.getLogger(SqlClient.class);
    private static final Pattern TABLE_NAME_PATTERN =
            Pattern.compile("(?i)\\b(from|into|table|update)\\s+([a-zA-Z_][a-zA-Z0-9_]*)");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    enum SqlKind {
        DDL,
        DML,
        UNKNOWN
    }

    static SqlKind classifySql(String sql) {
        if (sql == null || sql.isBlank()) {
            return SqlKind.UNKNOWN;
        }
        String normalized = sql.trim().toLowerCase();
        if (normalized.startsWith("create")
                || normalized.startsWith("drop")
                || normalized.startsWith("alter")
                || normalized.startsWith("truncate")) {
            return SqlKind.DDL;
        }
        if (normalized.startsWith("select")
                || normalized.startsWith("insert")
                || normalized.startsWith("update")
                || normalized.startsWith("delete")) {
            return SqlKind.DML;
        }
        return SqlKind.UNKNOWN;
    }

    static String extractTableName(String sql) {
        if (sql == null) {
            return null;
        }
        Matcher matcher = TABLE_NAME_PATTERN.matcher(sql.trim());
        if (matcher.find()) {
            return matcher.group(2);
        }
        return null;
    }

    static String readActiveMaster(CuratorFramework zkClient, String fallback) {
        if (zkClient == null) {
            return fallback;
        }
        try {
            if (zkClient.checkExists().forPath("/active-master") == null) {
                return fallback;
            }
            byte[] bytes = zkClient.getData().forPath("/active-master");
            if (bytes == null || bytes.length == 0) {
                return fallback;
            }
            Map<?, ?> node = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
            Object address = node.get("address");
            if (address != null && !String.valueOf(address).isBlank()) {
                return String.valueOf(address);
            }
            Object id = node.get("masterId");
            return id == null ? fallback : String.valueOf(id);
        } catch (Exception e) {
            log.warn("Failed to read /active-master: {}", e.getMessage());
            return fallback;
        }
    }

    public static void main(String[] args) throws IOException {
        String zkConnect = System.getenv().getOrDefault("ZK_CONNECT", "zk1:2181,zk2:2181,zk3:2181");
        String masterFallback = System.getenv().getOrDefault("MASTER_ADDR", "master-1:8080");
        log.info("SuperSQL Client starting, ZK={}", zkConnect);

        // ── ZooKeeper connection ───────────────────────────────────────────────
        CuratorFramework zkClient = null;
        RouteCache routeCache = new RouteCache(30_000);
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
            } else {
                log.warn("ZooKeeper connection timed out — some features may be unavailable");
            }
            activeMaster = readActiveMaster(zkClient, masterFallback);
            log.info("Discovered active master: {}", activeMaster);
        } catch (Exception e) {
            log.warn("ZooKeeper init failed: {} — running in offline mode", e.getMessage());
        }

        // ── REPL ──────────────────────────────────────────────────────────────
        // Check if running interactively (stdin is a terminal) or as daemon
        boolean interactive = System.console() != null || System.in.available() > 0;
        if (!interactive) {
            log.info("No interactive terminal detected — running in daemon/standby mode");
            log.info("Attach with: docker exec -it client java -jar /app/client.jar");
            // Keep alive so Docker considers the container running
            try { Thread.currentThread().join(); } catch (InterruptedException ignored) {}
            return;
        }

        System.out.println("SuperSQL> Connected to cluster (stub mode — SQL execution not yet implemented)");
        System.out.println("SuperSQL> Type 'exit' or 'quit' to disconnect.");
        System.out.print("SuperSQL> ");
        System.out.flush();

        try (Scanner sc = new Scanner(System.in)) {
            while (sc.hasNextLine()) {
                String line = sc.nextLine().trim();
                if (line.isEmpty()) {
                    System.out.print("SuperSQL> ");
                    System.out.flush();
                    continue;
                }
                if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                    System.out.println("Bye.");
                    break;
                }

                SqlKind kind = classifySql(line);
                String tableName = extractTableName(line);

                if (kind == SqlKind.DDL) {
                    System.out.println("[route] DDL -> Master " + activeMaster + " (not yet executed)");
                } else if (kind == SqlKind.DML) {
                    if (tableName == null) {
                        System.out.println("[route] DML -> unable to infer table name");
                    } else {
                        TableLocation loc = routeCache.get(tableName);
                        if (loc == null) {
                            System.out.println("[route] cache miss for table '" + tableName + "', query Master "
                                    + activeMaster + " (not yet executed)");
                        } else {
                            System.out.println("[route] cache hit table '" + tableName + "' -> "
                                    + loc.getPrimaryRS().getHost() + ":" + loc.getPrimaryRS().getPort()
                                    + " (not yet executed)");
                        }
                    }
                } else {
                    System.out.println("[route] unknown SQL kind: " + line);
                }

                System.out.print("SuperSQL> ");
                System.out.flush();
            }
        }

        if (zkClient != null) {
            zkClient.close();
        }
    }
}
