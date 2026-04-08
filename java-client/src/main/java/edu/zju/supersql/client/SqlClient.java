package edu.zju.supersql.client;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Scanner;

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

    public static void main(String[] args) throws IOException {
        String zkConnect = System.getenv().getOrDefault("ZK_CONNECT", "zk1:2181,zk2:2181,zk3:2181");
        log.info("SuperSQL Client starting, ZK={}", zkConnect);

        // ── ZooKeeper connection ───────────────────────────────────────────────
        CuratorFramework zkClient = null;
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
            // TODO Sprint 3: read /active-master node, build MasterRpcClient
            // TODO Sprint 3: initialize RouteCache(ttlMs=30_000)
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
                // TODO Sprint 3: route SQL to Master or RegionServer
                System.out.println("[stub] SQL not yet routed: " + line);
                System.out.print("SuperSQL> ");
                System.out.flush();
            }
        }

        if (zkClient != null) {
            zkClient.close();
        }
    }
}
