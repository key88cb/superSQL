package edu.zju.supersql.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import edu.zju.supersql.master.election.LeaderElector;
import edu.zju.supersql.master.rpc.MasterServiceImpl;
import edu.zju.supersql.rpc.MasterService;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Master server entry point.
 * Starts:
 *   - ZooKeeper connection via Curator (non-fatal if ZK unavailable at boot)
 *   - Thrift TThreadPoolServer on MASTER_THRIFT_PORT (default 8080)
 *   - HTTP health endpoint on MASTER_HTTP_PORT (default 8880)
 */
public class MasterServer {

    private static final Logger log = LoggerFactory.getLogger(MasterServer.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static ScheduledExecutorService startActiveHeartbeatScheduler() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Master-Active-Heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(
                MasterRuntimeContext::updateActiveHeartbeat,
                0,
                5,
                TimeUnit.SECONDS);
        return scheduler;
    }

    static String resolveRole() {
        return MasterRuntimeContext.isActiveMaster() ? "ACTIVE" : "STANDBY";
    }

    static byte[] buildHealthPayload() {
        Map<String, Object> payload = new HashMap<>();
        payload.put("status", "ok");
        payload.put("role", resolveRole());
        try {
            return MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build health payload", e);
        }
    }

    static byte[] buildStatusPayload(int thriftPort, int httpPort, String zkConnect) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("status", "ok");
        payload.put("role", resolveRole());
        payload.put("masterId", MasterRuntimeContext.getMasterId());
        payload.put("address", MasterRuntimeContext.getMasterAddress());
        payload.put("thriftPort", thriftPort);
        payload.put("httpPort", httpPort);
        payload.put("zkConnect", zkConnect);
        payload.put("zkReady", MasterRuntimeContext.isReady());
        payload.put("timestamp", System.currentTimeMillis());
        try {
            return MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build status payload", e);
        }
    }

    public static void main(String[] args) throws Exception {
        int thriftPort = Integer.parseInt(System.getenv().getOrDefault("MASTER_THRIFT_PORT", "8080"));
        int httpPort   = Integer.parseInt(System.getenv().getOrDefault("MASTER_HTTP_PORT",   "8880"));
        String zkConnect = System.getenv().getOrDefault("ZK_CONNECT", "zk1:2181,zk2:2181,zk3:2181");
        String masterId  = System.getenv().getOrDefault("MASTER_ID", "master-1");

        log.info("Starting SuperSQL Master: id={} thriftPort={} httpPort={} zk={}",
                masterId, thriftPort, httpPort, zkConnect);

        // ── ZooKeeper connection (best-effort at startup) ──────────────────────
        CuratorFramework zkClient = null;
        LeaderElector leaderElector = null;
        try {
            RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkConnect)
                    .sessionTimeoutMs(30_000)
                    .connectionTimeoutMs(10_000)
                    .retryPolicy(retry)
                    .namespace("supersql")
                    .build();
            zkClient.start();
            zkClient.blockUntilConnected(10, java.util.concurrent.TimeUnit.SECONDS);
            log.info("ZooKeeper client started (connecting to {})", zkConnect);

            // S0-06: 创建 ZK 基础目录（namespace="supersql"，实际路径为 /supersql/masters 等）
            String[] basePaths = {"/masters", "/masters/active-heartbeat", "/region_servers", "/meta/tables", "/assignments", "/active-master"};
            for (String path : basePaths) {
                if (zkClient.checkExists().forPath(path) == null) {
                    zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
                    log.info("Created ZK base path: {}", path);
                }
            }
            log.info("ZK base directories initialized");

            MasterRuntimeContext.initialize(zkClient, masterId, thriftPort);
            MasterRuntimeContext.tryBootstrapActiveMaster();
            startActiveHeartbeatScheduler();
            log.info("Active heartbeat scheduler started (interval=5s)");

            leaderElector = new LeaderElector(zkClient, masterId, masterId + ":" + thriftPort);
            leaderElector.start();
            LeaderElector finalLeaderElector = leaderElector;
            Runtime.getRuntime().addShutdownHook(new Thread(finalLeaderElector::close));

            // TODO Sprint 1: MetaManager.init(zkClient)
        } catch (Exception e) {
            log.warn("ZooKeeper connection failed at startup — proceeding without ZK: {}", e.getMessage());
        }

        // ── HTTP health server ─────────────────────────────────────────────────
        HttpServer healthServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        healthServer.createContext("/health", exchange -> {
            byte[] body = buildHealthPayload();
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        healthServer.createContext("/status", exchange -> {
            byte[] body = buildStatusPayload(thriftPort, httpPort, zkConnect);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        healthServer.start();
        log.info("Health endpoints listening on :{} (/health, /status)", httpPort);

        // ── Thrift TThreadPoolServer ───────────────────────────────────────────
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("MasterService",
                new MasterService.Processor<>(new MasterServiceImpl()));
        // TODO Sprint 1: add more processors if needed

        TServerSocket serverTransport = new TServerSocket(thriftPort);
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(4)
                .maxWorkerThreads(32);

        TThreadPoolServer server = new TThreadPoolServer(serverArgs);
        log.info("MasterServer (Thrift) listening on :{}", thriftPort);

        // Serve blocks the thread — run it in foreground
        server.serve();
    }
}
