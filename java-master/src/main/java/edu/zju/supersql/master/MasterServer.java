package edu.zju.supersql.master;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import edu.zju.supersql.master.balance.RebalanceScheduler;
import edu.zju.supersql.master.election.LeaderElector;
import edu.zju.supersql.master.meta.MetaManager;
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
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;

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

    static ScheduledExecutorService startActiveHeartbeatScheduler(long intervalMs) {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Master-Active-Heartbeat");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(
                MasterRuntimeContext::updateActiveHeartbeat,
                0,
                intervalMs,
                TimeUnit.MILLISECONDS);
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
        return buildStatusPayload(thriftPort, httpPort, zkConnect, null, null);
    }

    static byte[] buildStatusPayload(int thriftPort,
                                     int httpPort,
                                     String zkConnect,
                                     RebalanceScheduler rebalanceScheduler) {
        return buildStatusPayload(thriftPort, httpPort, zkConnect, rebalanceScheduler, null);
    }

    static byte[] buildStatusPayload(int thriftPort,
                                     int httpPort,
                                     String zkConnect,
                                     RebalanceScheduler rebalanceScheduler,
                                     MasterServiceImpl.RouteRepairSnapshot routeRepairSnapshot) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("status", "ok");
        payload.put("role", resolveRole());
        payload.put("masterId", MasterRuntimeContext.getMasterId());
        payload.put("address", MasterRuntimeContext.getMasterAddress());
        payload.put("thriftPort", thriftPort);
        payload.put("httpPort", httpPort);
        payload.put("zkConnect", zkConnect);
        payload.put("zkReady", MasterRuntimeContext.isReady());
        payload.put("rebalanceScheduler", buildRebalanceSchedulerPayload(rebalanceScheduler));
        payload.put("routeRepair", buildRouteRepairPayload(routeRepairSnapshot));
        payload.put("timestamp", System.currentTimeMillis());
        try {
            return MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build status payload", e);
        }
    }

    private static Map<String, Object> buildRebalanceSchedulerPayload(RebalanceScheduler rebalanceScheduler) {
        Map<String, Object> status = new LinkedHashMap<>();
        if (rebalanceScheduler == null) {
            status.put("available", false);
            return status;
        }
        RebalanceScheduler.Snapshot snapshot = rebalanceScheduler.snapshot();
        status.put("available", true);
        status.put("enabled", snapshot.enabled());
        status.put("started", snapshot.started());
        status.put("intervalMs", snapshot.intervalMs());
        status.put("minGapMs", snapshot.minGapMs());
        status.put("tickCount", snapshot.tickCount());
        status.put("triggerCount", snapshot.triggerCount());
        status.put("throttledCount", snapshot.throttledCount());
        status.put("successCount", snapshot.successCount());
        status.put("failureCount", snapshot.failureCount());
        status.put("externalRequestCount", snapshot.externalRequestCount());
        status.put("lastAttemptAtMs", snapshot.lastAttemptAtMs());
        status.put("lastSuccessAtMs", snapshot.lastSuccessAtMs());
        status.put("lastFailureAtMs", snapshot.lastFailureAtMs());
        status.put("lastError", snapshot.lastError());
        status.put("lastTriggerReason", snapshot.lastTriggerReason());
        return status;
    }

    private static Map<String, Object> buildRouteRepairPayload(MasterServiceImpl.RouteRepairSnapshot snapshot) {
        Map<String, Object> status = new LinkedHashMap<>();
        if (snapshot == null) {
            status.put("available", false);
            return status;
        }
        status.put("available", true);
        status.put("runCount", snapshot.runCount());
        status.put("totalRepairedTables", snapshot.totalRepairedTables());
        status.put("lastRunAtMs", snapshot.lastRunAtMs());
        status.put("lastRunRepairedCount", snapshot.lastRunRepairedCount());
        status.put("lastRepairedTable", snapshot.lastRepairedTable());
        return status;
    }

    static RegionServerWatcher.Listener buildMembershipRebalanceListener(RebalanceScheduler rebalanceScheduler) {
        return buildMembershipRebalanceListener(rebalanceScheduler, () -> 0);
    }

    static RegionServerWatcher.Listener buildMembershipRebalanceListener(RebalanceScheduler rebalanceScheduler,
                                                                         IntSupplier routeRepairTrigger) {
        return new RegionServerWatcher.Listener() {
            @Override
            public void onRegionServerUp(String rsId) {
                rebalanceScheduler.requestTrigger("rs_up:" + rsId);
                try {
                    routeRepairTrigger.getAsInt();
                } catch (Exception e) {
                    log.warn("Membership up repair trigger failed rsId={} cause={}", rsId, e.getMessage());
                }
            }

            @Override
            public void onRegionServerDown(String rsId) {
                rebalanceScheduler.requestTrigger("rs_down:" + rsId);
                try {
                    routeRepairTrigger.getAsInt();
                } catch (Exception e) {
                    log.warn("Membership down repair trigger failed rsId={} cause={}", rsId, e.getMessage());
                }
            }
        };
    }

    static int preloadMetadataFromZk(CuratorFramework zkClient) {
        if (zkClient == null) {
            return 0;
        }
        try {
            int tableCount = new MetaManager(zkClient).listTables().size();
            log.info("Metadata preload completed: tableCount={}", tableCount);
            return tableCount;
        } catch (Exception e) {
            log.warn("Metadata preload failed, continue startup: {}", e.getMessage());
            return 0;
        }
    }

    static TMultiplexedProcessor buildThriftProcessor() {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("MasterService",
                new MasterService.Processor<>(new MasterServiceImpl()));
        return processor;
    }

    public static void main(String[] args) throws Exception {
        MasterConfig config = MasterConfig.fromSystemEnv();
        int thriftPort = config.thriftPort();
        int httpPort = config.httpPort();
        String zkConnect = config.zkConnect();
        String masterId = config.masterId();

        log.info("Starting SuperSQL Master: id={} thriftPort={} httpPort={} zk={}",
                masterId, thriftPort, httpPort, zkConnect);

        // ── ZooKeeper connection (best-effort at startup) ──────────────────────
        CuratorFramework zkClient = null;
        LeaderElector leaderElector = null;
        RegionServerWatcher regionServerWatcher = null;
        RebalanceScheduler rebalanceScheduler = null;
        MasterServiceImpl scheduledService = null;
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
            for (String path : ZkPaths.bootstrapPaths()) {
                if (zkClient.checkExists().forPath(path) == null) {
                    zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
                    log.info("Created ZK base path: {}", path);
                }
            }
            log.info("ZK base directories initialized");

            MasterRuntimeContext.initialize(zkClient, masterId, thriftPort);
            MasterRuntimeContext.tryBootstrapActiveMaster();
            startActiveHeartbeatScheduler(config.heartbeatIntervalMs());
            log.info("Active heartbeat scheduler started (interval={}ms)", config.heartbeatIntervalMs());

            leaderElector = new LeaderElector(zkClient, masterId, masterId + ":" + thriftPort);
            leaderElector.start();
            preloadMetadataFromZk(zkClient);
                scheduledService = new MasterServiceImpl();
            rebalanceScheduler = new RebalanceScheduler(
                    config.rebalanceSchedulerEnabled(),
                    config.rebalanceIntervalMs(),
                    config.rebalanceMinGapMs(),
                    scheduledService::triggerRebalance);
            rebalanceScheduler.start();
                regionServerWatcher = new RegionServerWatcher(zkClient,
                    buildMembershipRebalanceListener(rebalanceScheduler,
                            scheduledService::repairTableRoutesBestEffort));
                regionServerWatcher.start();
            LeaderElector finalLeaderElector = leaderElector;
            RegionServerWatcher finalRegionServerWatcher = regionServerWatcher;
            RebalanceScheduler finalRebalanceScheduler = rebalanceScheduler;
            Runtime.getRuntime().addShutdownHook(new Thread(finalLeaderElector::close));
            Runtime.getRuntime().addShutdownHook(new Thread(finalRegionServerWatcher::close));
            Runtime.getRuntime().addShutdownHook(new Thread(finalRebalanceScheduler::close));
        } catch (Exception e) {
            log.warn("ZooKeeper connection failed at startup — proceeding without ZK: {}", e.getMessage());
        }

        // ── HTTP health server ─────────────────────────────────────────────────
        HttpServer healthServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        RebalanceScheduler statusScheduler = rebalanceScheduler;
        MasterServiceImpl statusService = scheduledService;
        healthServer.createContext("/health", exchange -> {
            byte[] body = buildHealthPayload();
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        healthServer.createContext("/status", exchange -> {
            byte[] body = buildStatusPayload(
                    thriftPort,
                    httpPort,
                    zkConnect,
                    statusScheduler,
                    statusService == null ? null : statusService.routeRepairSnapshot());
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        healthServer.start();
        log.info("Health endpoints listening on :{} (/health, /status)", httpPort);

        // ── Thrift TThreadPoolServer ───────────────────────────────────────────
        TMultiplexedProcessor processor = buildThriftProcessor();

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
