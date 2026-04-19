package edu.zju.supersql.regionserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import edu.zju.supersql.regionserver.rpc.RegionAdminServiceImpl;
import edu.zju.supersql.regionserver.rpc.RegionServiceImpl;
import edu.zju.supersql.regionserver.rpc.ReplicaSyncServiceImpl;
import edu.zju.supersql.rpc.RegionAdminService;
import edu.zju.supersql.rpc.RegionService;
import edu.zju.supersql.rpc.ReplicaSyncService;
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
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * RegionServer entry point.
 * Starts:
 *   - ZooKeeper connection via Curator
 *   - Thrift TThreadPoolServer on RS_THRIFT_PORT (default 9090)
 *     with TMultiplexedProcessor: RegionService + RegionAdminService + ReplicaSyncService
 *   - HTTP health endpoint on RS_HTTP_PORT (default 9190)
 */
public class RegionServerMain {

    private static final Logger log = LoggerFactory.getLogger(RegionServerMain.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    static byte[] buildStatusPayload(String rsId,
                                     String rsHost,
                                     int thriftPort,
                                     int httpPort,
                                     String zkConnect,
                                     String dataDir,
                                     String walDir,
                                     boolean miniSqlAlive) {
        return buildStatusPayload(
            rsId,
            rsHost,
            thriftPort,
            httpPort,
            zkConnect,
            dataDir,
            walDir,
            miniSqlAlive,
                null,
                null);
        }

        static byte[] buildStatusPayload(String rsId,
                         String rsHost,
                         int thriftPort,
                         int httpPort,
                         String zkConnect,
                         String dataDir,
                         String walDir,
                         boolean miniSqlAlive,
                                     Map<String, Object> transferManifestVerification,
                                     Map<String, Object> transferTable) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", "ok");
        payload.put("rsId", rsId);
        payload.put("rsHost", rsHost);
        payload.put("thriftPort", thriftPort);
        payload.put("httpPort", httpPort);
        payload.put("zkConnect", zkConnect);
        payload.put("dataDir", dataDir);
        payload.put("walDir", walDir);
        payload.put("miniSqlAlive", miniSqlAlive);
        if (transferManifestVerification == null) {
            Map<String, Object> defaults = new LinkedHashMap<>();
            defaults.put("total", 0L);
            defaults.put("success", 0L);
            defaults.put("failure", 0L);
            defaults.put("lastSuccessTs", 0L);
            defaults.put("lastFailureTs", 0L);
            defaults.put("lastFailureMessage", "");
            payload.put("transferManifestVerification", defaults);
        } else {
            payload.put("transferManifestVerification", transferManifestVerification);
        }

        if (transferTable == null) {
            Map<String, Object> defaults = new LinkedHashMap<>();
            defaults.put("total", 0L);
            defaults.put("success", 0L);
            defaults.put("failure", 0L);
            defaults.put("lastSuccessTs", 0L);
            Map<String, Object> reasons = new LinkedHashMap<>();
            reasons.put("table_not_found", 0L);
            reasons.put("target_reject", 0L);
            reasons.put("transport_error", 0L);
            reasons.put("other", 0L);
            defaults.put("failureReasons", reasons);
            defaults.put("lastFailureTs", 0L);
            defaults.put("lastFailureReason", "");
            defaults.put("lastFailureMessage", "");
            payload.put("transferTable", defaults);
        } else {
            payload.put("transferTable", transferTable);
        }
        payload.put("timestamp", System.currentTimeMillis());
        try {
            return MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to build regionserver status payload", e);
        }
    }

    public static void main(String[] args) throws Exception {
        RegionServerConfig config = RegionServerConfig.fromSystemEnv();
        String rsId = config.rsId();
        String rsHost = config.rsHost();
        int thriftPort = config.thriftPort();
        int httpPort = config.httpPort();
        String zkConnect = config.zkConnect();
        String dataDir = config.dataDir();
        String walDir = config.walDir();
        String miniSqlBin = config.miniSqlBin();
        int minReplicaAcks = config.minReplicaAcks();

        log.info("Starting SuperSQL RegionServer: id={} host={} thriftPort={} httpPort={} zk={}",
                rsId, rsHost, thriftPort, httpPort, zkConnect);
        log.info("  data={} wal={} minisql={} minReplicaAcks={}", dataDir, walDir, miniSqlBin, minReplicaAcks);

        // ── ZooKeeper connection ───────────────────────────────────────────────
        CuratorFramework zkClient = null;
        RegionServerRegistrar registrar = null;
        ScheduledExecutorService heartbeatExecutor = null;
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
            log.info("ZooKeeper client started (connecting to {})", zkConnect);

            registrar = new RegionServerRegistrar(zkClient, rsId);
            registrar.register(rsHost, thriftPort);

            RegionServerRegistrar finalRegistrar = registrar;
            heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "RS-Heartbeat");
                t.setDaemon(true);
                return t;
            });
            heartbeatExecutor.scheduleAtFixedRate(() ->
                            finalRegistrar.heartbeat(rsHost, thriftPort, 0, 0.0, 0.0, 0.0),
                    config.heartbeatIntervalMs(),
                    config.heartbeatIntervalMs(),
                    TimeUnit.MILLISECONDS);
            log.info("RegionServer heartbeat scheduler started (interval={}ms)", config.heartbeatIntervalMs());
        } catch (Exception e) {
            log.warn("ZooKeeper connection failed at startup — proceeding without ZK: {}", e.getMessage());
        }

        // ── Engine Startup & Recovery ──────────────────────────────────────────
        WalManager walManager = new WalManager(walDir);
        walManager.init();

        MiniSqlProcess miniSql = new MiniSqlProcess(miniSqlBin, dataDir);
        try {
            miniSql.start();
            // S4-05: 触发核心崩溃恢复 (Crash Recovery) 重放机制
            walManager.recover(miniSql);
            log.info("RegionServer engine recovery sequence completed.");
        } catch (Exception e) {
            log.error("CRITICAL: Failed to start MiniSql engine or recover data: {}", e.getMessage());
            System.exit(1);
        }

        // ── HTTP health server ─────────────────────────────────────────────────
        HttpServer healthServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        healthServer.createContext("/health", exchange -> {
            byte[] body = "OK".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        final RegionAdminServiceImpl[] adminServiceRef = new RegionAdminServiceImpl[1];
        healthServer.createContext("/status", exchange -> {
            Map<String, Object> manifestStats = adminServiceRef[0] != null
                    ? adminServiceRef[0].getTransferManifestVerificationStats()
                    : null;
            Map<String, Object> transferTableStats = adminServiceRef[0] != null
                ? adminServiceRef[0].getTransferTableStats()
                : null;
            byte[] body = buildStatusPayload(
                    rsId,
                    rsHost,
                    thriftPort,
                    httpPort,
                    zkConnect,
                    dataDir,
                    walDir,
                    miniSql.isAlive(),
                    manifestStats,
                    transferTableStats);
            exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        healthServer.start();
        log.info("Health endpoints listening on :{} (/health, /status)", httpPort);

        // ── Service wiring ────────────────────────────────────────────────────
        WriteGuard writeGuard = new WriteGuard();
        ReplicaManager replicaManager = new ReplicaManager();
        String selfAddress = rsHost + ":" + thriftPort;

        ReplicaSyncServiceImpl replicaSync = new ReplicaSyncServiceImpl(miniSql, walManager);
        replicaSync.init();
        
        RegionServiceImpl regionService = new RegionServiceImpl(
            miniSql, walManager, replicaManager, writeGuard, zkClient, selfAddress, minReplicaAcks);
        RegionAdminServiceImpl adminService = new RegionAdminServiceImpl(
                writeGuard, zkClient, dataDir, rsId);
        adminServiceRef[0] = adminService;

        // ── Thrift TMultiplexedProcessor ──────────────────────────────────────
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionService",
                new RegionService.Processor<>(regionService));
        processor.registerProcessor("RegionAdminService",
                new RegionAdminService.Processor<>(adminService));
        processor.registerProcessor("ReplicaSyncService",
                new ReplicaSyncService.Processor<>(replicaSync));

        TServerSocket serverTransport = new TServerSocket(thriftPort);
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(4)
                .maxWorkerThreads(32);

        TThreadPoolServer server = new TThreadPoolServer(serverArgs);
        log.info("RegionServer {} (Thrift) listening on :{}", rsId, thriftPort);

        // ── Periodic Checkpoint Thread ─────────────────────────────────────────
        Thread checkpointThread = new Thread(() -> {
            log.info("Starting background WAL checkpoint thread...");
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    // Checkpoint every 5 minutes (default)
                    Thread.sleep(TimeUnit.MINUTES.toMillis(5));
                    if (miniSql.isAlive()) {
                        walManager.performCheckpoint(miniSql);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("Error in background checkpoint thread: ", e);
                }
            }
        }, "CheckpointThread");
        checkpointThread.setDaemon(true);
        checkpointThread.start();

        server.serve();
    }
}
