package edu.zju.supersql.regionserver;

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

    public static void main(String[] args) throws Exception {
        String rsId      = System.getenv().getOrDefault("RS_ID",      "rs-1");
        String rsHost    = System.getenv().getOrDefault("RS_HOST",    "rs-1");
        int thriftPort   = Integer.parseInt(System.getenv().getOrDefault("RS_THRIFT_PORT", "9090"));
        int httpPort     = Integer.parseInt(System.getenv().getOrDefault("RS_HTTP_PORT",   "9190"));
        String zkConnect = System.getenv().getOrDefault("ZK_CONNECT", "zk1:2181,zk2:2181,zk3:2181");
        String dataDir   = System.getenv().getOrDefault("RS_DATA_DIR",  "/data/db");
        String walDir    = System.getenv().getOrDefault("RS_WAL_DIR",   "/data/wal");
        String miniSqlBin= System.getenv().getOrDefault("MINISQL_BIN",  "/opt/minisql/main");

        log.info("Starting SuperSQL RegionServer: id={} host={} thriftPort={} httpPort={} zk={}",
                rsId, rsHost, thriftPort, httpPort, zkConnect);
        log.info("  data={} wal={} minisql={}", dataDir, walDir, miniSqlBin);

        // ── ZooKeeper connection ───────────────────────────────────────────────
        CuratorFramework zkClient = null;
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
            // TODO Sprint 2: RegionServerRegistrar.register(zkClient, rsId, rsHost, thriftPort)
            // TODO Sprint 2: start heartbeat thread (every 10s)
        } catch (Exception e) {
            log.warn("ZooKeeper connection failed at startup — proceeding without ZK: {}", e.getMessage());
        }

        // ── Engine Startup & Recovery ──────────────────────────────────────────
        WalManager walManager = new WalManager(walDir);
        walManager.init();

        MiniSqlProcess miniSql = new MiniSqlProcess(miniSqlBin, dataDir);
        try {
            miniSql.start();
            log.info("RegionServer engine recovery sequence completed.");
        } catch (Exception e) {
            log.error("CRITICAL: Failed to start MiniSql engine during recovery: {}", e.getMessage());
            System.exit(1);
        }

        // ── HTTP health server ─────────────────────────────────────────────────
        HttpServer healthServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        healthServer.createContext("/health", exchange -> {
            byte[] body = "OK".getBytes();
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) { os.write(body); }
        });
        healthServer.start();
        log.info("Health endpoint listening on :{}/health", httpPort);

        // ── Thrift TMultiplexedProcessor ──────────────────────────────────────
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionService",
                new RegionService.Processor<>(new RegionServiceImpl()));
        processor.registerProcessor("RegionAdminService",
                new RegionAdminService.Processor<>(new RegionAdminServiceImpl()));
        processor.registerProcessor("ReplicaSyncService",
                new ReplicaSyncService.Processor<>(new ReplicaSyncServiceImpl()));

        TServerSocket serverTransport = new TServerSocket(thriftPort);
        TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(4)
                .maxWorkerThreads(32);

        TThreadPoolServer server = new TThreadPoolServer(serverArgs);
        log.info("RegionServer {} (Thrift) listening on :{}", rsId, thriftPort);

        server.serve();
    }
}
