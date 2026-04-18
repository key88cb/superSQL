package edu.zju.supersql.regionserver;

import edu.zju.supersql.regionserver.rpc.ReplicaSyncServiceImpl;
import edu.zju.supersql.rpc.ReplicaSyncService;
import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Integration test for ReplicaManager using embedded Thrift servers.
 * Does not require ZooKeeper or MiniSQL.
 */
class ReplicaManagerTest {

    private TServer server1;
    private TServer server2;
    private ExecutorService serverPool;
    private int port1;
    private int port2;

    @BeforeEach
    void setUp() throws Exception {
        ReplicaSyncServiceImpl.resetForTests();

        MiniSqlProcess mockMiniSql = Mockito.mock(MiniSqlProcess.class);
        Mockito.when(mockMiniSql.execute(Mockito.anyString())).thenReturn(">>> SUCCESS\n");

        port1 = freePort();
        port2 = freePort();
        WalManager mockWal = Mockito.mock(WalManager.class);
        server1 = buildServer(port1, new ReplicaSyncServiceImpl(mockMiniSql, mockWal));
        server2 = buildServer(port2, new ReplicaSyncServiceImpl(mockMiniSql, mockWal));

        serverPool = Executors.newFixedThreadPool(2);
        serverPool.submit(server1::serve);
        serverPool.submit(server2::serve);
        Thread.sleep(200); // allow servers to start
    }

    @AfterEach
    void tearDown() {
        if (server1 != null) server1.stop();
        if (server2 != null) server2.stop();
        if (serverPool != null) serverPool.shutdownNow();
        ReplicaSyncServiceImpl.resetForTests();
    }

    @Test
    void syncToReplicasReturnsAckWhenAtLeastOneServerResponds() {
        ReplicaManager manager = new ReplicaManager();
        WalEntry entry = buildEntry(10L, "orders", "insert into orders values(1);");

        int acks = manager.syncToReplicas(entry,
                List.of("127.0.0.1:" + port1, "127.0.0.1:" + port2));
        Assertions.assertTrue(acks >= 1, "Expected at least 1 ACK, got " + acks);
    }

    @Test
    void syncToReplicasShouldReachRequiredAckCountWhenAvailable() {
        ReplicaManager manager = new ReplicaManager();
        WalEntry entry = buildEntry(11L, "orders", "insert into orders values(11);");

        int acks = manager.syncToReplicas(entry,
                List.of("127.0.0.1:" + port1, "127.0.0.1:" + port2), 2);

        Assertions.assertEquals(2, acks, "Expected all ACKs to be collected for requiredAcks=2");
    }

    @Test
    void syncToReplicasReturnsZeroWhenNoServersAvailable() {
        ReplicaManager manager = new ReplicaManager();
        WalEntry entry = buildEntry(20L, "orders", "insert into orders values(2);");

        // Use non-existent ports
        int acks = manager.syncToReplicas(entry, List.of("127.0.0.1:1", "127.0.0.1:2"));
        Assertions.assertEquals(0, acks);
    }

    @Test
    void syncToReplicasEmptyListReturnsZero() {
        ReplicaManager manager = new ReplicaManager();
        WalEntry entry = buildEntry(30L, "t", "insert into t values(3);");
        int acks = manager.syncToReplicas(entry, List.of());
        Assertions.assertEquals(0, acks);
    }

    @Test
    void commitOnReplicasCompletesWithoutException() throws InterruptedException {
        ReplicaManager manager = new ReplicaManager();
        // commitOnReplicas is fire-and-forget; just verify no exception
        manager.commitOnReplicas("orders", 10L,
                List.of("127.0.0.1:" + port1, "127.0.0.1:" + port2));
        Thread.sleep(500); // give async tasks time to complete
        // No assertion needed — test passes if no exception is thrown
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private static TServer buildServer(int port, ReplicaSyncService.Iface impl) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("ReplicaSyncService",
                new ReplicaSyncService.Processor<>(impl));
        TServerSocket transport = new TServerSocket(port);
        return new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(4));
    }

    private static WalEntry buildEntry(long lsn, String tableName, String sql) {
        WalEntry entry = new WalEntry(lsn, lsn * 10, tableName,
                WalOpType.INSERT, System.currentTimeMillis());
        entry.setAfterRow(sql.getBytes(StandardCharsets.UTF_8));
        return entry;
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
