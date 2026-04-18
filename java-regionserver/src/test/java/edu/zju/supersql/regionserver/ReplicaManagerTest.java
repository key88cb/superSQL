package edu.zju.supersql.regionserver;

import edu.zju.supersql.regionserver.rpc.ReplicaSyncServiceImpl;
import edu.zju.supersql.rpc.ReplicaSyncService;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

    @Test
    void commitOneWithRetryShouldReturnFalseForUnreachableReplica() {
        ReplicaManager manager = new ReplicaManager();
        boolean committed = manager.commitOneWithRetry("orders", 999L, "127.0.0.1:1");
        Assertions.assertFalse(committed);
    }

    @Test
    void commitOneWithRetryShouldReturnTrueWhenReplicaHasEntry() {
        ReplicaManager manager = new ReplicaManager();
        WalEntry entry = buildEntry(44L, "orders", "insert into orders values(44);");

        int acks = manager.syncToReplicas(entry, List.of("127.0.0.1:" + port1), 1);
        Assertions.assertEquals(1, acks);

        boolean committed = manager.commitOneWithRetry("orders", 44L, "127.0.0.1:" + port1);
        Assertions.assertTrue(committed);
    }

    @Test
    void reconcileReplicasShouldBackfillLaggingReplicaUsingPullLog() throws Exception {
        int donorPort = freePort();
        int laggingPort = freePort();
        InMemoryReplicaSyncService donor = new InMemoryReplicaSyncService();
        InMemoryReplicaSyncService lagging = new InMemoryReplicaSyncService();
        donor.preload(buildEntry(100L, "orders", "insert into orders values(100);"), true);
        donor.preload(buildEntry(101L, "orders", "insert into orders values(101);"), true);
        lagging.preload(buildEntry(100L, "orders", "insert into orders values(100);"), true);

        TServer donorServer = buildServer(donorPort, donor);
        TServer laggingServer = buildServer(laggingPort, lagging);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        pool.submit(donorServer::serve);
        pool.submit(laggingServer::serve);
        Thread.sleep(200);

        try {
            ReplicaManager manager = new ReplicaManager();
            String donorAddress = "127.0.0.1:" + donorPort;
            String laggingAddress = "127.0.0.1:" + laggingPort;

            Assertions.assertEquals(101L, getMaxLsn(donorAddress, "orders"));
            Assertions.assertEquals(100L, getMaxLsn(laggingAddress, "orders"));

            manager.reconcileReplicas("orders", 101L, List.of(donorAddress, laggingAddress));

            Assertions.assertEquals(101L, getMaxLsn(laggingAddress, "orders"));
            Response secondCommit = commitLog(laggingAddress, "orders", 101L);
            Assertions.assertEquals(StatusCode.OK, secondCommit.getCode());
            Assertions.assertTrue(secondCommit.getMessage().contains("ALREADY_COMMITTED"));
        } finally {
            donorServer.stop();
            laggingServer.stop();
            pool.shutdownNow();
        }
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

    private static long getMaxLsn(String address, String tableName) throws Exception {
        String[] hostPort = address.split(":");
        try (TTransport transport = new TFramedTransport(
                new TSocket(hostPort[0], Integer.parseInt(hostPort[1]), 3_000))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            return client.getMaxLsn(tableName);
        }
    }

    private static Response commitLog(String address, String tableName, long lsn) throws Exception {
        String[] hostPort = address.split(":");
        try (TTransport transport = new TFramedTransport(
                new TSocket(hostPort[0], Integer.parseInt(hostPort[1]), 3_000))) {
            transport.open();
            TMultiplexedProtocol protocol = new TMultiplexedProtocol(
                    new TBinaryProtocol(transport), "ReplicaSyncService");
            ReplicaSyncService.Client client = new ReplicaSyncService.Client(protocol);
            return client.commitLog(tableName, lsn);
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static final class InMemoryReplicaSyncService implements ReplicaSyncService.Iface {
        private final ConcurrentSkipListMap<Long, WalEntry> walByLsn = new ConcurrentSkipListMap<>();
        private final Set<Long> committed = ConcurrentHashMap.newKeySet();

        void preload(WalEntry entry, boolean isCommitted) {
            walByLsn.put(entry.getLsn(), new WalEntry(entry));
            if (isCommitted) {
                committed.add(entry.getLsn());
            }
        }

        @Override
        public Response syncLog(WalEntry entry) {
            walByLsn.put(entry.getLsn(), new WalEntry(entry));
            Response r = new Response(StatusCode.OK);
            r.setMessage("ACK lsn=" + entry.getLsn());
            return r;
        }

        @Override
        public List<WalEntry> pullLog(String tableName, long startLsn) {
            if (tableName == null || tableName.isBlank()) {
                return Collections.emptyList();
            }
            List<WalEntry> result = new ArrayList<>();
            for (WalEntry entry : walByLsn.tailMap(startLsn).values()) {
                if (tableName.equals(entry.getTableName())) {
                    result.add(new WalEntry(entry));
                }
            }
            return result;
        }

        @Override
        public long getMaxLsn(String tableName) {
            if (tableName == null || tableName.isBlank()) {
                return -1L;
            }
            long max = -1L;
            for (WalEntry entry : walByLsn.values()) {
                if (tableName.equals(entry.getTableName())) {
                    max = Math.max(max, entry.getLsn());
                }
            }
            return max;
        }

        @Override
        public Response commitLog(String tableName, long lsn) {
            WalEntry entry = walByLsn.get(lsn);
            if (entry == null || !tableName.equals(entry.getTableName())) {
                Response r = new Response(StatusCode.TABLE_NOT_FOUND);
                r.setMessage("No wal entry found for table=" + tableName + " lsn=" + lsn);
                return r;
            }
            if (!committed.add(lsn)) {
                Response r = new Response(StatusCode.OK);
                r.setMessage("ALREADY_COMMITTED lsn=" + lsn);
                return r;
            }
            Response r = new Response(StatusCode.OK);
            r.setMessage("COMMITTED lsn=" + lsn);
            return r;
        }
    }
}
