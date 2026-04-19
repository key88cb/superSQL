package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.ReplicaManager;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InOrder;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

/**
 * Unit tests for RegionServiceImpl.
 * MiniSqlProcess, ReplicaManager are mocked; WalManager uses a real temp directory.
 */
class RegionServiceImplTest {

    @TempDir
    Path walDir;

    private MiniSqlProcess mockMiniSql;
    private ReplicaManager mockReplicaManager;
    private WalManager walManager;
    private WriteGuard writeGuard;
    private RegionServiceImpl service;

    @BeforeEach
    void setUp() throws Exception {
        mockMiniSql       = Mockito.mock(MiniSqlProcess.class);
        mockReplicaManager = Mockito.mock(ReplicaManager.class);

        // Default: miniSQL returns SUCCESS; replicas return 1 ACK
        Mockito.when(mockMiniSql.execute(Mockito.anyString()))
                .thenReturn(">>> SUCCESS\n");
        Mockito.when(mockReplicaManager.syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.anyInt()))
                .thenReturn(1);

        walManager = new WalManager(walDir.toString());
        walManager.init();

        writeGuard = new WriteGuard();

        // ZkClient = null → no replica address lookup, empty list.
        // Use minReplicaAcks=0 as permissive unit-test baseline; strict behavior is covered by dedicated tests below.
        service = new RegionServiceImpl(mockMiniSql, walManager, mockReplicaManager,
            writeGuard, null, "rs-1:9090", 0);
    }

    // ── write path ───────────────────────────────────────────────────────────

    @Test
    void insertGoesThoughWalAndReplicaSync() throws Exception {
        String sql = "insert into orders values(1,'test');";
        QueryResult result = service.execute("orders", sql);

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());

        // WAL file was created
        Assertions.assertTrue(walDir.resolve("orders.wal").toFile().exists());

        // miniSQL was called
        Mockito.verify(mockMiniSql).execute(sql);

        // syncToReplicas was called (no replicas returned from null zkClient, so empty list)
        Mockito.verify(mockReplicaManager).syncToReplicas(Mockito.any(), Mockito.eq(List.of()), Mockito.eq(0));
        Mockito.verify(mockReplicaManager, Mockito.never())
            .reconcileReplicasAsync(Mockito.anyString(), Mockito.anyLong(), Mockito.anyList());
    }

    @Test
    void pausedTableReturnsMOVING() throws Exception {
        writeGuard.pause("orders");
        QueryResult result = service.execute("orders", "insert into orders values(2,'x');");

        Assertions.assertEquals(StatusCode.MOVING, result.getStatus().getCode());
        // miniSQL must NOT be called
        Mockito.verify(mockMiniSql, Mockito.never()).execute(Mockito.anyString());
    }

    @Test
    void readOperationSkipsWalAndReplicaSync() throws Exception {
        service.execute("orders", "select * from orders where id=1;");

        // WAL file should NOT exist (read op)
        Assertions.assertFalse(walDir.resolve("orders.wal").toFile().exists());

        // syncToReplicas should NOT be called
        Mockito.verify(mockReplicaManager, Mockito.never())
            .syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.anyInt());

        // miniSQL IS called for reads
        Mockito.verify(mockMiniSql).execute("select * from orders where id=1;");
    }

    @Test
    void walEntryLsnIsMonotonicallyIncreasing() throws Exception {
        service.execute("t", "insert into t values(1);");
        service.execute("t", "insert into t values(2);");
        service.execute("t", "insert into t values(3);");

        List<edu.zju.supersql.rpc.WalEntry> entries =
                walManager.readEntriesAfter("t", 0L);
        Assertions.assertEquals(3, entries.size());
        Assertions.assertTrue(entries.get(0).getLsn() < entries.get(1).getLsn());
        Assertions.assertTrue(entries.get(1).getLsn() < entries.get(2).getLsn());
    }

    @Test
    void writeShouldFailWhenReplicaAcksBelowConfiguredMinimum() throws Exception {
        CuratorFramework zkClient = mockAssignmentZk("orders", "127.0.0.1", 9091);
        RegionServiceImpl strictService = new RegionServiceImpl(
                mockMiniSql, walManager, mockReplicaManager, writeGuard, zkClient, "rs-1:9090", 1);
        Mockito.when(mockReplicaManager.syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(1))).thenReturn(0);

        QueryResult result = strictService.execute("orders", "insert into orders values(1,'x');");

        Assertions.assertEquals(StatusCode.ERROR, result.getStatus().getCode());
        Assertions.assertTrue(result.getStatus().getMessage().contains("Insufficient replica ACKs"));
        Mockito.verify(mockMiniSql, Mockito.never()).execute("insert into orders values(1,'x');");
        Mockito.verify(mockReplicaManager, Mockito.never())
            .reconcileReplicasAsync(Mockito.anyString(), Mockito.anyLong(), Mockito.anyList());
        Assertions.assertTrue(walManager.readUncommittedEntries("orders").isEmpty());
    }

    @Test
    void writeShouldSucceedWhenReplicaAcksMeetConfiguredMinimum() throws Exception {
        CuratorFramework zkClient = mockAssignmentZk("orders", "127.0.0.1", 9091);
        RegionServiceImpl strictService = new RegionServiceImpl(
                mockMiniSql, walManager, mockReplicaManager, writeGuard, zkClient, "rs-1:9090", 1);
        Mockito.when(mockReplicaManager.syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(1))).thenReturn(1);

        QueryResult result = strictService.execute("orders", "insert into orders values(2,'y');");

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Mockito.verify(mockMiniSql).execute("insert into orders values(2,'y');");
        Mockito.verify(mockReplicaManager).syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(1));
        Mockito.verify(mockReplicaManager)
            .reconcileReplicasAsync(Mockito.eq("orders"), Mockito.anyLong(), Mockito.anyList());
    }

        @Test
        void writeCommitsWalAfterLocalExecution() throws Exception {
        CuratorFramework zkClient = mockAssignmentZk("orders", "127.0.0.1", 9091);
        RegionServiceImpl strictService = new RegionServiceImpl(
            mockMiniSql, walManager, mockReplicaManager, writeGuard, zkClient, "rs-1:9090", 1);
        Mockito.when(mockReplicaManager.syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(1))).thenReturn(1);

        QueryResult result = strictService.execute("orders", "insert into orders values(7,'ok');");

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        InOrder inOrder = Mockito.inOrder(mockMiniSql, mockReplicaManager);
        inOrder.verify(mockMiniSql).execute("insert into orders values(7,'ok');");
        inOrder.verify(mockReplicaManager).commitOnReplicas(Mockito.eq("orders"), Mockito.anyLong(), Mockito.anyList());
        }

        @Test
        void localEngineErrorShouldAbortWalAndAbortReplicas() throws Exception {
        CuratorFramework zkClient = mockAssignmentZk("orders", "127.0.0.1", 9091);
        RegionServiceImpl strictService = new RegionServiceImpl(
            mockMiniSql, walManager, mockReplicaManager, writeGuard, zkClient, "rs-1:9090", 1);
        Mockito.when(mockReplicaManager.syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(1))).thenReturn(1);
        Mockito.when(mockMiniSql.execute("insert into orders values(8,'bad');"))
            .thenReturn(">>> Error: duplicate key\n");

        QueryResult result = strictService.execute("orders", "insert into orders values(8,'bad');");

        Assertions.assertEquals(StatusCode.ERROR, result.getStatus().getCode());
        Mockito.verify(mockReplicaManager).abortOnReplicas(Mockito.eq("orders"), Mockito.anyLong(), Mockito.anyList());
        Mockito.verify(mockReplicaManager, Mockito.never())
            .commitOnReplicas(Mockito.eq("orders"), Mockito.anyLong(), Mockito.anyList());
        Mockito.verify(mockReplicaManager, Mockito.never())
            .reconcileReplicasAsync(Mockito.eq("orders"), Mockito.anyLong(), Mockito.anyList());
        Assertions.assertTrue(walManager.readEntriesAfter("orders", 0L).isEmpty());
        Assertions.assertTrue(walManager.readUncommittedEntries("orders").isEmpty());
        }

    @Test
    void writeShouldFailWhenReplicaTargetsBelowConfiguredMinimum() throws Exception {
        RegionServiceImpl strictService = new RegionServiceImpl(
                mockMiniSql, walManager, mockReplicaManager, writeGuard, null, "rs-1:9090", 1);

        QueryResult result = strictService.execute("orders", "insert into orders values(3,'z');");

        Assertions.assertEquals(StatusCode.ERROR, result.getStatus().getCode());
        Assertions.assertTrue(result.getStatus().getMessage().contains("Insufficient replica targets"));
        Mockito.verify(mockReplicaManager, Mockito.never())
            .syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.anyInt());
        Mockito.verify(mockMiniSql, Mockito.never()).execute("insert into orders values(3,'z');");
        Assertions.assertTrue(walManager.readUncommittedEntries("orders").isEmpty());
    }

    // ── executeBatch ─────────────────────────────────────────────────────────

    @Test
    void executeBatchRunsAllStatements() throws Exception {
        List<String> sqls = List.of(
                "insert into t values(1);",
                "insert into t values(2);",
                "insert into t values(3);"
        );
        QueryResult result = service.executeBatch("t", sqls);
        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Mockito.verify(mockMiniSql, Mockito.times(3)).execute(Mockito.anyString());
    }

    @Test
    void executeBatchStopsOnFirstError() throws Exception {
        Mockito.when(mockMiniSql.execute("insert into t values(2);"))
                .thenReturn(">>> Error: duplicate key\n");

        List<String> sqls = List.of(
                "insert into t values(1);",
                "insert into t values(2);",
                "insert into t values(3);"
        );
        QueryResult result = service.executeBatch("t", sqls);
        Assertions.assertEquals(StatusCode.ERROR, result.getStatus().getCode());
        // Third statement should NOT be executed
        Mockito.verify(mockMiniSql, Mockito.never()).execute("insert into t values(3);");
    }

    // ── createIndex / dropIndex ──────────────────────────────────────────────

    @Test
    void createIndexForwardsToEngine() throws Exception {
        service.createIndex("orders", "create index idx_id on orders(id);");
        Mockito.verify(mockMiniSql).execute("create index idx_id on orders(id);");
        Mockito.verify(mockReplicaManager).syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(0));
    }

    @Test
    void dropIndexForwardsToEngine() throws Exception {
        service.dropIndex("orders", "idx_id");
        Mockito.verify(mockMiniSql).execute("drop index idx_id;");
        Mockito.verify(mockReplicaManager).syncToReplicas(Mockito.any(), Mockito.anyList(), Mockito.eq(0));
    }

    @Test
    void createIndexShouldRespectPausedWriteGuard() throws Exception {
        writeGuard.pause("orders");

        Response response = service.createIndex("orders", "create index idx_id on orders(id);");

        Assertions.assertEquals(StatusCode.MOVING, response.getCode());
        Mockito.verify(mockMiniSql, Mockito.never()).execute("create index idx_id on orders(id);");
    }

    // ── ping ─────────────────────────────────────────────────────────────────

    @Test
    void pingReturnsOk() throws Exception {
        Response r = service.ping();
        Assertions.assertEquals(StatusCode.OK, r.getCode());
        Assertions.assertEquals("pong", r.getMessage());
    }

    private static CuratorFramework mockAssignmentZk(String tableName, String host, int port) throws Exception {
        CuratorFramework zkClient = Mockito.mock(CuratorFramework.class, Mockito.RETURNS_DEEP_STUBS);
        String path = "/assignments/" + tableName;
        byte[] payload = ("{" +
                "\"tableName\":\"" + tableName + "\"," +
                "\"replicas\":[{" +
                "\"id\":\"rs-2\"," +
                "\"host\":\"" + host + "\"," +
                "\"port\":" + port +
                "}]}" ).getBytes(StandardCharsets.UTF_8);
        Mockito.when(zkClient.checkExists().forPath(path)).thenReturn(new Stat());
        Mockito.when(zkClient.getData().forPath(path)).thenReturn(payload);
        return zkClient;
    }
}
