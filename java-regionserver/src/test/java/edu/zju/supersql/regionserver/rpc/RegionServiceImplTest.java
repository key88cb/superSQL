package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.ReplicaManager;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalOpType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

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
        Mockito.when(mockReplicaManager.syncToReplicas(Mockito.any(), Mockito.anyList()))
                .thenReturn(1);

        walManager = new WalManager(walDir.toString());
        walManager.init();

        writeGuard = new WriteGuard();

        // ZkClient = null → no replica address lookup, empty list
        service = new RegionServiceImpl(mockMiniSql, walManager, mockReplicaManager,
                writeGuard, null, "rs-1:9090");
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
        Mockito.verify(mockReplicaManager).syncToReplicas(Mockito.any(), Mockito.eq(List.of()));
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
                .syncToReplicas(Mockito.any(), Mockito.anyList());

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
    }

    @Test
    void dropIndexForwardsToEngine() throws Exception {
        service.dropIndex("orders", "idx_id");
        Mockito.verify(mockMiniSql).execute("drop index idx_id;");
    }

    // ── ping ─────────────────────────────────────────────────────────────────

    @Test
    void pingReturnsOk() throws Exception {
        Response r = service.ping();
        Assertions.assertEquals(StatusCode.OK, r.getCode());
        Assertions.assertEquals("pong", r.getMessage());
    }
}
