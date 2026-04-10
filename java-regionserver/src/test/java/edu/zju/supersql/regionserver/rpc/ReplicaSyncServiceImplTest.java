package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

class ReplicaSyncServiceImplTest {

    private MiniSqlProcess mockMiniSql;

    @BeforeEach
    void setUp() throws Exception {
        ReplicaSyncServiceImpl.resetForTests();
        mockMiniSql = Mockito.mock(MiniSqlProcess.class);
        Mockito.when(mockMiniSql.execute(Mockito.anyString())).thenReturn(">>> SUCCESS\n");
    }

    private ReplicaSyncServiceImpl service() {
        return new ReplicaSyncServiceImpl(mockMiniSql);
    }

    @Test
    void syncAndPullAndCommitShouldWork() throws Exception {
        ReplicaSyncServiceImpl svc = service();

        WalEntry entry = new WalEntry(10L, 200L, "t_user", WalOpType.INSERT,
                System.currentTimeMillis());
        entry.setAfterRow("insert into t_user values(1,'a');".getBytes());
        Response syncResponse = svc.syncLog(entry);

        Assertions.assertEquals(StatusCode.OK, syncResponse.getCode());
        Assertions.assertTrue(syncResponse.getMessage().contains("ACK"));

        long maxLsn = svc.getMaxLsn("t_user");
        Assertions.assertEquals(10L, maxLsn);

        List<WalEntry> entries = svc.pullLog("t_user", 1L);
        Assertions.assertEquals(1, entries.size());
        Assertions.assertEquals(10L, entries.get(0).getLsn());

        Response commitResponse = svc.commitLog("t_user", 10L);
        Assertions.assertEquals(StatusCode.OK, commitResponse.getCode());
        Assertions.assertTrue(commitResponse.getMessage().contains("COMMITTED"));

        // Verify SQL was replayed on local engine
        Mockito.verify(mockMiniSql).execute("insert into t_user values(1,'a');");
    }

    @Test
    void commitMissingLsnShouldReturnTableNotFound() throws Exception {
        Response response = service().commitLog("missing_table", 1L);
        Assertions.assertEquals(StatusCode.TABLE_NOT_FOUND, response.getCode());
    }

    @Test
    void pullLogShouldRespectStartLsnAndOrdering() throws Exception {
        ReplicaSyncServiceImpl svc = service();

        svc.syncLog(walEntry(8L, "t_order", WalOpType.INSERT));
        svc.syncLog(walEntry(2L, "t_order", WalOpType.UPDATE));
        svc.syncLog(walEntry(5L, "t_order", WalOpType.DELETE));

        List<WalEntry> entries = svc.pullLog("t_order", 3L);
        Assertions.assertEquals(2, entries.size());
        Assertions.assertEquals(5L, entries.get(0).getLsn());
        Assertions.assertEquals(8L, entries.get(1).getLsn());
    }

    @Test
    void syncInvalidEntryShouldReturnError() throws Exception {
        Response response = service().syncLog(new WalEntry());
        Assertions.assertEquals(StatusCode.ERROR, response.getCode());
    }

    @Test
    void commitLogDoesNotCallMiniSqlWhenNoAfterRow() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        // Entry without afterRow
        WalEntry entry = new WalEntry(50L, 500L, "t_no_sql", WalOpType.DELETE,
                System.currentTimeMillis());
        svc.syncLog(entry);
        svc.commitLog("t_no_sql", 50L);
        Mockito.verify(mockMiniSql, Mockito.never()).execute(Mockito.anyString());
    }

    private static WalEntry walEntry(long lsn, String table, WalOpType op) {
        WalEntry e = new WalEntry(lsn, lsn * 10, table, op, System.currentTimeMillis());
        e.setAfterRow(("select 1;" ).getBytes());
        return e;
    }
}
