package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.rpc.LogDecisionState;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import edu.zju.supersql.regionserver.WalManager;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

class ReplicaSyncServiceImplTest {

    private MiniSqlProcess mockMiniSql;
    private WalManager walManager;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        ReplicaSyncServiceImpl.resetForTests();
        mockMiniSql = Mockito.mock(MiniSqlProcess.class);
        Mockito.when(mockMiniSql.execute(Mockito.anyString())).thenReturn(">>> SUCCESS\n");
        walManager = new WalManager(tempDir.toString());
        walManager.init();
    }

    private ReplicaSyncServiceImpl service() {
        return new ReplicaSyncServiceImpl(mockMiniSql, walManager);
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

        long maxLsnBeforeCommit = svc.getMaxLsn("t_user");
        Assertions.assertEquals(-1L, maxLsnBeforeCommit);

        List<WalEntry> beforeCommitEntries = svc.pullLog("t_user", 1L);
        Assertions.assertTrue(beforeCommitEntries.isEmpty());

        Response commitResponse = svc.commitLog("t_user", 10L);
        Assertions.assertEquals(StatusCode.OK, commitResponse.getCode());
        Assertions.assertTrue(commitResponse.getMessage().contains("COMMITTED"));

        long maxLsnAfterCommit = svc.getMaxLsn("t_user");
        Assertions.assertEquals(10L, maxLsnAfterCommit);

        List<WalEntry> afterCommitEntries = svc.pullLog("t_user", 1L);
        Assertions.assertEquals(1, afterCommitEntries.size());
        Assertions.assertEquals(10L, afterCommitEntries.get(0).getLsn());

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
        svc.commitLog("t_order", 8L);
        svc.commitLog("t_order", 2L);
        svc.commitLog("t_order", 5L);

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

    @Test
    void commitLogShouldBeIdempotentForRepeatedRequests() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        String table = "idempotent_table";
        String sql = "insert into idempotent_table values(1);";

        WalEntry entry = new WalEntry(60L, 600L, table, WalOpType.INSERT, System.currentTimeMillis());
        entry.setAfterRow(sql.getBytes());
        svc.syncLog(entry);

        Response first = svc.commitLog(table, 60L);
        Response second = svc.commitLog(table, 60L);

        Assertions.assertEquals(StatusCode.OK, first.getCode());
        Assertions.assertEquals(StatusCode.OK, second.getCode());
        Assertions.assertTrue(second.getMessage().contains("ALREADY_COMMITTED"));
        Mockito.verify(mockMiniSql, Mockito.times(1)).execute(sql);
    }

    @Test
    void initShouldRebuildCommittedCacheFromWal() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        WalEntry entry = walEntry(70L, "t_cache", WalOpType.INSERT);
        svc.syncLog(entry);
        svc.commitLog("t_cache", 70L);

        Assertions.assertTrue(ReplicaSyncServiceImpl.COMMITTED_LSNS.containsKey("t_cache"));
        Assertions.assertTrue(ReplicaSyncServiceImpl.COMMITTED_LSNS.get("t_cache").contains(70L));

        ReplicaSyncServiceImpl.COMMITTED_LSNS.clear();
        svc.init();

        Assertions.assertTrue(ReplicaSyncServiceImpl.COMMITTED_LSNS.containsKey("t_cache"));
        Assertions.assertTrue(ReplicaSyncServiceImpl.COMMITTED_LSNS.get("t_cache").contains(70L));
    }

    @Test
    void syncShouldPersistToDiskAndInitShouldRestore() throws Exception {
        ReplicaSyncServiceImpl svc1 = service();
        String table = "persisted_test";
        
        WalEntry entry = walEntry(100L, table, WalOpType.INSERT);
        entry.setAfterRow("insert into persisted_test values(1);".getBytes());
        svc1.syncLog(entry);
        
        // Create new service instance to simulate restart
        ReplicaSyncServiceImpl svc2 = new ReplicaSyncServiceImpl(mockMiniSql, walManager);
        svc2.init(); // Recovery
        
        List<WalEntry> entriesBeforeCommit = svc2.pullLog(table, 50L);
        Assertions.assertTrue(entriesBeforeCommit.isEmpty(), "Uncommitted log should not be visible to pullLog");
        
        // Verify commit also works after restoration
        svc2.commitLog(table, 100L);
        List<WalEntry> entriesAfterCommit = svc2.pullLog(table, 50L);
        Assertions.assertEquals(1, entriesAfterCommit.size());
        Assertions.assertEquals(100L, entriesAfterCommit.get(0).getLsn());
        Mockito.verify(mockMiniSql).execute("insert into persisted_test values(1);");
    }

    @Test
    void timedOutPrepareShouldBeAutoAbortedByResolver() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        String table = "t_timeout";
        long staleTxnId = System.currentTimeMillis() - 10_000L;
        WalEntry entry = new WalEntry(200L, staleTxnId, table, WalOpType.INSERT, System.currentTimeMillis());
        entry.setAfterRow("insert into t_timeout values(1);".getBytes());

        svc.syncLog(entry);
        Assertions.assertEquals(1, walManager.readUncommittedEntries(table).size());

        svc.resolveTimedOutPrepares(System.currentTimeMillis(), 1_000L);

        Response commitResponse = svc.commitLog(table, 200L);
        Assertions.assertEquals(StatusCode.TABLE_NOT_FOUND, commitResponse.getCode());
        Assertions.assertTrue(walManager.readUncommittedEntries(table).isEmpty());

        Map<String, Object> stats = svc.getPrepareResolutionStats();
        Assertions.assertTrue(((Number) stats.get("autoAborted")).longValue() >= 1L);
        Assertions.assertEquals(table, stats.get("lastAbortTable"));
        Assertions.assertEquals(200L, ((Number) stats.get("lastAbortLsn")).longValue());
    }

    @Test
    void autoTimeoutAbortShouldAllowResyncAndEventuallyCommit() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        String table = "t_timeout_override";
        long lsn = 202L;
        String sql = "insert into t_timeout_override values(1);";
        long staleTxnId = System.currentTimeMillis() - 10_000L;

        WalEntry entry = new WalEntry(lsn, staleTxnId, table, WalOpType.INSERT, System.currentTimeMillis());
        entry.setAfterRow(sql.getBytes());

        Assertions.assertEquals(StatusCode.OK, svc.syncLog(entry).getCode());
        svc.resolveTimedOutPrepares(System.currentTimeMillis(), 1_000L);

        LogDecisionState aborted = svc.getLogDecisionState(table, lsn);
        Assertions.assertTrue(aborted.isDecided());
        Assertions.assertTrue(aborted.isSetCommitted());
        Assertions.assertFalse(aborted.isCommitted());
        Assertions.assertTrue(aborted.getDecisionId().startsWith("auto-timeout-abort-"));

        // Simulate late repair/replication replay from donor: re-sync same entry then commit.
        Response resync = svc.syncLog(entry);
        Assertions.assertEquals(StatusCode.OK, resync.getCode());

        Response commit = svc.commitLog(table, lsn);
        Assertions.assertEquals(StatusCode.OK, commit.getCode());

        LogDecisionState finalState = svc.getLogDecisionState(table, lsn);
        Assertions.assertTrue(finalState.isDecided());
        Assertions.assertTrue(finalState.isSetCommitted());
        Assertions.assertTrue(finalState.isCommitted());
        Mockito.verify(mockMiniSql).execute(sql);
    }

    @Test
    void freshPrepareShouldNotBeAutoAborted() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        String table = "t_fresh";
        long freshTxnId = System.currentTimeMillis();
        WalEntry entry = new WalEntry(201L, freshTxnId, table, WalOpType.INSERT, System.currentTimeMillis());
        String sql = "insert into t_fresh values(1);";
        entry.setAfterRow(sql.getBytes());

        svc.syncLog(entry);
        svc.resolveTimedOutPrepares(System.currentTimeMillis(), 60_000L);

        Response commitResponse = svc.commitLog(table, 201L);
        Assertions.assertEquals(StatusCode.OK, commitResponse.getCode());
        Mockito.verify(mockMiniSql).execute(sql);
    }

    @Test
    void finalizeLogDecisionShouldCommitAndExposeDecisionState() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        String table = "t_decision_commit";
        long lsn = 301L;
        String sql = "insert into t_decision_commit values(1);";

        WalEntry entry = new WalEntry(lsn, System.currentTimeMillis(), table, WalOpType.INSERT, System.currentTimeMillis());
        entry.setAfterRow(sql.getBytes());
        Assertions.assertEquals(StatusCode.OK, svc.syncLog(entry).getCode());

        Response finalize = svc.finalizeLogDecision(table, lsn, true, "decision-commit-301", System.currentTimeMillis());
        Assertions.assertEquals(StatusCode.OK, finalize.getCode());
        Assertions.assertTrue(finalize.getMessage().contains("DECIDED_COMMIT"));

        LogDecisionState state = svc.getLogDecisionState(table, lsn);
        Assertions.assertTrue(state.isDecided());
        Assertions.assertTrue(state.isSetCommitted());
        Assertions.assertTrue(state.isCommitted());
        Assertions.assertEquals("decision-commit-301", state.getDecisionId());

        Mockito.verify(mockMiniSql).execute(sql);
    }

    @Test
    void finalizeLogDecisionShouldAbortAndRejectLaterCommit() throws Exception {
        ReplicaSyncServiceImpl svc = service();
        String table = "t_decision_abort";
        long lsn = 302L;

        WalEntry entry = new WalEntry(lsn, System.currentTimeMillis(), table, WalOpType.INSERT, System.currentTimeMillis());
        entry.setAfterRow("insert into t_decision_abort values(1);".getBytes());
        Assertions.assertEquals(StatusCode.OK, svc.syncLog(entry).getCode());

        Response abort = svc.finalizeLogDecision(table, lsn, false, "decision-abort-302", System.currentTimeMillis());
        Assertions.assertEquals(StatusCode.OK, abort.getCode());
        Assertions.assertTrue(abort.getMessage().contains("DECIDED_ABORT"));

        LogDecisionState state = svc.getLogDecisionState(table, lsn);
        Assertions.assertTrue(state.isDecided());
        Assertions.assertTrue(state.isSetCommitted());
        Assertions.assertFalse(state.isCommitted());

        Response commit = svc.commitLog(table, lsn);
        Assertions.assertEquals(StatusCode.ERROR, commit.getCode());
        Assertions.assertTrue(commit.getMessage().contains("Decision conflict"));
        Mockito.verify(mockMiniSql, Mockito.never()).execute("insert into t_decision_abort values(1);");
    }

    private static WalEntry walEntry(long lsn, String table, WalOpType op) {
        WalEntry e = new WalEntry(lsn, lsn * 10, table, op, System.currentTimeMillis());
        e.setAfterRow(("select 1;" ).getBytes());
        return e;
    }
}
