package edu.zju.supersql.regionserver;

import edu.zju.supersql.rpc.WalEntry;
import edu.zju.supersql.rpc.WalOpType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

class WalManagerTest {

    @TempDir
    Path tempDir;

    private WalManager wal;

    @BeforeEach
    void setUp() {
        wal = new WalManager(tempDir.toString());
        wal.init();
    }

    // ── binary round-trip ─────────────────────────────────────────────────────

    @Test
    void appendAndReadEntriesAfterRoundTrip() throws IOException {
        wal.appendEntry("t_orders", 1L, 100L, WalOpType.INSERT,
                "insert into t_orders values(1,'a');");
        wal.commit("t_orders", 1L);
        wal.appendEntry("t_orders", 2L, 101L, WalOpType.UPDATE,
                "update t_orders set name='b' where id=1;");
        wal.commit("t_orders", 2L);
        wal.appendEntry("t_orders", 3L, 102L, WalOpType.DELETE,
                "delete from t_orders where id=1;");
        wal.commit("t_orders", 3L);

        List<WalEntry> all = wal.readEntriesAfter("t_orders", 1L);
        Assertions.assertEquals(3, all.size());
        Assertions.assertEquals(1L, all.get(0).getLsn());
        Assertions.assertEquals(WalOpType.INSERT, all.get(0).getOpType());

        List<WalEntry> partial = wal.readEntriesAfter("t_orders", 2L);
        Assertions.assertEquals(2, partial.size());
        Assertions.assertEquals(2L, partial.get(0).getLsn());
        Assertions.assertEquals(3L, partial.get(1).getLsn());
    }

    @Test
    void sqlPayloadPreservedInAfterRow() throws IOException {
        String sql = "insert into t_user values(42,'hello world');";
        wal.appendEntry("t_user", 10L, 1L, WalOpType.INSERT, sql);
        wal.commit("t_user", 10L);

        List<WalEntry> entries = wal.readEntriesAfter("t_user", 10L);
        Assertions.assertEquals(1, entries.size());
        String recovered = new String(entries.get(0).getAfterRow(), StandardCharsets.UTF_8);
        Assertions.assertEquals(sql, recovered);
    }

    @Test
    void readEntriesAfterMissingFileReturnsEmpty() throws IOException {
        List<WalEntry> entries = wal.readEntriesAfter("nonexistent_table", 0L);
        Assertions.assertTrue(entries.isEmpty());
    }

    // ── LSN monotonicity ──────────────────────────────────────────────────────

    @Test
    void nextLsnIsStrictlyMonotonic() {
        long a = wal.nextLsn();
        long b = wal.nextLsn();
        long c = wal.nextLsn();
        Assertions.assertTrue(a < b);
        Assertions.assertTrue(b < c);
    }

    @Test
    void initRecoversMaxLsnFromExistingFiles() throws IOException {
        wal.appendEntry("t_recover", 5L, 1L, WalOpType.INSERT, "insert into t_recover values(1);");
        wal.appendEntry("t_recover", 9L, 2L, WalOpType.INSERT, "insert into t_recover values(2);");

        // Re-init should recover maxLsn = 9
        WalManager wal2 = new WalManager(tempDir.toString());
        wal2.init();
        long next = wal2.nextLsn();
        Assertions.assertTrue(next > 9, "nextLsn after recovery should be > 9, got " + next);
    }

    // ── checkpoint cleanup ────────────────────────────────────────────────────

    @Test
    void checkpointThresholdDetectedCorrectly() {
        // Threshold = 1000; first 999 increments should not trigger
        for (int i = 0; i < 999; i++) {
            Assertions.assertFalse(wal.incrementWriteCount(),
                    "Should not trigger before threshold at i=" + i);
        }
        // 1000th increment triggers
        Assertions.assertTrue(wal.incrementWriteCount());
        // After reset, counter starts fresh
        wal.resetWriteCount();
        Assertions.assertFalse(wal.incrementWriteCount());
    }

    @Test
    void scanMaxLsnOnEmptyFileReturnsZero() throws IOException {
        File empty = tempDir.resolve("empty.wal").toFile();
        empty.createNewFile();
        Assertions.assertEquals(0L, wal.scanMaxLsn(empty));
    }

    // ── per-table isolation ───────────────────────────────────────────────────

    @Test
    void entriesAreScopedPerTable() throws IOException {
        wal.appendEntry("table_a", 1L, 1L, WalOpType.INSERT, "insert into table_a values(1);");
        wal.commit("table_a", 1L);
        wal.appendEntry("table_b", 2L, 2L, WalOpType.INSERT, "insert into table_b values(2);");
        wal.commit("table_b", 2L);

        List<WalEntry> a = wal.readEntriesAfter("table_a", 0L);
        List<WalEntry> b = wal.readEntriesAfter("table_b", 0L);

        Assertions.assertEquals(1, a.size());
        Assertions.assertEquals(1L, a.get(0).getLsn());
        Assertions.assertEquals(1, b.size());
        Assertions.assertEquals(2L, b.get(0).getLsn());
    }

    @Test
    void uncommittedEntriesAreIgnoredDuringRead() throws IOException {
        wal.appendEntry("t_uncommitted", 100L, 1L, WalOpType.INSERT, "insert into t_uncommitted values(1);");
        // No commit called

        List<WalEntry> entries = wal.readEntriesAfter("t_uncommitted", 0L);
        Assertions.assertTrue(entries.isEmpty(), "Uncommitted entries should be ignored by readEntriesAfter");
    }

    @Test
    void abortedEntriesShouldNotAppearInCommittedOrUncommittedReads() throws IOException {
        wal.appendEntry("t_abort", 200L, 1L, WalOpType.INSERT, "insert into t_abort values(1);");
        wal.abort("t_abort", 200L);

        List<WalEntry> committed = wal.readEntriesAfter("t_abort", 0L);
        List<WalEntry> uncommitted = wal.readUncommittedEntries("t_abort");

        Assertions.assertTrue(committed.isEmpty(), "Aborted entries should not be replayed as committed");
        Assertions.assertTrue(uncommitted.isEmpty(), "Aborted entries should not be tracked as uncommitted");
    }

    @Test
    void recoverShouldReplayOnlyCommittedEntriesAfterCheckpoint() throws Exception {
        String committedBeforeCheckpoint = "insert into t_recover values(1);";
        String preparedSql = "insert into t_recover values(2);";
        String abortedSql = "insert into t_recover values(3);";
        String committedAfterCheckpoint = "insert into t_recover values(4);";

        wal.appendEntry("t_recover", 10L, 1L, WalOpType.INSERT, committedBeforeCheckpoint);
        wal.commit("t_recover", 10L);
        wal.appendEntry("t_recover", 11L, 2L, WalOpType.INSERT, preparedSql);
        wal.appendEntry("t_recover", 12L, 3L, WalOpType.INSERT, abortedSql);
        wal.abort("t_recover", 12L);
        wal.appendEntry("t_recover", 13L, 4L, WalOpType.INSERT, committedAfterCheckpoint);
        wal.commit("t_recover", 13L);

        MiniSqlProcess process = Mockito.mock(MiniSqlProcess.class);
        Mockito.when(process.checkpoint()).thenReturn(10L, 13L);

        wal.recover(process);

        Mockito.verify(process, Mockito.times(1)).execute(committedAfterCheckpoint);
        Mockito.verify(process, Mockito.times(2)).checkpoint();
    }

    @Test
    void unknownStatusShouldBeIgnoredByCommittedAndUncommittedReaders() throws Exception {
        wal.appendEntry("t_unknown", 300L, 1L, WalOpType.INSERT, "insert into t_unknown values(1);");
        File walFile = tempDir.resolve("t_unknown.wal").toFile();
        try (RandomAccessFile raf = new RandomAccessFile(walFile, "rw")) {
            raf.seek(17L);
            raf.writeByte(9);
        }

        List<WalEntry> committed = wal.readEntriesAfter("t_unknown", 0L);
        List<WalEntry> uncommitted = wal.readUncommittedEntries("t_unknown");

        Assertions.assertTrue(committed.isEmpty(), "Unknown status must not be replayed as committed");
        Assertions.assertTrue(uncommitted.isEmpty(), "Unknown status must not be restored as uncommitted");
    }
}
