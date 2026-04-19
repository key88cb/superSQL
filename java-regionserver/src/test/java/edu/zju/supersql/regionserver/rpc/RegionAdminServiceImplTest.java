package edu.zju.supersql.regionserver.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.rpc.DataChunk;
import edu.zju.supersql.rpc.RegionAdminService;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;

/**
 * Unit tests for RegionAdminServiceImpl.
 * ZkClient is null for all tests (no ZK cluster needed).
 */
class RegionAdminServiceImplTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path dataDir;

    private WriteGuard writeGuard;
    private RegionAdminServiceImpl service;

    @BeforeEach
    void setUp() {
        writeGuard = new WriteGuard();
        service = new RegionAdminServiceImpl(writeGuard, null, dataDir.toString(), "rs-test");
    }

    // ── pause / resume ─────────────────────────────────────────────────────────

    @Test
    void pauseAndResumeReturnOk() throws Exception {
        Response pause = service.pauseTableWrite("orders");
        Assertions.assertEquals(StatusCode.OK, pause.getCode());
        Assertions.assertTrue(writeGuard.isPaused("orders"));

        Response resume = service.resumeTableWrite("orders");
        Assertions.assertEquals(StatusCode.OK, resume.getCode());
        Assertions.assertFalse(writeGuard.isPaused("orders"));
    }

    @Test
    void pauseOnlyAffectsTargetTable() throws Exception {
        service.pauseTableWrite("t1");
        Assertions.assertTrue(writeGuard.isPaused("t1"));
        Assertions.assertFalse(writeGuard.isPaused("t2"));
    }

    // ── deleteLocalTable ───────────────────────────────────────────────────────

    @Test
    void deleteLocalTableRemovesMatchingFiles() throws Exception {
        // Create some files that should be deleted
        Files.writeString(dataDir.resolve("orders"), "data");
        Files.writeString(dataDir.resolve("orders_index"), "idx");
        // This file should NOT be deleted
        Files.writeString(dataDir.resolve("other_table"), "other");

        Response r = service.deleteLocalTable("orders");
        Assertions.assertEquals(StatusCode.OK, r.getCode());

        Assertions.assertFalse(dataDir.resolve("orders").toFile().exists());
        Assertions.assertFalse(dataDir.resolve("orders_index").toFile().exists());
        Assertions.assertTrue(dataDir.resolve("other_table").toFile().exists());
    }

    @Test
    void deleteLocalTableReturnOkWhenNoFiles() throws Exception {
        Response r = service.deleteLocalTable("nonexistent");
        Assertions.assertEquals(StatusCode.OK, r.getCode());
    }

    // ── invalidateClientCache ─────────────────────────────────────────────────

    @Test
    void invalidateClientCacheReturnsOk() throws Exception {
        Response r = service.invalidateClientCache("orders");
        Assertions.assertEquals(StatusCode.OK, r.getCode());
    }

    // ── copyTableData ─────────────────────────────────────────────────────────

    @Test
    void copyTableDataWritesChunkAtOffset() throws Exception {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        DataChunk chunk = new DataChunk("test_table", "test_table", 0L,
                ByteBuffer.wrap(data), true);
        Response r = service.copyTableData(chunk);

        Assertions.assertEquals(StatusCode.OK, r.getCode());

        File written = dataDir.resolve("test_table").toFile();
        Assertions.assertTrue(written.exists());
        Assertions.assertArrayEquals(data, Files.readAllBytes(written.toPath()));
    }

    @Test
    void copyTableDataAppendsAtCorrectOffset() throws Exception {
        byte[] first  = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] second = " world".getBytes(StandardCharsets.UTF_8);

        service.copyTableData(new DataChunk("file2", "file2", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertFalse(dataDir.resolve("file2").toFile().exists());
        service.copyTableData(new DataChunk("file2", "file2", 5L, ByteBuffer.wrap(second), true));

        byte[] combined = Files.readAllBytes(dataDir.resolve("file2"));
        Assertions.assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), combined);
    }

    @Test
    void copyTableDataDoesNotPublishFinalFileBeforeLastChunk() throws Exception {
        Response r = service.copyTableData(new DataChunk(
                "users", "users", 0L, ByteBuffer.wrap("part".getBytes(StandardCharsets.UTF_8)), false));

        Assertions.assertEquals(StatusCode.OK, r.getCode());
        Assertions.assertFalse(dataDir.resolve("users").toFile().exists());
        Assertions.assertTrue(dataDir.resolve("users.part").toFile().exists());
    }

    @Test
    void copyTableDataReturnsErrorOnNullChunk() throws Exception {
        Response r = service.copyTableData(new DataChunk());
        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
    }

    @Test
    void copyTableDataReturnsErrorOnNegativeOffset() throws Exception {
        DataChunk chunk = new DataChunk("orders", "orders", -1L,
                ByteBuffer.wrap("x".getBytes(StandardCharsets.UTF_8)), true);
        Response r = service.copyTableData(chunk);
        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
    }

    @Test
    void copyTableDataReturnsErrorOnUnexpectedOffset() throws Exception {
        byte[] first = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] second = " world".getBytes(StandardCharsets.UTF_8);

        Response firstResp = service.copyTableData(new DataChunk("file3", "file3", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertEquals(StatusCode.OK, firstResp.getCode());

        Response secondResp = service.copyTableData(new DataChunk("file3", "file3", 8L, ByteBuffer.wrap(second), true));
        Assertions.assertEquals(StatusCode.ERROR, secondResp.getCode());
        Assertions.assertTrue(secondResp.getMessage().contains("unexpected offset"));
        Assertions.assertFalse(dataDir.resolve("file3.part").toFile().exists());
    }

        @Test
        void copyTableDataAllowsCleanRestartAfterOffsetMismatch() throws Exception {
        Response first = service.copyTableData(new DataChunk(
            "users", "users_reset", 0L, ByteBuffer.wrap("hello".getBytes(StandardCharsets.UTF_8)), false));
        Assertions.assertEquals(StatusCode.OK, first.getCode());
        Assertions.assertTrue(dataDir.resolve("users_reset.part").toFile().exists());

        Response mismatch = service.copyTableData(new DataChunk(
            "users", "users_reset", 6L, ByteBuffer.wrap("x".getBytes(StandardCharsets.UTF_8)), true));
        Assertions.assertEquals(StatusCode.ERROR, mismatch.getCode());
        Assertions.assertFalse(dataDir.resolve("users_reset.part").toFile().exists());

        Response restart = service.copyTableData(new DataChunk(
            "users", "users_reset", 0L, ByteBuffer.wrap("ok".getBytes(StandardCharsets.UTF_8)), true));
        Assertions.assertEquals(StatusCode.OK, restart.getCode());
        Assertions.assertArrayEquals("ok".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(dataDir.resolve("users_reset")));
        }

    @Test
    void copyTableDataReturnsErrorOnUnsafeFileName() throws Exception {
        DataChunk chunk = new DataChunk("users", "../escape", 0L,
                ByteBuffer.wrap("x".getBytes(StandardCharsets.UTF_8)), true);

        Response r = service.copyTableData(chunk);
        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
        Assertions.assertTrue(r.getMessage().contains("unsafe fileName"));
    }

    @Test
    void copyTableDataAllowsRestartFromZeroAfterCompletedFile() throws Exception {
        byte[] firstPayload = "abc".getBytes(StandardCharsets.UTF_8);
        byte[] secondPayload = "xyz".getBytes(StandardCharsets.UTF_8);

        Response first = service.copyTableData(new DataChunk("file4", "file4", 0L, ByteBuffer.wrap(firstPayload), true));
        Assertions.assertEquals(StatusCode.OK, first.getCode());

        Response restart = service.copyTableData(new DataChunk("file4", "file4", 0L, ByteBuffer.wrap(secondPayload), true));
        Assertions.assertEquals(StatusCode.OK, restart.getCode());
        Assertions.assertArrayEquals(secondPayload, Files.readAllBytes(dataDir.resolve("file4")));
    }

    @Test
    void copyTableDataShouldTrackOffsetsByTableAndFileName() throws Exception {
        Response first = service.copyTableData(new DataChunk(
                "table_a", "shared_name", 0L,
                ByteBuffer.wrap("aaaa".getBytes(StandardCharsets.UTF_8)), false));
        Assertions.assertEquals(StatusCode.OK, first.getCode());

        Response second = service.copyTableData(new DataChunk(
                "table_b", "shared_name", 0L,
                ByteBuffer.wrap("bbbb".getBytes(StandardCharsets.UTF_8)), true));

        Assertions.assertEquals(StatusCode.OK, second.getCode());
        Assertions.assertTrue(Files.exists(dataDir.resolve("shared_name")));
    }

    @Test
    void copyTableDataShouldAcceptDuplicateChunkDuringInFlightTransfer() throws Exception {
        byte[] first = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] second = " world".getBytes(StandardCharsets.UTF_8);

        Response firstResp = service.copyTableData(new DataChunk("dup", "dup_file", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertEquals(StatusCode.OK, firstResp.getCode());

        Response duplicateResp = service.copyTableData(new DataChunk("dup", "dup_file", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertEquals(StatusCode.OK, duplicateResp.getCode());
        Assertions.assertTrue(duplicateResp.getMessage().contains("duplicate chunk acknowledged"));

        Response lastResp = service.copyTableData(new DataChunk("dup", "dup_file", 5L, ByteBuffer.wrap(second), true));
        Assertions.assertEquals(StatusCode.OK, lastResp.getCode());
        Assertions.assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(dataDir.resolve("dup_file")));
    }

    @Test
    void copyTableDataShouldResumeFromStagingFileAfterServiceRestart() throws Exception {
        byte[] first = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] second = " world".getBytes(StandardCharsets.UTF_8);

        Response firstResp = service.copyTableData(new DataChunk("resume", "resume_file", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertEquals(StatusCode.OK, firstResp.getCode());
        Assertions.assertTrue(Files.exists(dataDir.resolve("resume_file.part")));

        RegionAdminServiceImpl restartedService = new RegionAdminServiceImpl(
                writeGuard,
                null,
                dataDir.toString(),
                "rs-test");

        Response resumedResp = restartedService.copyTableData(new DataChunk(
                "resume", "resume_file", 5L, ByteBuffer.wrap(second), true));
        Assertions.assertEquals(StatusCode.OK, resumedResp.getCode());
        Assertions.assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8),
                Files.readAllBytes(dataDir.resolve("resume_file")));
    }

    @Test
    void copyTableDataShouldRejectConflictingDuplicateWithoutResettingProgress() throws Exception {
        byte[] first = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] conflicting = "HELLO".getBytes(StandardCharsets.UTF_8);
        byte[] second = " world".getBytes(StandardCharsets.UTF_8);

        Response firstResp = service.copyTableData(new DataChunk("dup3", "dup3_file", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertEquals(StatusCode.OK, firstResp.getCode());

        Response conflictingResp = service.copyTableData(new DataChunk("dup3", "dup3_file", 0L, ByteBuffer.wrap(conflicting), false));
        Assertions.assertEquals(StatusCode.ERROR, conflictingResp.getCode());
        Assertions.assertTrue(conflictingResp.getMessage().contains("conflicting duplicate offset"));
        Assertions.assertTrue(Files.exists(dataDir.resolve("dup3_file.part")));

        Response lastResp = service.copyTableData(new DataChunk("dup3", "dup3_file", 5L, ByteBuffer.wrap(second), true));
        Assertions.assertEquals(StatusCode.OK, lastResp.getCode());
        Assertions.assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(dataDir.resolve("dup3_file")));
    }

    @Test
    void copyTableDataShouldAcceptDuplicateLastChunkAfterPublish() throws Exception {
        byte[] first = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] second = " world".getBytes(StandardCharsets.UTF_8);

        Response firstResp = service.copyTableData(new DataChunk("dup2", "dup2_file", 0L, ByteBuffer.wrap(first), false));
        Assertions.assertEquals(StatusCode.OK, firstResp.getCode());

        Response lastResp = service.copyTableData(new DataChunk("dup2", "dup2_file", 5L, ByteBuffer.wrap(second), true));
        Assertions.assertEquals(StatusCode.OK, lastResp.getCode());

        Response duplicateLastResp = service.copyTableData(new DataChunk("dup2", "dup2_file", 5L, ByteBuffer.wrap(second), true));
        Assertions.assertEquals(StatusCode.OK, duplicateLastResp.getCode());
        Assertions.assertTrue(duplicateLastResp.getMessage().contains("duplicate chunk acknowledged"));
        Assertions.assertArrayEquals("hello world".getBytes(StandardCharsets.UTF_8), Files.readAllBytes(dataDir.resolve("dup2_file")));
    }

    @Test
    void transferTableShouldFailWhenTargetRejectsChunk() throws Exception {
        Files.writeString(dataDir.resolve("orders_data"), "payload");

        int port = freePort();
        RegionAdminService.Iface rejecting = new RejectingCopyService();
        TServer server = buildServer(port, rejecting);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(server::serve);
        Thread.sleep(200);

        try {
            Response r = service.transferTable("orders", "127.0.0.1", port);
            Assertions.assertEquals(StatusCode.ERROR, r.getCode());
            Assertions.assertTrue(r.getMessage().contains("copyTableData rejected"));
        } finally {
            server.stop();
            pool.shutdownNow();
        }
    }

    @Test
    void transferTableShouldSendFinalChunkForEmptyFile() throws Exception {
        Files.write(dataDir.resolve("orders_empty"), new byte[0]);

        int port = freePort();
        RecordingCopyService recording = new RecordingCopyService();
        TServer server = buildServer(port, recording);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(server::serve);
        Thread.sleep(200);

        try {
            Response r = service.transferTable("orders", "127.0.0.1", port);
            Assertions.assertEquals(StatusCode.OK, r.getCode());
            Assertions.assertEquals(2, recording.chunks.size());
            DataChunk chunk = recording.chunks.stream()
                    .filter(c -> c.getFileName().equals("orders_empty"))
                    .findFirst()
                    .orElseThrow();
            Assertions.assertEquals("orders_empty", chunk.getFileName());
            Assertions.assertEquals(0L, chunk.getOffset());
            Assertions.assertTrue(chunk.isIsLast());
            Assertions.assertEquals(0, chunk.bufferForData().remaining());
        } finally {
            server.stop();
            pool.shutdownNow();
        }
    }

    @Test
    void transferTableShouldRetryChunkOnTransientTargetFailure() throws Exception {
        Files.writeString(dataDir.resolve("orders_retry"), "payload");

        int port = freePort();
        FlakyCopyService flaky = new FlakyCopyService();
        TServer server = buildServer(port, flaky);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(server::serve);
        Thread.sleep(200);

        try {
            Response r = service.transferTable("orders", "127.0.0.1", port);
            Assertions.assertEquals(StatusCode.OK, r.getCode());
            Assertions.assertEquals(3, flaky.attemptCount.get());
        } finally {
            server.stop();
            pool.shutdownNow();
        }
    }

    @Test
    void transferTableShouldIgnoreStagingFiles() throws Exception {
        Files.writeString(dataDir.resolve("orders_data"), "payload");
        Files.writeString(dataDir.resolve("orders_data.part"), "staging");

        int port = freePort();
        RecordingCopyService recording = new RecordingCopyService();
        TServer server = buildServer(port, recording);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(server::serve);
        Thread.sleep(200);

        try {
            Response r = service.transferTable("orders", "127.0.0.1", port);
            Assertions.assertEquals(StatusCode.OK, r.getCode());
            Assertions.assertFalse(recording.chunks.isEmpty());
            Assertions.assertTrue(recording.chunks.stream().noneMatch(c -> c.getFileName().endsWith(".part")));
            Assertions.assertTrue(recording.chunks.stream().anyMatch(c -> c.getFileName().equals("orders_data")));
            Assertions.assertTrue(recording.chunks.stream().anyMatch(c -> c.getFileName().startsWith("__supersql_transfer_manifest__.")));
        } finally {
            server.stop();
            pool.shutdownNow();
        }
    }

    @Test
    void transferTableShouldSendManifestAfterDataChunks() throws Exception {
        Files.writeString(dataDir.resolve("orders_data"), "payload");

        int port = freePort();
        RecordingCopyService recording = new RecordingCopyService();
        TServer server = buildServer(port, recording);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(server::serve);
        Thread.sleep(200);

        try {
            Response r = service.transferTable("orders", "127.0.0.1", port);
            Assertions.assertEquals(StatusCode.OK, r.getCode());
            Assertions.assertFalse(recording.chunks.isEmpty());

            DataChunk lastChunk = recording.chunks.get(recording.chunks.size() - 1);
            Assertions.assertTrue(lastChunk.getFileName().startsWith("__supersql_transfer_manifest__."));
            Assertions.assertTrue(lastChunk.isIsLast());

            byte[] manifestBytes = new byte[lastChunk.bufferForData().remaining()];
            lastChunk.bufferForData().get(manifestBytes);
            java.util.Map<?, ?> manifest = MAPPER.readValue(manifestBytes, java.util.Map.class);
            Assertions.assertEquals("orders", String.valueOf(manifest.get("tableName")));
            List<?> files = (List<?>) manifest.get("files");
            Assertions.assertEquals(1, files.size());
            java.util.Map<?, ?> item = (java.util.Map<?, ?>) files.get(0);
            Assertions.assertEquals("orders_data", String.valueOf(item.get("fileName")));
            Assertions.assertEquals(crc32("payload".getBytes(StandardCharsets.UTF_8)),
                    ((Number) item.get("crc32")).longValue());
        } finally {
            server.stop();
            pool.shutdownNow();
        }
    }

    @Test
    void copyTableDataShouldRejectTransferManifestWhenFileMissing() throws Exception {
        byte[] manifest = "{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_missing\",\"size\":1,\"crc32\":0}]}"
                .getBytes(StandardCharsets.UTF_8);
        Response r = service.copyTableData(new DataChunk(
                "orders",
                "__supersql_transfer_manifest__.orders.json",
                0L,
                ByteBuffer.wrap(manifest),
                true));

        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
        Assertions.assertTrue(r.getMessage().contains("missing"));
    }

    @Test
    void copyTableDataShouldRejectTransferManifestWithEmptyFilesList() throws Exception {
        byte[] manifest = "{\"tableName\":\"orders\",\"files\":[]}"
                .getBytes(StandardCharsets.UTF_8);
        Response r = service.copyTableData(new DataChunk(
                "orders",
                "__supersql_transfer_manifest__.orders.json",
                0L,
                ByteBuffer.wrap(manifest),
                true));

        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
        Assertions.assertTrue(r.getMessage().contains("empty files list"));
    }

    @Test
    void copyTableDataShouldRejectTransferManifestWithStagingEntry() throws Exception {
        Files.writeString(dataDir.resolve("orders_data.part"), "staging");
        byte[] manifest = "{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_data.part\",\"size\":7,\"crc32\":0}]}"
                .getBytes(StandardCharsets.UTF_8);

        Response r = service.copyTableData(new DataChunk(
                "orders",
                "__supersql_transfer_manifest__.orders.json",
                0L,
                ByteBuffer.wrap(manifest),
                true));

        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
        Assertions.assertTrue(r.getMessage().contains("invalid data fileName"));
    }

        @Test
        void copyTableDataShouldRejectTransferManifestWithCrossTableEntry() throws Exception {
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        Response dataResp = service.copyTableData(new DataChunk(
            "users",
            "users_data",
            0L,
            ByteBuffer.wrap(data),
            true));
        Assertions.assertEquals(StatusCode.OK, dataResp.getCode());

        long crc32 = crc32(data);
        byte[] manifest = ("{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"users_data\",\"size\":7,\"crc32\":"
            + crc32 + "}]}")
            .getBytes(StandardCharsets.UTF_8);

        Response r = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(manifest),
            true));

        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
        Assertions.assertTrue(r.getMessage().contains("outside table scope"));
        }

        @Test
        void copyTableDataShouldRejectTransferManifestWithDuplicateEntries() throws Exception {
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        Response dataResp = service.copyTableData(new DataChunk(
            "orders",
            "orders_data",
            0L,
            ByteBuffer.wrap(data),
            true));
        Assertions.assertEquals(StatusCode.OK, dataResp.getCode());

        long crc32 = crc32(data);
        byte[] manifest = ("{\"tableName\":\"orders\",\"files\":["
            + "{\"fileName\":\"orders_data\",\"size\":7,\"crc32\":" + crc32 + "},"
            + "{\"fileName\":\"orders_data\",\"size\":7,\"crc32\":" + crc32 + "}]}")
            .getBytes(StandardCharsets.UTF_8);

        Response r = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(manifest),
            true));

        Assertions.assertEquals(StatusCode.ERROR, r.getCode());
        Assertions.assertTrue(r.getMessage().contains("duplicate fileName"));
        }

    @Test
    void copyTableDataShouldAcceptTransferManifestWhenFilesMatch() throws Exception {
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        long crc32 = crc32(data);
        Response dataResp = service.copyTableData(new DataChunk(
                "orders",
                "orders_data",
                0L,
                ByteBuffer.wrap(data),
                true));
        Assertions.assertEquals(StatusCode.OK, dataResp.getCode());

        byte[] manifest = ("{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_data\",\"size\":7,\"crc32\":"
            + crc32 + "}]}")
                .getBytes(StandardCharsets.UTF_8);
        Response manifestResp = service.copyTableData(new DataChunk(
                "orders",
                "__supersql_transfer_manifest__.orders.json",
                0L,
                ByteBuffer.wrap(manifest),
                true));

        Assertions.assertEquals(StatusCode.OK, manifestResp.getCode());
        Assertions.assertTrue(manifestResp.getMessage().contains("verified"));
    }

        @Test
        void copyTableDataShouldAcknowledgeDuplicateTransferManifest() throws Exception {
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        long crc32 = crc32(data);
        Response dataResp = service.copyTableData(new DataChunk(
            "orders",
            "orders_data",
            0L,
            ByteBuffer.wrap(data),
            true));
        Assertions.assertEquals(StatusCode.OK, dataResp.getCode());

        byte[] manifest = ("{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_data\",\"size\":7,\"crc32\":"
            + crc32 + "}]}")
            .getBytes(StandardCharsets.UTF_8);

        Response first = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(manifest),
            true));
        Assertions.assertEquals(StatusCode.OK, first.getCode());
        Assertions.assertTrue(first.getMessage().contains("verified"));

        Response duplicate = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(manifest),
            true));
        Assertions.assertEquals(StatusCode.OK, duplicate.getCode());
        Assertions.assertTrue(duplicate.getMessage().contains("duplicate transfer manifest acknowledged"));

        Map<String, Object> snapshot = service.getTransferManifestVerificationStats();
        Assertions.assertEquals(2L, ((Number) snapshot.get("total")).longValue());
        Assertions.assertEquals(2L, ((Number) snapshot.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) snapshot.get("failure")).longValue());
        Assertions.assertEquals(1L, ((Number) snapshot.get("duplicateAcks")).longValue());
        }

        @Test
        void copyTableDataShouldRejectTransferManifestWhenChecksumMismatch() throws Exception {
        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        Response dataResp = service.copyTableData(new DataChunk(
            "orders",
            "orders_data",
            0L,
            ByteBuffer.wrap(data),
            true));
        Assertions.assertEquals(StatusCode.OK, dataResp.getCode());

        byte[] manifest = "{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_data\",\"size\":7,\"crc32\":1}]}"
            .getBytes(StandardCharsets.UTF_8);
        Response manifestResp = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(manifest),
            true));

        Assertions.assertEquals(StatusCode.ERROR, manifestResp.getCode());
        Assertions.assertTrue(manifestResp.getMessage().contains("checksum mismatch"));
        }

        @Test
        void transferManifestVerificationStatsShouldTrackSuccessAndFailure() throws Exception {
        Map<String, Object> initial = service.getTransferManifestVerificationStats();
        Assertions.assertEquals(0L, ((Number) initial.get("total")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("duplicateAcks")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("lastSuccessTs")).longValue());

        byte[] failureManifest = "{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_missing\",\"size\":1,\"crc32\":0}]}"
            .getBytes(StandardCharsets.UTF_8);
        Response failureResp = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(failureManifest),
            true));
        Assertions.assertEquals(StatusCode.ERROR, failureResp.getCode());

        byte[] data = "payload".getBytes(StandardCharsets.UTF_8);
        long crc32 = crc32(data);
        Response dataResp = service.copyTableData(new DataChunk(
            "orders",
            "orders_data",
            0L,
            ByteBuffer.wrap(data),
            true));
        Assertions.assertEquals(StatusCode.OK, dataResp.getCode());

        byte[] successManifest = ("{\"tableName\":\"orders\",\"files\":[{\"fileName\":\"orders_data\",\"size\":7,\"crc32\":"
            + crc32 + "}]}")
            .getBytes(StandardCharsets.UTF_8);
        Response successResp = service.copyTableData(new DataChunk(
            "orders",
            "__supersql_transfer_manifest__.orders.json",
            0L,
            ByteBuffer.wrap(successManifest),
            true));
        Assertions.assertEquals(StatusCode.OK, successResp.getCode());

        Map<String, Object> snapshot = service.getTransferManifestVerificationStats();
        Assertions.assertEquals(2L, ((Number) snapshot.get("total")).longValue());
        Assertions.assertEquals(1L, ((Number) snapshot.get("success")).longValue());
        Assertions.assertEquals(1L, ((Number) snapshot.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) snapshot.get("duplicateAcks")).longValue());
        Assertions.assertTrue(((Number) snapshot.get("lastSuccessTs")).longValue() > 0L);
        Assertions.assertTrue(((Number) snapshot.get("lastFailureTs")).longValue() > 0L);
        Assertions.assertTrue(String.valueOf(snapshot.get("lastFailureMessage")).contains("missing"));
        }

    @Test
    void transferTableStatsShouldTrackReasonsAndSuccess() throws Exception {
        Map<String, Object> initial = service.getTransferTableStats();
        Assertions.assertEquals(0L, ((Number) initial.get("total")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("success")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("failure")).longValue());
        Assertions.assertEquals(0L, ((Number) initial.get("lastSuccessTs")).longValue());

        Response notFound = service.transferTable("orders", "127.0.0.1", 9999);
        Assertions.assertEquals(StatusCode.TABLE_NOT_FOUND, notFound.getCode());

        Files.writeString(dataDir.resolve("orders_data"), "payload");

        int rejectPort = freePort();
        TServer rejectServer = buildServer(rejectPort, new RejectingCopyService());
        ExecutorService rejectPool = Executors.newSingleThreadExecutor();
        rejectPool.submit(rejectServer::serve);
        Thread.sleep(200);
        try {
            Response rejected = service.transferTable("orders", "127.0.0.1", rejectPort);
            Assertions.assertEquals(StatusCode.ERROR, rejected.getCode());
            Assertions.assertTrue(rejected.getMessage().contains("copyTableData rejected"));
        } finally {
            rejectServer.stop();
            rejectPool.shutdownNow();
        }

        int okPort = freePort();
        TServer okServer = buildServer(okPort, new RecordingCopyService());
        ExecutorService okPool = Executors.newSingleThreadExecutor();
        okPool.submit(okServer::serve);
        Thread.sleep(200);
        try {
            Response ok = service.transferTable("orders", "127.0.0.1", okPort);
            Assertions.assertEquals(StatusCode.OK, ok.getCode());
        } finally {
            okServer.stop();
            okPool.shutdownNow();
        }

        Map<String, Object> snapshot = service.getTransferTableStats();
        Assertions.assertEquals(3L, ((Number) snapshot.get("total")).longValue());
        Assertions.assertEquals(1L, ((Number) snapshot.get("success")).longValue());
        Assertions.assertEquals(2L, ((Number) snapshot.get("failure")).longValue());
        Assertions.assertTrue(((Number) snapshot.get("lastSuccessTs")).longValue() > 0L);

        Map<?, ?> reasons = (Map<?, ?>) snapshot.get("failureReasons");
        Assertions.assertEquals(1L, ((Number) reasons.get("table_not_found")).longValue());
        Assertions.assertEquals(1L, ((Number) reasons.get("target_reject")).longValue());
        Assertions.assertEquals(0L, ((Number) reasons.get("transport_error")).longValue());
        Assertions.assertEquals(0L, ((Number) reasons.get("source_io_error")).longValue());
        Assertions.assertEquals(0L, ((Number) reasons.get("other")).longValue());
        Assertions.assertEquals("target_reject", String.valueOf(snapshot.get("lastFailureReason")));
    }

    @Test
    void transferTableStatsShouldClassifySourceIoError() throws Exception {
        Files.writeString(dataDir.resolve("orders_data"), "payload");
        Files.createDirectories(dataDir.resolve("orders_bad"));

        int okPort = freePort();
        TServer okServer = buildServer(okPort, new RecordingCopyService());
        ExecutorService okPool = Executors.newSingleThreadExecutor();
        okPool.submit(okServer::serve);
        Thread.sleep(200);
        try {
            Response failed = service.transferTable("orders", "127.0.0.1", okPort);
            Assertions.assertEquals(StatusCode.ERROR, failed.getCode());
        } finally {
            okServer.stop();
            okPool.shutdownNow();
        }

        Map<String, Object> snapshot = service.getTransferTableStats();
        Map<?, ?> reasons = (Map<?, ?>) snapshot.get("failureReasons");
        Assertions.assertEquals(1L, ((Number) reasons.get("source_io_error")).longValue());
        Assertions.assertEquals("source_io_error", String.valueOf(snapshot.get("lastFailureReason")));
    }

    @Test
    void transferTableLastFailureMessageShouldBeSanitizedAndBounded() throws Exception {
        Files.writeString(dataDir.resolve("orders_data"), "payload");

        String longMessage = "reject by test server\n" + "x".repeat(600);
        int rejectPort = freePort();
        TServer rejectServer = buildServer(rejectPort, new RejectingCopyService(longMessage));
        ExecutorService rejectPool = Executors.newSingleThreadExecutor();
        rejectPool.submit(rejectServer::serve);
        Thread.sleep(200);
        try {
            Response rejected = service.transferTable("orders", "127.0.0.1", rejectPort);
            Assertions.assertEquals(StatusCode.ERROR, rejected.getCode());
        } finally {
            rejectServer.stop();
            rejectPool.shutdownNow();
        }

        Map<String, Object> snapshot = service.getTransferTableStats();
        String lastFailureMessage = String.valueOf(snapshot.get("lastFailureMessage"));
        Assertions.assertFalse(lastFailureMessage.contains("\n"));
        Assertions.assertTrue(lastFailureMessage.contains("copyTableData rejected"));
        Assertions.assertTrue(lastFailureMessage.length() <= 256);
    }

    @Test
    void transferTableShouldReturnTableNotFoundWhenOnlyStagingFilesExist() throws Exception {
        Files.writeString(dataDir.resolve("orders_orphan.part"), "staging");

        Response r = service.transferTable("orders", "127.0.0.1", 9999);
        Assertions.assertEquals(StatusCode.TABLE_NOT_FOUND, r.getCode());
    }

    // ── heartbeat / registerRegionServer ──────────────────────────────────────

    @Test
    void heartbeatAndRegisterReturnOk() throws Exception {
        edu.zju.supersql.rpc.RegionServerInfo info =
                new edu.zju.supersql.rpc.RegionServerInfo("rs-test", "localhost", 9090);
        Assertions.assertEquals(StatusCode.OK, service.heartbeat(info).getCode());
        Assertions.assertEquals(StatusCode.OK, service.registerRegionServer(info).getCode());
    }

    private static TServer buildServer(int port, RegionAdminService.Iface impl) throws Exception {
        TMultiplexedProcessor processor = new TMultiplexedProcessor();
        processor.registerProcessor("RegionAdminService", new RegionAdminService.Processor<>(impl));
        TServerSocket transport = new TServerSocket(port);
        return new TThreadPoolServer(new TThreadPoolServer.Args(transport)
                .processor(processor)
                .transportFactory(new TFramedTransport.Factory())
                .minWorkerThreads(1)
                .maxWorkerThreads(2));
    }

    private static int freePort() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    private static final class RejectingCopyService implements RegionAdminService.Iface {
        private final String message;

        private RejectingCopyService() {
            this("reject by test server");
        }

        private RejectingCopyService(String message) {
            this.message = message;
        }

        @Override
        public Response pauseTableWrite(String tableName) {
            return ok();
        }

        @Override
        public Response resumeTableWrite(String tableName) {
            return ok();
        }

        @Override
        public Response transferTable(String tableName, String targetHost, int targetPort) {
            return ok();
        }

        @Override
        public Response copyTableData(DataChunk chunk) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage(message);
            return r;
        }

        @Override
        public Response deleteLocalTable(String tableName) {
            return ok();
        }

        @Override
        public Response registerRegionServer(RegionServerInfo info) {
            return ok();
        }

        @Override
        public Response heartbeat(RegionServerInfo info) {
            return ok();
        }

        @Override
        public Response invalidateClientCache(String tableName) {
            return ok();
        }

        private static Response ok() {
            return new Response(StatusCode.OK);
        }
    }

    private static final class RecordingCopyService implements RegionAdminService.Iface {
        private final List<DataChunk> chunks = new CopyOnWriteArrayList<>();

        @Override
        public Response pauseTableWrite(String tableName) {
            return ok();
        }

        @Override
        public Response resumeTableWrite(String tableName) {
            return ok();
        }

        @Override
        public Response transferTable(String tableName, String targetHost, int targetPort) {
            return ok();
        }

        @Override
        public Response copyTableData(DataChunk chunk) {
            chunks.add(chunk.deepCopy());
            return ok();
        }

        @Override
        public Response deleteLocalTable(String tableName) {
            return ok();
        }

        @Override
        public Response registerRegionServer(RegionServerInfo info) {
            return ok();
        }

        @Override
        public Response heartbeat(RegionServerInfo info) {
            return ok();
        }

        @Override
        public Response invalidateClientCache(String tableName) {
            return ok();
        }

        private static Response ok() {
            return new Response(StatusCode.OK);
        }
    }

    private static final class FlakyCopyService implements RegionAdminService.Iface {
        private final AtomicInteger attemptCount = new AtomicInteger();

        @Override
        public Response pauseTableWrite(String tableName) {
            return ok();
        }

        @Override
        public Response resumeTableWrite(String tableName) {
            return ok();
        }

        @Override
        public Response transferTable(String tableName, String targetHost, int targetPort) {
            return ok();
        }

        @Override
        public Response copyTableData(DataChunk chunk) {
            int current = attemptCount.incrementAndGet();
            if (current == 1) {
                Response r = new Response(StatusCode.ERROR);
                r.setMessage("transient failure");
                return r;
            }
            return ok();
        }

        @Override
        public Response deleteLocalTable(String tableName) {
            return ok();
        }

        @Override
        public Response registerRegionServer(RegionServerInfo info) {
            return ok();
        }

        @Override
        public Response heartbeat(RegionServerInfo info) {
            return ok();
        }

        @Override
        public Response invalidateClientCache(String tableName) {
            return ok();
        }

        private static Response ok() {
            return new Response(StatusCode.OK);
        }
    }

    private static long crc32(byte[] data) {
        CRC32 crc32 = new CRC32();
        crc32.update(data, 0, data.length);
        return crc32.getValue();
    }
}
