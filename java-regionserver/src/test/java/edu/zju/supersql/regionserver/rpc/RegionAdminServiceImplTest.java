package edu.zju.supersql.regionserver.rpc;

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
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Unit tests for RegionAdminServiceImpl.
 * ZkClient is null for all tests (no ZK cluster needed).
 */
class RegionAdminServiceImplTest {

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
            r.setMessage("reject by test server");
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
}
