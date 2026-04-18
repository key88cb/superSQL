package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.ReplicaManager;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.rpc.DataChunk;
import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionAdminService;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

class RegionPlannedFeaturesTddTest {

    @TempDir
    Path dataDir;

    @TempDir
    Path walDir;

    @Test
    void executeBatchShouldRunStatementsInOrderAndReturnOk() throws Exception {
        RecordingMiniSqlProcess miniSql = new RecordingMiniSqlProcess();
        WalManager walManager = new WalManager(walDir.toString());
        walManager.init();
        RegionServiceImpl service = new RegionServiceImpl(
                miniSql,
            walManager,
                new ReplicaManager(),
                new WriteGuard(),
                null,
                "rs-1:9090");

        QueryResult result = service.executeBatch("users", List.of(
                "insert into users values(1);",
                "insert into users values(2);"));

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(List.of(
                "insert into users values(1);",
                "insert into users values(2);"), miniSql.executedSql);
    }

    @Test
    void createAndDropIndexShouldPropagateToMiniSql() throws Exception {
        RecordingMiniSqlProcess miniSql = new RecordingMiniSqlProcess();
        WalManager walManager = new WalManager(walDir.toString());
        walManager.init();
        RegionServiceImpl service = new RegionServiceImpl(
                miniSql,
            walManager,
                new ReplicaManager(),
                new WriteGuard(),
                null,
                "rs-1:9090");

        Response create = service.createIndex("users", "create index idx_users_id on users(id);");
        Response drop = service.dropIndex("users", "idx_users_id");

        Assertions.assertEquals(StatusCode.OK, create.getCode());
        Assertions.assertEquals(StatusCode.OK, drop.getCode());
        Assertions.assertTrue(miniSql.executedSql.stream().anyMatch(sql -> sql.contains("create index")));
        Assertions.assertTrue(miniSql.executedSql.stream().anyMatch(sql -> sql.contains("drop index")));
    }

    @Test
    void transferAndCopyTableShouldReturnOkOnceMigrationHooksExist() throws Exception {
        Files.writeString(dataDir.resolve("users_data"), "payload");

        int port = freePort();
        TServer server = buildServer(port, new AcceptingAdminService());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        pool.submit(server::serve);
        Thread.sleep(200);

        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(),
                null,
                dataDir.toString(),
                "rs-test");

        try {
            Assertions.assertEquals(StatusCode.OK, service.transferTable("users", "127.0.0.1", port).getCode());
        } finally {
            server.stop();
            pool.shutdownNow();
        }
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

    private static final class AcceptingAdminService implements RegionAdminService.Iface {
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

    private static class RecordingMiniSqlProcess extends MiniSqlProcess {

        private final java.util.List<String> executedSql = new java.util.ArrayList<>();

        RecordingMiniSqlProcess() {
            super("unused-bin", "unused-dir");
        }

        @Override
        public synchronized String execute(String sql) throws IOException {
            executedSql.add(sql);
            return ">>> SUCCESS\n";
        }
    }
}
