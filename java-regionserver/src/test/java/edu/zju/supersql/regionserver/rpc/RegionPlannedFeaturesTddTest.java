package edu.zju.supersql.regionserver.rpc;

import edu.zju.supersql.regionserver.MiniSqlProcess;
import edu.zju.supersql.regionserver.ReplicaManager;
import edu.zju.supersql.regionserver.WalManager;
import edu.zju.supersql.regionserver.WriteGuard;
import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

@Disabled("TDD spec for planned RegionServer features that are not implemented yet")
class RegionPlannedFeaturesTddTest {

    @Test
    void executeBatchShouldRunStatementsInOrderAndReturnOk() throws Exception {
        RecordingMiniSqlProcess miniSql = new RecordingMiniSqlProcess();
        RegionServiceImpl service = new RegionServiceImpl(
                miniSql,
                new WalManager("unused"),
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
        RegionServiceImpl service = new RegionServiceImpl(
                miniSql,
                new WalManager("unused"),
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
        RegionAdminServiceImpl service = new RegionAdminServiceImpl(
                new WriteGuard(),
                null,
                "unused-dir",
                "rs-test");

        Assertions.assertEquals(StatusCode.OK, service.transferTable("users", "127.0.0.1", 9099).getCode());
    }

    private static class RecordingMiniSqlProcess extends MiniSqlProcess {

        private final java.util.List<String> executedSql = new java.util.ArrayList<>();

        RecordingMiniSqlProcess() {
            super("unused-bin", "unused-dir");
        }

        @Override
        public synchronized String execute(String sql) throws IOException {
            executedSql.add(sql);
            return ">>> SUCCESS\n>>> ";
        }
    }
}
