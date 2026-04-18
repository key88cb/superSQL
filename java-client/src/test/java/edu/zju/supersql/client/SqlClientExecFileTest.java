package edu.zju.supersql.client;

import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SqlClientExecFileTest {

    @Test
    void classifySqlShouldDetectExecfileCommand() {
        Assertions.assertEquals(SqlClient.SqlKind.EXECFILE,
                SqlClient.classifySql("execfile ./test_file.sql;"));
    }

    @Test
    void extractExecFilePathShouldHandleQuotesAndSemicolon() {
        Assertions.assertEquals("./test file.sql",
                SqlClient.extractExecFilePath("execfile \"./test file.sql\";"));
        Assertions.assertEquals("scripts/init.sql",
                SqlClient.extractExecFilePath("execfile scripts/init.sql"));
        Assertions.assertNull(SqlClient.extractExecFilePath("execfile"));
    }

    @Test
    void parseSqlStatementsShouldIgnoreCommentLinesAndSplitBySemicolon() {
        String script = "-- setup\n"
                + "create table t(id int);\n"
                + "# insert rows\n"
                + "insert into t values(1);\n"
                + "insert into t values(2);\n"
                + "select * from t";

        List<String> statements = SqlClient.parseSqlStatements(script);
        Assertions.assertEquals(4, statements.size());
        Assertions.assertEquals("create table t(id int);", statements.get(0));
        Assertions.assertEquals("insert into t values(1);", statements.get(1));
        Assertions.assertEquals("insert into t values(2);", statements.get(2));
        Assertions.assertEquals("select * from t", statements.get(3));
    }

    @Test
    void executeDmlBatchWithRetryShouldRetryMovingResponses() throws Exception {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("orders", primary, List.of(primary));
        RouteCache cache = new RouteCache(60_000);
        cache.put("orders", location);

        SqlClient.LocationResolver resolver = (table, activeMaster, routeCache, config) -> {
            TableLocation cached = routeCache.get(table);
            if (cached != null) {
                return cached;
            }
            routeCache.put(table, location);
            return location;
        };

        StubBatchRegionFactory factory = new StubBatchRegionFactory();
        factory.enqueueBatchResult("rs-1", queryResult(StatusCode.MOVING, "moving"));
        factory.enqueueBatchResult("rs-1", queryResult(StatusCode.OK, "ok"));

        List<Long> sleeps = new ArrayList<>();
        QueryResult result = SqlClient.executeDmlBatchWithRetry(
                "orders",
                List.of("insert into orders values(1);", "insert into orders values(2);"),
                "master:8080",
                cache,
                config(3, 50, 25),
                resolver,
                factory,
                sleeps::add);

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(List.of(50L), sleeps);
        Assertions.assertEquals(2, factory.batchCallCount("rs-1"));
    }

    private static QueryResult queryResult(StatusCode code, String message) {
        Response response = new Response(code);
        response.setMessage(message);
        return new QueryResult(response);
    }

    private static ClientConfig config(int attempts, int initialBackoffMs, int stepBackoffMs) {
        return new ClientConfig(
                "zk",
                "master:8080",
                30_000,
                3_000,
                3_000,
                attempts,
                initialBackoffMs,
                stepBackoffMs,
                ClientConfig.ReadConsistency.EVENTUAL
        );
    }

    private static final class StubBatchRegionFactory implements SqlClient.RegionClientFactory {

        private final Map<String, Deque<QueryResult>> batchOutcomesByRs = new HashMap<>();
        private final Map<String, Integer> batchCallCountByRs = new HashMap<>();

        void enqueueBatchResult(String rsId, QueryResult result) {
            batchOutcomesByRs.computeIfAbsent(rsId, key -> new ArrayDeque<>()).addLast(result);
        }

        int batchCallCount(String rsId) {
            return batchCallCountByRs.getOrDefault(rsId, 0);
        }

        @Override
        public SqlClient.RegionClientSession open(RegionServerInfo target, ClientConfig config) {
            return new SqlClient.RegionClientSession() {
                @Override
                public QueryResult execute(String tableName, String sql) {
                    throw new UnsupportedOperationException("execute should not be called in batch test");
                }

                @Override
                public QueryResult executeBatch(String tableName, List<String> sqls) {
                    int current = batchCallCountByRs.getOrDefault(target.getId(), 0);
                    batchCallCountByRs.put(target.getId(), current + 1);
                    Deque<QueryResult> outcomes = batchOutcomesByRs.get(target.getId());
                    if (outcomes == null || outcomes.isEmpty()) {
                        Response fail = new Response(StatusCode.ERROR);
                        fail.setMessage("no stubbed batch outcome");
                        return new QueryResult(fail);
                    }
                    return outcomes.removeFirst();
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
