package edu.zju.supersql.client;

import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class SqlClientPlannedFeaturesTddTest {

    @Test
    void shouldFollowNotLeaderRedirectAutomatically() throws Exception {
        RegionServerInfo rs1 = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        RegionServerInfo rs2 = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        TableLocation loc1 = new TableLocation("orders", rs1, List.of(rs1));
        TableLocation loc2 = new TableLocation("orders", rs2, List.of(rs2));

        RouteCache cache = new RouteCache(60_000);
        cache.put("orders", loc1);

        AtomicInteger fetchCount = new AtomicInteger(0);
        SqlClient.LocationResolver resolver = (table, activeMaster, routeCache, config) -> {
            TableLocation cached = routeCache.get(table);
            if (cached != null) {
                return cached;
            }
            int round = fetchCount.incrementAndGet();
            TableLocation fresh = round == 1 ? loc2 : loc2;
            routeCache.put(table, fresh);
            return fresh;
        };

        StubRegionFactory regionFactory = new StubRegionFactory();
        regionFactory.enqueueResult("rs-1", queryResult(StatusCode.REDIRECT, "redirect"));
        regionFactory.enqueueResult("rs-2", queryResult(StatusCode.OK, "ok"));

        QueryResult result = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                retryConfig(3, 1, 1),
                resolver,
                regionFactory,
                ms -> { }
        );

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(1, fetchCount.get());
    }

    @Test
    void shouldRetryWhenTableIsMovingUntilNewRouteBecomesVisible() throws Exception {
        RegionServerInfo rs = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation loc = new TableLocation("orders", rs, List.of(rs));

        RouteCache cache = new RouteCache(60_000);
        cache.put("orders", loc);

        AtomicInteger fetchCount = new AtomicInteger(0);
        SqlClient.LocationResolver resolver = (table, activeMaster, routeCache, config) -> {
            TableLocation cached = routeCache.get(table);
            if (cached != null) {
                return cached;
            }
            fetchCount.incrementAndGet();
            routeCache.put(table, loc);
            return loc;
        };

        StubRegionFactory regionFactory = new StubRegionFactory();
        regionFactory.enqueueResult("rs-1", queryResult(StatusCode.MOVING, "moving-1"));
        regionFactory.enqueueResult("rs-1", queryResult(StatusCode.MOVING, "moving-2"));
        regionFactory.enqueueResult("rs-1", queryResult(StatusCode.OK, "ok"));

        List<Long> sleeps = new ArrayList<>();
        QueryResult result = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                retryConfig(5, 100, 50),
                resolver,
                regionFactory,
                sleeps::add
        );

        Assertions.assertEquals(StatusCode.OK, result.getStatus().getCode());
        Assertions.assertEquals(List.of(100L, 150L), sleeps);
        Assertions.assertEquals(2, fetchCount.get());
    }

    @Test
    void shouldInvalidateRouteCacheWhenMasterBroadcastsVersionChange() {
        RouteCache cache = new RouteCache(60_000);
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation location = new TableLocation("orders", primary, List.of(primary));
        location.setVersion(1L);

        cache.put("orders", location);
        cache.invalidateIfVersionMismatch("orders", 2L);

        Assertions.assertNull(cache.get("orders"));
    }

    private static ClientConfig retryConfig(int attempts, int initialBackoffMs, int stepBackoffMs) {
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

    private static QueryResult queryResult(StatusCode code, String message) {
        Response response = new Response(code);
        response.setMessage(message);
        return new QueryResult(response);
    }

    private static final class StubRegionFactory implements SqlClient.RegionClientFactory {

        private final Map<String, Deque<Object>> outcomesByRs = new HashMap<>();

        void enqueueResult(String rsId, QueryResult result) {
            outcomesByRs.computeIfAbsent(rsId, k -> new ArrayDeque<>()).addLast(result);
        }

        @Override
        public SqlClient.RegionClientSession open(RegionServerInfo target, ClientConfig config) {
            return new SqlClient.RegionClientSession() {
                @Override
                public QueryResult execute(String tableName, String sql) throws Exception {
                    Deque<Object> outcomes = outcomesByRs.get(target.getId());
                    if (outcomes == null || outcomes.isEmpty()) {
                        throw new IOException("No stubbed region outcome for " + target.getId());
                    }
                    Object next = outcomes.removeFirst();
                    if (next instanceof Exception e) {
                        throw e;
                    }
                    return (QueryResult) next;
                }

                @Override
                public void close() {
                }
            };
        }
    }
}
