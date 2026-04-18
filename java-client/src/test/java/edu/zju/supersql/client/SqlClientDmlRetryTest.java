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

class SqlClientDmlRetryTest {

    @Test
    void shouldReturnMovingWhenRetriesExhausted() throws Exception {
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
        regionFactory.enqueueResult("rs-1", result(StatusCode.MOVING, "moving-1"));
        regionFactory.enqueueResult("rs-1", result(StatusCode.MOVING, "moving-2"));
        regionFactory.enqueueResult("rs-1", result(StatusCode.MOVING, "moving-3"));

        List<Long> sleeps = new ArrayList<>();
        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                config(3, 120, 30, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                sleeps::add
        );

        Assertions.assertEquals(StatusCode.MOVING, qr.getStatus().getCode());
        Assertions.assertEquals(List.of(120L, 150L), sleeps);
        Assertions.assertEquals(2, fetchCount.get());
    }

    @Test
    void shouldKeepRetryingMovingWhenAttemptsConfiguredAsZero() throws Exception {
        RegionServerInfo rs = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        TableLocation loc = new TableLocation("orders", rs, List.of(rs));

        RouteCache cache = new RouteCache(60_000);
        cache.put("orders", loc);

        SqlClient.LocationResolver resolver = (table, activeMaster, routeCache, config) -> {
            TableLocation cached = routeCache.get(table);
            if (cached != null) {
                return cached;
            }
            routeCache.put(table, loc);
            return loc;
        };

        StubRegionFactory regionFactory = new StubRegionFactory();
        regionFactory.enqueueResult("rs-1", result(StatusCode.MOVING, "moving-1"));
        regionFactory.enqueueResult("rs-1", result(StatusCode.MOVING, "moving-2"));
        regionFactory.enqueueResult("rs-1", result(StatusCode.OK, "ok"));

        List<Long> sleeps = new ArrayList<>();
        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                config(0, 50, 25, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                sleeps::add
        );

        Assertions.assertEquals(StatusCode.OK, qr.getStatus().getCode());
        Assertions.assertEquals(List.of(50L, 75L), sleeps);
    }

    @Test
    void shouldRetryWhenPrimaryTemporarilyUnavailable() throws Exception {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        RegionServerInfo replica = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        TableLocation loc = new TableLocation("orders", primary, List.of(primary, replica));

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
        regionFactory.enqueueException("rs-1", new IOException("connection refused"));
        regionFactory.enqueueResult("rs-1", result(StatusCode.OK, "ok"));

        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                config(3, 100, 50, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                ms -> {
                }
        );

        Assertions.assertEquals(StatusCode.OK, qr.getStatus().getCode());
        Assertions.assertEquals(1, fetchCount.get());
        Assertions.assertEquals(0, regionFactory.openCount("rs-2"));
    }

    @Test
    void shouldFallbackToReplicaWhenSelectPrimaryUnavailable() throws Exception {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        RegionServerInfo replica = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        TableLocation loc = new TableLocation("orders", primary, List.of(primary, replica));

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
        regionFactory.enqueueException("rs-1", new IOException("primary unavailable"));
        regionFactory.enqueueResult("rs-2", result(StatusCode.OK, "ok"));

        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "select * from orders;",
                "master:8080",
                cache,
                config(3, 100, 50, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                ms -> {
                }
        );

        Assertions.assertEquals(StatusCode.OK, qr.getStatus().getCode());
        Assertions.assertEquals(0, fetchCount.get());
        Assertions.assertEquals(1, regionFactory.openCount("rs-2"));
    }

    @Test
    void shouldNotFallbackToReplicaWhenSelectStrongConsistency() {
        RegionServerInfo primary = new RegionServerInfo("rs-1", "127.0.0.1", 9090);
        RegionServerInfo replica = new RegionServerInfo("rs-2", "127.0.0.1", 9091);
        TableLocation loc = new TableLocation("orders", primary, List.of(primary, replica));

        RouteCache cache = new RouteCache(60_000);
        cache.put("orders", loc);

        SqlClient.LocationResolver resolver = (table, activeMaster, routeCache, config) -> {
            TableLocation cached = routeCache.get(table);
            if (cached != null) {
                return cached;
            }
            routeCache.put(table, loc);
            return loc;
        };

        StubRegionFactory regionFactory = new StubRegionFactory();
        regionFactory.enqueueException("rs-1", new IOException("primary unavailable"));
        regionFactory.enqueueResult("rs-2", result(StatusCode.OK, "ok"));

        Assertions.assertThrows(IOException.class, () -> SqlClient.executeDmlWithRetry(
                "orders",
                "select * from orders;",
                "master:8080",
                cache,
                config(1, 100, 50, ClientConfig.ReadConsistency.STRONG),
                resolver,
                regionFactory,
                ms -> {
                }
        ));
        Assertions.assertEquals(0, regionFactory.openCount("rs-2"));
    }

    private static QueryResult result(StatusCode code, String msg) {
        Response response = new Response(code);
        response.setMessage(msg);
        return new QueryResult(response);
    }

    private static ClientConfig config(int attempts,
                                       int initialBackoffMs,
                                       int stepBackoffMs,
                                       ClientConfig.ReadConsistency readConsistency) {
        return new ClientConfig(
                "zk",
                "master:8080",
                30_000,
                3_000,
                3_000,
                attempts,
                initialBackoffMs,
                stepBackoffMs,
                readConsistency
        );
    }

    private static final class StubRegionFactory implements SqlClient.RegionClientFactory {

        private final Map<String, Deque<Object>> outcomesByRs = new HashMap<>();

        void enqueueResult(String rsId, QueryResult result) {
            outcomesByRs.computeIfAbsent(rsId, k -> new ArrayDeque<>()).addLast(result);
        }

        void enqueueException(String rsId, Exception exception) {
            outcomesByRs.computeIfAbsent(rsId, k -> new ArrayDeque<>()).addLast(exception);
        }

        int openCount(String rsId) {
            return openCountByRs.getOrDefault(rsId, 0);
        }

        private final Map<String, Integer> openCountByRs = new HashMap<>();

        @Override
        public SqlClient.RegionClientSession open(RegionServerInfo target, ClientConfig config) {
            openCountByRs.merge(target.getId(), 1, Integer::sum);
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
