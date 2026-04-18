package edu.zju.supersql.client;

import edu.zju.supersql.rpc.QueryResult;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.Response;
import edu.zju.supersql.rpc.StatusCode;
import edu.zju.supersql.rpc.TableLocation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class SqlClientRoutingMetricsTest {

    @BeforeEach
    void setUp() {
        SqlClient.resetRoutingMetricsForTests();
    }

    @Test
    void shouldCountRedirectAndLocationFetch() throws Exception {
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
            fetchCount.incrementAndGet();
            routeCache.put(table, loc2);
            return loc2;
        };

        StubRegionFactory regionFactory = new StubRegionFactory();
        regionFactory.enqueueResult("rs-1", result(StatusCode.REDIRECT, "redirect"));
        regionFactory.enqueueResult("rs-2", result(StatusCode.OK, "ok"));

        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                config(3, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                ms -> {
                }
        );

        Assertions.assertEquals(StatusCode.OK, qr.getStatus().getCode());
        Assertions.assertEquals(1, fetchCount.get());

        ClientRoutingMetrics.MetricsSnapshot metrics = SqlClient.snapshotRoutingMetrics().get("orders");
        Assertions.assertNotNull(metrics);
        Assertions.assertEquals(1L, metrics.redirectCount());
        Assertions.assertEquals(1L, metrics.locationFetchCount());
    }

    @Test
    void shouldCountMovingRetryAndExceptionRetry() throws Exception {
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
        regionFactory.enqueueResult("rs-1", result(StatusCode.MOVING, "moving"));
        regionFactory.enqueueException("rs-1", new IOException("transient"));
        regionFactory.enqueueResult("rs-1", result(StatusCode.OK, "ok"));

        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "insert into orders values (1);",
                "master:8080",
                cache,
                config(4, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                ms -> {
                }
        );

        Assertions.assertEquals(StatusCode.OK, qr.getStatus().getCode());

        ClientRoutingMetrics.MetricsSnapshot metrics = SqlClient.snapshotRoutingMetrics().get("orders");
        Assertions.assertNotNull(metrics);
        Assertions.assertEquals(1L, metrics.movingRetryCount());
        Assertions.assertEquals(1L, metrics.retryOnExceptionCount());
    }

    @Test
    void shouldCountReadFallbackWhenReplicaServesSelect() throws Exception {
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
        regionFactory.enqueueException("rs-1", new IOException("primary down"));
        regionFactory.enqueueResult("rs-2", result(StatusCode.OK, "ok"));

        QueryResult qr = SqlClient.executeDmlWithRetry(
                "orders",
                "select * from orders;",
                "master:8080",
                cache,
                config(2, ClientConfig.ReadConsistency.EVENTUAL),
                resolver,
                regionFactory,
                ms -> {
                }
        );

        Assertions.assertEquals(StatusCode.OK, qr.getStatus().getCode());

        ClientRoutingMetrics.MetricsSnapshot metrics = SqlClient.snapshotRoutingMetrics().get("orders");
        Assertions.assertNotNull(metrics);
        Assertions.assertEquals(1L, metrics.readFallbackCount());
    }

    private static QueryResult result(StatusCode code, String message) {
        Response response = new Response(code);
        response.setMessage(message);
        return new QueryResult(response);
    }

    private static ClientConfig config(int attempts, ClientConfig.ReadConsistency consistency) {
        return new ClientConfig(
                "zk",
                "master:8080",
                30_000,
                3_000,
                3_000,
                attempts,
                10,
                10,
                consistency
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
