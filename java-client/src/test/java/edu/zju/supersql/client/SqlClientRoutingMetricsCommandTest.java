package edu.zju.supersql.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class SqlClientRoutingMetricsCommandTest {

    @Test
    void formatRoutingMetricsLinesShouldHandleEmptySnapshot() {
        List<String> lines = SqlClient.formatRoutingMetricsLines(Map.of());

        Assertions.assertEquals(1, lines.size());
        Assertions.assertEquals("(no routing metrics)", lines.get(0));
    }

    @Test
    void formatRoutingMetricsLinesShouldRenderDeterministicRows() {
        Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot = new LinkedHashMap<>();
        snapshot.put("orders", new ClientRoutingMetrics.MetricsSnapshot(2, 3, 4, 5, 6));
        snapshot.put("accounts", new ClientRoutingMetrics.MetricsSnapshot(10, 11, 12, 13, 14));

        List<String> lines = SqlClient.formatRoutingMetricsLines(snapshot);

        Assertions.assertEquals("Routing Metrics:", lines.get(0));
        Assertions.assertEquals(
                "  accounts: redirects=10 movingRetries=11 exceptionRetries=12 locationFetches=13 readFallbacks=14",
                lines.get(1));
        Assertions.assertEquals(
                "  orders: redirects=2 movingRetries=3 exceptionRetries=4 locationFetches=5 readFallbacks=6",
                lines.get(2));
    }
}
