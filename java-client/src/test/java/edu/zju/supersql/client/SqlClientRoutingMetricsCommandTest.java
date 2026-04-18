package edu.zju.supersql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class SqlClientRoutingMetricsCommandTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @TempDir
    Path tempDir;

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

    @Test
    void formatRoutingMetricsJsonShouldRenderDeterministicRows() throws Exception {
        Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot = new LinkedHashMap<>();
        snapshot.put("orders", new ClientRoutingMetrics.MetricsSnapshot(2, 3, 4, 5, 6));
        snapshot.put("accounts", new ClientRoutingMetrics.MetricsSnapshot(10, 11, 12, 13, 14));

        String json = SqlClient.formatRoutingMetricsJson(snapshot);
        Map<?, ?> root = MAPPER.readValue(json, Map.class);

        Assertions.assertEquals(2, ((Number) root.get("tableCount")).intValue());
        List<?> tables = (List<?>) root.get("tables");
        Assertions.assertEquals(2, tables.size());

        Map<?, ?> first = (Map<?, ?>) tables.get(0);
        Map<?, ?> second = (Map<?, ?>) tables.get(1);

        Assertions.assertEquals("accounts", first.get("table"));
        Assertions.assertEquals(10, ((Number) first.get("redirects")).intValue());
        Assertions.assertEquals("orders", second.get("table"));
        Assertions.assertEquals(2, ((Number) second.get("redirects")).intValue());
    }

    @Test
    void extractRoutingMetricsExportPathShouldSupportQuotedAndUnquotedPath() {
        Assertions.assertNull(SqlClient.extractRoutingMetricsExportPath("show routing metrics"));

        String unquoted = SqlClient.extractRoutingMetricsExportPath("show routing metrics export metrics.json");
        Assertions.assertEquals("metrics.json", unquoted);

        String quoted = SqlClient.extractRoutingMetricsExportPath("show routing metrics export 'logs/metrics.json'");
        Assertions.assertEquals("logs/metrics.json", quoted);
    }

    @Test
    void exportRoutingMetricsJsonShouldWriteJsonFile() throws Exception {
        Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot = new LinkedHashMap<>();
        snapshot.put("orders", new ClientRoutingMetrics.MetricsSnapshot(1, 2, 3, 4, 5));

        Path out = tempDir.resolve("metrics").resolve("routing-metrics.json");
        String writtenPath = SqlClient.exportRoutingMetricsJson(snapshot, out.toString());

        Path written = Path.of(writtenPath);
        Assertions.assertTrue(Files.exists(written));

        Map<?, ?> json = MAPPER.readValue(Files.readString(written, StandardCharsets.UTF_8), Map.class);
        Assertions.assertEquals(1, ((Number) json.get("tableCount")).intValue());
        List<?> tables = (List<?>) json.get("tables");
        Assertions.assertEquals(1, tables.size());
        Map<?, ?> row = (Map<?, ?>) tables.get(0);
        Assertions.assertEquals("orders", row.get("table"));
        Assertions.assertEquals(1, ((Number) row.get("redirects")).intValue());
    }
}
