package edu.zju.supersql.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

class ClientMetricsHttpServerTest {

    @Test
    void shouldReturnNullWhenDisabled() throws Exception {
        ClientMetricsHttpServer server = ClientMetricsHttpServer.startFromEnv(Map.of(), () -> "m 1");
        Assertions.assertNull(server);
    }

    @Test
    void shouldServePrometheusMetricsOnGet() throws Exception {
        ClientMetricsHttpServer server = ClientMetricsHttpServer.startFromEnv(
                Map.of(
                        "CLIENT_METRICS_HTTP_ENABLED", "true",
                        "CLIENT_METRICS_HTTP_HOST", "127.0.0.1",
                        "CLIENT_METRICS_HTTP_PORT", "0"
                ),
                () -> "supersql_client_routing_table_count 1\n"
        );
        Assertions.assertNotNull(server);

        try {
            URI uri = new URI("http://127.0.0.1:" + server.getPort() + "/metrics");
            HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("GET");

            int code = conn.getResponseCode();
            String body;
            try (InputStream is = conn.getInputStream()) {
                body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }

            Assertions.assertEquals(200, code);
            Assertions.assertTrue(conn.getContentType().startsWith("text/plain"));
            Assertions.assertTrue(body.contains("supersql_client_routing_table_count 1"));
        } finally {
            server.close();
        }
    }

    @Test
    void shouldRejectNonGetMethod() throws Exception {
        ClientMetricsHttpServer server = ClientMetricsHttpServer.startFromEnv(
                Map.of(
                        "CLIENT_METRICS_HTTP_ENABLED", "true",
                        "CLIENT_METRICS_HTTP_HOST", "127.0.0.1",
                        "CLIENT_METRICS_HTTP_PORT", "0"
                ),
                () -> "supersql_client_routing_table_count 1\n"
        );
        Assertions.assertNotNull(server);

        try {
            URI uri = new URI("http://127.0.0.1:" + server.getPort() + "/metrics");
            HttpURLConnection conn = (HttpURLConnection) uri.toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.getOutputStream().write("x".getBytes(StandardCharsets.UTF_8));

            Assertions.assertEquals(405, conn.getResponseCode());
        } finally {
            server.close();
        }
    }
}
