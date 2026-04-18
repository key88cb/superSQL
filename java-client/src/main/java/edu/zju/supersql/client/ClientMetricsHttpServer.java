package edu.zju.supersql.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Optional HTTP endpoint for exporting client routing metrics in Prometheus text format.
 */
final class ClientMetricsHttpServer implements AutoCloseable {

    private static final String ENV_ENABLED = "CLIENT_METRICS_HTTP_ENABLED";
    private static final String ENV_HOST = "CLIENT_METRICS_HTTP_HOST";
    private static final String ENV_PORT = "CLIENT_METRICS_HTTP_PORT";

    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 9464;

    private final HttpServer server;

    private ClientMetricsHttpServer(HttpServer server) {
        this.server = server;
    }

    static ClientMetricsHttpServer startFromEnv(Map<String, String> env,
                                                Supplier<String> metricsSupplier) throws IOException {
        if (!readEnabled(env)) {
            return null;
        }

        String host = readHost(env);
        int port = readPort(env);
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        server.createContext("/metrics", new MetricsHandler(metricsSupplier));
        server.setExecutor(null);
        server.start();
        return new ClientMetricsHttpServer(server);
    }

    int getPort() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    static boolean readEnabled(Map<String, String> env) {
        String raw = env.get(ENV_ENABLED);
        if (raw == null || raw.isBlank()) {
            return false;
        }
        String normalized = raw.trim().toLowerCase(Locale.ROOT);
        return "true".equals(normalized)
                || "1".equals(normalized)
                || "yes".equals(normalized)
                || "on".equals(normalized);
    }

    static String readHost(Map<String, String> env) {
        String host = env.get(ENV_HOST);
        return (host == null || host.isBlank()) ? DEFAULT_HOST : host.trim();
    }

    static int readPort(Map<String, String> env) {
        String raw = env.get(ENV_PORT);
        if (raw == null || raw.isBlank()) {
            return DEFAULT_PORT;
        }
        try {
            int parsed = Integer.parseInt(raw.trim());
            if (parsed < 0 || parsed > 65535) {
                return DEFAULT_PORT;
            }
            return parsed;
        } catch (NumberFormatException e) {
            return DEFAULT_PORT;
        }
    }

    private static final class MetricsHandler implements HttpHandler {
        private final Supplier<String> metricsSupplier;

        private MetricsHandler(Supplier<String> metricsSupplier) {
            this.metricsSupplier = metricsSupplier;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(405, -1);
                    return;
                }

                String payload = metricsSupplier.get();
                if (payload == null) {
                    payload = "";
                }
                byte[] body = payload.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                exchange.getResponseBody().write(body);
            } finally {
                exchange.close();
            }
        }
    }
}
