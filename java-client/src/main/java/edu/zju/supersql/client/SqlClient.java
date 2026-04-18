package edu.zju.supersql.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.rpc.*;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SuperSQL interactive REPL client.
 *
 * <p>Routing rules:
 * <ul>
 *   <li>DDL (CREATE TABLE / DROP TABLE) → MasterService</li>
 *   <li>DML (SELECT / INSERT / UPDATE / DELETE) → RegionService via RouteCache</li>
 *   <li>SHOW TABLES → MasterService.listTables()</li>
 * </ul>
 */
public class SqlClient {

    private static final Logger log = LoggerFactory.getLogger(SqlClient.class);
    private static final Pattern TABLE_NAME_PATTERN =
            Pattern.compile("(?i)\\b(from|into|table|update)\\s+([a-zA-Z_][a-zA-Z0-9_]*)");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final ClientRoutingMetrics ROUTING_METRICS = new ClientRoutingMetrics();
    private static final String ROUTING_METRICS_EXPORT_PREFIX = "show routing metrics export";

    enum SqlKind { DDL, DML, SHOW_TABLES, SHOW_ROUTING_METRICS, EXECFILE, UNKNOWN }

    static SqlKind classifySql(String sql) {
        if (sql == null || sql.isBlank()) return SqlKind.UNKNOWN;
        String s = sql.trim().toLowerCase();
        if (s.startsWith("execfile")) return SqlKind.EXECFILE;
        if (s.startsWith("show routing metrics")) return SqlKind.SHOW_ROUTING_METRICS;
        if (s.startsWith("show tables")) return SqlKind.SHOW_TABLES;
        if (s.startsWith("create") || s.startsWith("drop") || s.startsWith("alter")
                || s.startsWith("truncate")) return SqlKind.DDL;
        if (s.startsWith("select") || s.startsWith("insert")
                || s.startsWith("update") || s.startsWith("delete")) return SqlKind.DML;
        return SqlKind.UNKNOWN;
    }

    static String extractTableName(String sql) {
        if (sql == null) return null;
        Matcher m = TABLE_NAME_PATTERN.matcher(sql.trim());
        return m.find() ? m.group(2) : null;
    }

    static String readActiveMaster(CuratorFramework zkClient, String fallback) {
        if (zkClient == null) return fallback;
        try {
            if (zkClient.checkExists().forPath(ZkPaths.ACTIVE_MASTER) == null) return fallback;
            byte[] bytes = zkClient.getData().forPath(ZkPaths.ACTIVE_MASTER);
            if (bytes == null || bytes.length == 0) return fallback;
            Map<?, ?> node = MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), Map.class);
            Object address = node.get("address");
            if (address != null && !String.valueOf(address).isBlank()) return String.valueOf(address);
            Object id = node.get("masterId");
            return id == null ? fallback : String.valueOf(id);
        } catch (Exception e) {
            log.warn("Failed to read /active-master: {}", e.getMessage());
            return fallback;
        }
    }

    public static void main(String[] args) throws IOException {
        ClientConfig config = ClientConfig.fromSystemEnv();
        String zkConnect = config.zkConnect();
        String masterFallback = config.masterFallback();
        log.info("SuperSQL Client starting, ZK={}", zkConnect);

        CuratorFramework zkClient = null;
        RouteInvalidationWatcher routeInvalidationWatcher = null;
        RouteCache routeCache = new RouteCache(config.cacheTtlMs());
        String activeMaster = masterFallback;

        try {
            RetryPolicy retry = new ExponentialBackoffRetry(1000, 5);
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(zkConnect)
                    .sessionTimeoutMs(30_000)
                    .connectionTimeoutMs(15_000)
                    .retryPolicy(retry)
                    .namespace("supersql")
                    .build();
            zkClient.start();
            boolean connected = zkClient.blockUntilConnected(20, java.util.concurrent.TimeUnit.SECONDS);
            if (connected) {
                log.info("ZooKeeper connected");
                activeMaster = readActiveMaster(zkClient, masterFallback);
                log.info("Discovered active master: {}", activeMaster);
                routeInvalidationWatcher = RouteInvalidationWatcher.start(zkClient, routeCache);
                log.info("Route invalidation watcher started on {}", ZkPaths.META_TABLES);
            } else {
                log.warn("ZooKeeper connection timed out — using fallback master: {}", masterFallback);
            }
        } catch (Exception e) {
            log.warn("ZooKeeper init failed: {} — running in offline mode", e.getMessage());
        }

        try {
            boolean interactive = System.console() != null || System.in.available() > 0;
            if (!interactive) {
                log.info("No interactive terminal detected — running in daemon/standby mode");
                CountDownLatch shutdownSignal = new CountDownLatch(1);
                Runtime.getRuntime().addShutdownHook(new Thread(shutdownSignal::countDown,
                        "supersql-client-shutdown"));
                try {
                    shutdownSignal.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.info("Daemon/standby mode interrupted, shutting down");
                }
                return;
            }

            System.out.println("SuperSQL Client connected. Type 'exit' to quit.");
            System.out.print("SuperSQL> ");
            System.out.flush();

            final String finalActiveMaster = activeMaster;
            try (Scanner sc = new Scanner(System.in)) {
                while (sc.hasNextLine()) {
                    String line = sc.nextLine().trim();
                    if (line.isEmpty()) { printPrompt(); continue; }
                    if ("exit".equalsIgnoreCase(line) || "quit".equalsIgnoreCase(line)) {
                        System.out.println("Bye.");
                        break;
                    }

                    try {
                        handleSql(line, finalActiveMaster, routeCache, config);
                    } catch (Exception e) {
                        System.out.println("Error: " + e.getMessage());
                        log.debug("SQL execution error", e);
                    }

                    printPrompt();
                }
            }
        } finally {
            closeQuietly(routeInvalidationWatcher, "route invalidation watcher");
            closeQuietly(zkClient, "zk client");
        }
    }

    // ─────────────────────── routing ──────────────────────────────────────────

    static void handleSql(String sql, String activeMaster, RouteCache routeCache, ClientConfig config) throws Exception {
        SqlKind kind = classifySql(sql);

        switch (kind) {
            case SHOW_TABLES -> handleShowTables(activeMaster, config);
            case SHOW_ROUTING_METRICS -> handleShowRoutingMetrics(sql);
            case EXECFILE -> handleExecFile(sql, activeMaster, routeCache, config);
            case DDL         -> handleDdl(sql, activeMaster, routeCache, config);
            case DML         -> handleDml(sql, activeMaster, routeCache, config);
            default          -> System.out.println("Unknown SQL: " + sql);
        }
    }

    private static void handleExecFile(String command,
                                       String activeMaster,
                                       RouteCache routeCache,
                                       ClientConfig config) throws Exception {
        String scriptPath = extractExecFilePath(command);
        if (scriptPath == null) {
            System.out.println("Error: invalid execfile command, expected: execfile <path>");
            return;
        }

        Path path = Paths.get(scriptPath).toAbsolutePath().normalize();
        if (!Files.exists(path) || !Files.isRegularFile(path)) {
            System.out.println("Error: script file not found: " + path);
            return;
        }

        String content = Files.readString(path, StandardCharsets.UTF_8);
        List<String> statements = parseSqlStatements(content);
        if (statements.isEmpty()) {
            System.out.println("execfile: no executable statements in " + path);
            return;
        }

        int executed = 0;
        for (int i = 0; i < statements.size(); ) {
            String statement = statements.get(i);
            SqlKind kind = classifySql(statement);
            if (kind == SqlKind.EXECFILE) {
                System.out.println("Error: nested execfile is not supported: " + statement);
                break;
            }

            String tableName = extractTableName(statement);
            if (kind == SqlKind.DML && tableName != null) {
                List<String> batch = new ArrayList<>();
                int j = i;
                while (j < statements.size()) {
                    String candidate = statements.get(j);
                    if (classifySql(candidate) != SqlKind.DML) {
                        break;
                    }
                    String candidateTable = extractTableName(candidate);
                    if (candidateTable == null || !candidateTable.equals(tableName)) {
                        break;
                    }
                    batch.add(candidate);
                    j++;
                }

                if (batch.size() > 1) {
                    QueryResult batchResult = executeDmlBatchWithRetry(
                            tableName,
                            batch,
                            activeMaster,
                            routeCache,
                            config,
                            SqlClient::resolveLocation,
                            SqlClient::openRegionSession,
                            Thread::sleep);
                    printQueryResult(batchResult);
                    executed += batch.size();
                    i = j;
                    continue;
                }
            }

            handleSql(statement, activeMaster, routeCache, config);
            executed++;
            i++;
        }

        System.out.println("execfile completed: executed " + executed + " statements from " + path);
    }

    private static void handleShowRoutingMetrics(String sql) {
        Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot = snapshotRoutingMetrics();
        String exportPath = extractRoutingMetricsExportPath(sql);
        if (exportPath != null) {
            try {
                String exported = exportRoutingMetricsJson(snapshot, exportPath);
                System.out.println("routing metrics exported: " + exported);
            } catch (IOException e) {
                System.out.println("Error: failed to export routing metrics: " + e.getMessage());
            }
            return;
        }
        if (isRoutingMetricsJsonCommand(sql)) {
            System.out.println(formatRoutingMetricsJson(snapshot));
            return;
        }
        if (isRoutingMetricsPrometheusCommand(sql)) {
            System.out.println(formatRoutingMetricsPrometheus(snapshot));
            return;
        }
        for (String line : formatRoutingMetricsLines(snapshotRoutingMetrics())) {
            System.out.println(line);
        }
    }

    private static boolean isRoutingMetricsJsonCommand(String sql) {
        return sql != null && sql.trim().toLowerCase().startsWith("show routing metrics json");
    }

    private static boolean isRoutingMetricsPrometheusCommand(String sql) {
        return sql != null && sql.trim().toLowerCase().startsWith("show routing metrics prometheus");
    }

    static String extractRoutingMetricsExportPath(String sql) {
        if (sql == null) {
            return null;
        }
        String trimmed = sql.trim();
        String lower = trimmed.toLowerCase();
        if (!lower.startsWith(ROUTING_METRICS_EXPORT_PREFIX)) {
            return null;
        }

        String raw = trimmed.substring(ROUTING_METRICS_EXPORT_PREFIX.length()).trim();
        if (raw.isEmpty()) {
            return null;
        }
        if ((raw.startsWith("\"") && raw.endsWith("\""))
                || (raw.startsWith("'") && raw.endsWith("'"))) {
            raw = raw.substring(1, raw.length() - 1).trim();
        }
        return raw.isEmpty() ? null : raw;
    }

    static String exportRoutingMetricsJson(Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot,
                                           String outputPath) throws IOException {
        Path path = Paths.get(outputPath).toAbsolutePath().normalize();
        Path parent = path.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        String json = formatRoutingMetricsJson(snapshot);
        Files.writeString(path, json, StandardCharsets.UTF_8);
        return path.toString();
    }

    private static void handleShowTables(String activeMaster, ClientConfig config) throws Exception {
        try (MasterRpcClient master = MasterRpcClient.fromAddress(activeMaster, config.masterRpcTimeoutMs())) {
            List<TableLocation> tables = master.listTables();
            if (tables == null || tables.isEmpty()) {
                System.out.println("(no tables)");
            } else {
                System.out.println("Tables:");
                for (TableLocation t : tables) {
                    System.out.printf("  %-30s  primary=%s:%d%n",
                            t.getTableName(),
                            t.getPrimaryRS().getHost(),
                            t.getPrimaryRS().getPort());
                }
            }
        }
    }

    private static void handleDdl(String sql, String activeMaster, RouteCache routeCache, ClientConfig config) throws Exception {
        String normalized = sql.trim().toLowerCase();
        Response r;
        if (normalized.startsWith("create table")) {
            r = invokeMasterResponseWithRedirect(activeMaster, config, master -> master.createTable(sql));
        } else if (normalized.startsWith("drop table")) {
            String tableName = extractTableName(sql);
            if (tableName != null) routeCache.invalidate(tableName);
            String target = tableName != null ? tableName : sql;
            r = invokeMasterResponseWithRedirect(activeMaster, config, master -> master.dropTable(target));
        } else {
            // Other DDL (CREATE INDEX, DROP INDEX) — forward to region if table name known
            String tableName = extractTableName(sql);
            if (tableName != null) {
                TableLocation loc = resolveLocation(tableName, activeMaster, routeCache, config);
                try (RegionRpcClient region = RegionRpcClient.fromInfo(loc.getPrimaryRS(), config.regionRpcTimeoutMs())) {
                    if (normalized.startsWith("drop index")) {
                        String indexName = extractIndexName(sql);
                        r = region.dropIndex(tableName, indexName != null ? indexName : sql);
                    } else {
                        r = region.createIndex(tableName, sql);
                    }
                }
            } else {
                r = invokeMasterResponseWithRedirect(activeMaster, config, master -> master.createTable(sql));
            }
        }
        printResponse(r);
    }

    private static void handleDml(String sql, String activeMaster, RouteCache routeCache, ClientConfig config)
            throws Exception {
        String tableName = extractTableName(sql);
        if (tableName == null) {
            System.out.println("Error: could not determine table name from: " + sql);
            return;
        }

        QueryResult result = executeDmlWithRetry(
                tableName,
                sql,
                activeMaster,
                routeCache,
                config,
                SqlClient::resolveLocation,
                SqlClient::openRegionSession,
                Thread::sleep);

        if (result != null
                && result.isSetStatus()
                && result.getStatus().getCode() == StatusCode.MOVING) {
            System.out.println("Table is being migrated, retries exhausted. Please retry shortly.");
            return;
        }
        printQueryResult(result);
    }

    static QueryResult executeDmlWithRetry(String tableName,
                                           String sql,
                                           String activeMaster,
                                           RouteCache routeCache,
                                           ClientConfig config,
                                           LocationResolver locationResolver,
                                           RegionClientFactory regionClientFactory,
                                           Sleeper sleeper) throws Exception {
        boolean transparentMovingWait = config.movingRetryMaxAttempts() <= 0;
        int maxAttempts = transparentMovingWait
            ? Integer.MAX_VALUE
            : Math.max(1, config.movingRetryMaxAttempts());
        String attemptsLabel = transparentMovingWait ? "unbounded" : String.valueOf(maxAttempts);
        long initialBackoffMs = Math.max(0, config.movingRetryInitialBackoffMs());
        long stepBackoffMs = Math.max(0, config.movingRetryBackoffStepMs());
        boolean readOnlyQuery = isReadOnlySql(sql);
        boolean allowReadFailover = readOnlyQuery
            && config.readConsistency() == ClientConfig.ReadConsistency.EVENTUAL;

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean cacheMissBeforeResolve = routeCache.get(tableName) == null;
            TableLocation loc = locationResolver.resolve(tableName, activeMaster, routeCache, config);
            if (loc == null || !loc.isSetPrimaryRS()) {
                throw new IOException("Table location is unavailable for " + tableName);
            }
            if (cacheMissBeforeResolve) {
                ROUTING_METRICS.recordLocationFetch(tableName);
            }

            try {
                QueryResult result = allowReadFailover
                        ? executeReadWithFailover(tableName, sql, loc, config, regionClientFactory)
                        : executeOnTarget(tableName, sql, loc.getPrimaryRS(), config, regionClientFactory);
                if (result == null || !result.isSetStatus()) {
                    return result;
                }
                StatusCode code = result.getStatus().getCode();
                if (code == StatusCode.OK) {
                    return result;
                }

                if (code == StatusCode.REDIRECT) {
                    log.info("DML REDIRECT: table={}, attempt={}/{}, invalidating cache",
                            tableName, attempt, attemptsLabel);
                    ROUTING_METRICS.recordRedirect(tableName);
                    routeCache.invalidate(tableName);
                    if (attempt < maxAttempts) {
                        continue;
                    }
                    return result;
                }

                if (code == StatusCode.MOVING) {
                    log.info("DML MOVING: table={}, attempt={}/{}, invalidating cache",
                            tableName, attempt, attemptsLabel);
                    routeCache.invalidate(tableName);
                    if (attempt >= maxAttempts) {
                        return result;
                    }
                    ROUTING_METRICS.recordMovingRetry(tableName);
                    long backoffMs = initialBackoffMs + (attempt - 1L) * stepBackoffMs;
                    if (backoffMs > 0) {
                        try {
                            sleeper.sleep(backoffMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted while waiting for MOVING retry", e);
                        }
                    }
                    continue;
                }

                return result;
            } catch (Exception e) {
                lastException = e;
                routeCache.invalidate(tableName);
                if (attempt < maxAttempts) {
                    ROUTING_METRICS.recordRetryOnException(tableName);
                }
                log.warn("DML execution failed: table={}, attempt={}/{}, reason={}",
                        tableName, attempt, attemptsLabel, e.getMessage());
                if (attempt >= maxAttempts) {
                    throw e;
                }
            }
        }

        if (lastException != null) {
            throw lastException;
        }
        throw new IOException("DML retry exhausted for table " + tableName);
    }

    static QueryResult executeDmlBatchWithRetry(String tableName,
                                                List<String> sqlBatch,
                                                String activeMaster,
                                                RouteCache routeCache,
                                                ClientConfig config,
                                                LocationResolver locationResolver,
                                                RegionClientFactory regionClientFactory,
                                                Sleeper sleeper) throws Exception {
        if (sqlBatch == null || sqlBatch.isEmpty()) {
            Response status = new Response(StatusCode.OK);
            status.setMessage("No statements");
            return new QueryResult(status);
        }

        boolean transparentMovingWait = config.movingRetryMaxAttempts() <= 0;
        int maxAttempts = transparentMovingWait
                ? Integer.MAX_VALUE
                : Math.max(1, config.movingRetryMaxAttempts());
        String attemptsLabel = transparentMovingWait ? "unbounded" : String.valueOf(maxAttempts);
        long initialBackoffMs = Math.max(0, config.movingRetryInitialBackoffMs());
        long stepBackoffMs = Math.max(0, config.movingRetryBackoffStepMs());

        Exception lastException = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            boolean cacheMissBeforeResolve = routeCache.get(tableName) == null;
            TableLocation loc = locationResolver.resolve(tableName, activeMaster, routeCache, config);
            if (loc == null || !loc.isSetPrimaryRS()) {
                throw new IOException("Table location is unavailable for " + tableName);
            }
            if (cacheMissBeforeResolve) {
                ROUTING_METRICS.recordLocationFetch(tableName);
            }

            try (RegionClientSession region = regionClientFactory.open(loc.getPrimaryRS(), config)) {
                QueryResult result = region.executeBatch(tableName, sqlBatch);
                if (result == null || !result.isSetStatus()) {
                    return result;
                }
                StatusCode code = result.getStatus().getCode();
                if (code == StatusCode.OK) {
                    return result;
                }
                if (code == StatusCode.REDIRECT) {
                    ROUTING_METRICS.recordRedirect(tableName);
                    routeCache.invalidate(tableName);
                    if (attempt < maxAttempts) {
                        continue;
                    }
                    return result;
                }
                if (code == StatusCode.MOVING) {
                    ROUTING_METRICS.recordMovingRetry(tableName);
                    routeCache.invalidate(tableName);
                    if (attempt >= maxAttempts) {
                        return result;
                    }
                    long backoffMs = initialBackoffMs + (attempt - 1L) * stepBackoffMs;
                    if (backoffMs > 0) {
                        try {
                            sleeper.sleep(backoffMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted while waiting for MOVING retry", e);
                        }
                    }
                    continue;
                }
                return result;
            } catch (Exception e) {
                lastException = e;
                routeCache.invalidate(tableName);
                if (attempt < maxAttempts) {
                    ROUTING_METRICS.recordRetryOnException(tableName);
                }
                log.warn("DML batch execution failed: table={}, batchSize={}, attempt={}/{}, reason={}",
                        tableName, sqlBatch.size(), attempt, attemptsLabel, e.getMessage());
                if (attempt >= maxAttempts) {
                    throw e;
                }
            }
        }

        if (lastException != null) {
            throw lastException;
        }
        throw new IOException("DML batch retry exhausted for table " + tableName);
    }

    private static boolean isReadOnlySql(String sql) {
        return sql != null && sql.trim().toLowerCase().startsWith("select");
    }

    private static QueryResult executeOnTarget(String tableName,
                                               String sql,
                                               RegionServerInfo target,
                                               ClientConfig config,
                                               RegionClientFactory regionClientFactory) throws Exception {
        try (RegionClientSession region = regionClientFactory.open(target, config)) {
            return region.execute(tableName, sql);
        }
    }

    private static QueryResult executeReadWithFailover(String tableName,
                                                       String sql,
                                                       TableLocation loc,
                                                       ClientConfig config,
                                                       RegionClientFactory regionClientFactory) throws Exception {
        List<RegionServerInfo> targets = buildReadTargets(loc);
        Exception firstFailure = null;
        QueryResult lastNonOk = null;

        for (RegionServerInfo target : targets) {
            try {
                QueryResult result = executeOnTarget(tableName, sql, target, config, regionClientFactory);
                if (result == null || !result.isSetStatus()) {
                    if (!target.getId().equals(loc.getPrimaryRS().getId())) {
                        ROUTING_METRICS.recordReadFallback(tableName);
                    }
                    return result;
                }
                StatusCode code = result.getStatus().getCode();
                if (code == StatusCode.OK || code == StatusCode.REDIRECT || code == StatusCode.MOVING) {
                    if (!target.getId().equals(loc.getPrimaryRS().getId())) {
                        ROUTING_METRICS.recordReadFallback(tableName);
                    }
                    return result;
                }
                lastNonOk = result;
            } catch (Exception e) {
                if (firstFailure == null) {
                    firstFailure = e;
                }
                log.warn("Read execution failed: table={}, target={}({}:{})",
                        tableName, target.getId(), target.getHost(), target.getPort());
            }
        }

        if (lastNonOk != null) {
            return lastNonOk;
        }
        if (firstFailure != null) {
            throw firstFailure;
        }
        throw new IOException("No available replica for read on table " + tableName);
    }

    private static List<RegionServerInfo> buildReadTargets(TableLocation loc) {
        LinkedHashMap<String, RegionServerInfo> targetsById = new LinkedHashMap<>();
        RegionServerInfo primary = loc.getPrimaryRS();
        if (primary != null && primary.isSetId()) {
            targetsById.put(primary.getId(), primary);
        }
        if (loc.isSetReplicas()) {
            for (RegionServerInfo replica : loc.getReplicas()) {
                if (replica != null && replica.isSetId()) {
                    targetsById.putIfAbsent(replica.getId(), replica);
                }
            }
        }
        return new ArrayList<>(targetsById.values());
    }

    private static TableLocation resolveLocation(String tableName, String activeMaster,
                                                   RouteCache routeCache, ClientConfig config) throws Exception {
        TableLocation loc = routeCache.get(tableName);
        if (loc == null) {
            loc = fetchTableLocationWithRedirect(activeMaster, tableName, config);
            if (loc != null) {
                routeCache.put(tableName, loc);
            } else {
                throw new RuntimeException("Table not found: " + tableName);
            }
        }
        return loc;
    }

    static Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshotRoutingMetrics() {
        return ROUTING_METRICS.snapshot();
    }

    static List<String> formatRoutingMetricsLines(Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot) {
        List<String> lines = new ArrayList<>();
        if (snapshot == null || snapshot.isEmpty()) {
            lines.add("(no routing metrics)");
            return lines;
        }

        lines.add("Routing Metrics:");
        List<String> tables = new ArrayList<>(snapshot.keySet());
        tables.sort(String::compareTo);
        for (String table : tables) {
            ClientRoutingMetrics.MetricsSnapshot metrics = snapshot.get(table);
            if (metrics == null) {
                continue;
            }
            lines.add(String.format(
                    "  %s: redirects=%d movingRetries=%d exceptionRetries=%d locationFetches=%d readFallbacks=%d",
                    table,
                    metrics.redirectCount(),
                    metrics.movingRetryCount(),
                    metrics.retryOnExceptionCount(),
                    metrics.locationFetchCount(),
                    metrics.readFallbackCount()));
        }
        if (lines.size() == 1) {
            lines.add("(no routing metrics)");
        }
        return lines;
    }

    static String formatRoutingMetricsJson(Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot) {
        Map<String, Object> root = new LinkedHashMap<>();
        List<Map<String, Object>> tables = new ArrayList<>();

        if (snapshot != null && !snapshot.isEmpty()) {
            List<String> names = new ArrayList<>(snapshot.keySet());
            names.sort(String::compareTo);
            for (String name : names) {
                ClientRoutingMetrics.MetricsSnapshot metrics = snapshot.get(name);
                if (metrics == null) {
                    continue;
                }
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("table", name);
                row.put("redirects", metrics.redirectCount());
                row.put("movingRetries", metrics.movingRetryCount());
                row.put("exceptionRetries", metrics.retryOnExceptionCount());
                row.put("locationFetches", metrics.locationFetchCount());
                row.put("readFallbacks", metrics.readFallbackCount());
                tables.add(row);
            }
        }

        root.put("tableCount", tables.size());
        root.put("tables", tables);
        try {
            return MAPPER.writeValueAsString(root);
        } catch (Exception e) {
            log.warn("Failed to render routing metrics json: {}", e.getMessage());
            return "{\"tableCount\":0,\"tables\":[]}";
        }
    }

    static String formatRoutingMetricsPrometheus(Map<String, ClientRoutingMetrics.MetricsSnapshot> snapshot) {
        StringBuilder sb = new StringBuilder();
        sb.append("# TYPE supersql_client_routing_redirect_total counter\n");
        sb.append("# TYPE supersql_client_routing_moving_retry_total counter\n");
        sb.append("# TYPE supersql_client_routing_exception_retry_total counter\n");
        sb.append("# TYPE supersql_client_routing_location_fetch_total counter\n");
        sb.append("# TYPE supersql_client_routing_read_fallback_total counter\n");

        if (snapshot == null || snapshot.isEmpty()) {
            sb.append("supersql_client_routing_table_count 0\n");
            return sb.toString();
        }

        List<String> names = new ArrayList<>(snapshot.keySet());
        names.sort(String::compareTo);
        int renderedTables = 0;
        for (String table : names) {
            ClientRoutingMetrics.MetricsSnapshot metrics = snapshot.get(table);
            if (metrics == null) {
                continue;
            }
            String escapedTable = table.replace("\\", "\\\\").replace("\"", "\\\"");
            sb.append(String.format("supersql_client_routing_redirect_total{table=\"%s\"} %d%n",
                    escapedTable, metrics.redirectCount()));
            sb.append(String.format("supersql_client_routing_moving_retry_total{table=\"%s\"} %d%n",
                    escapedTable, metrics.movingRetryCount()));
            sb.append(String.format("supersql_client_routing_exception_retry_total{table=\"%s\"} %d%n",
                    escapedTable, metrics.retryOnExceptionCount()));
            sb.append(String.format("supersql_client_routing_location_fetch_total{table=\"%s\"} %d%n",
                    escapedTable, metrics.locationFetchCount()));
            sb.append(String.format("supersql_client_routing_read_fallback_total{table=\"%s\"} %d%n",
                    escapedTable, metrics.readFallbackCount()));
            renderedTables++;
        }
        sb.append(String.format("supersql_client_routing_table_count %d%n", renderedTables));
        return sb.toString();
    }

    static void resetRoutingMetricsForTests() {
        ROUTING_METRICS.reset();
    }

    // ─────────────────────── output ───────────────────────────────────────────

    static void printQueryResult(QueryResult result) {
        if (result == null) { System.out.println("(null result)"); return; }

        StatusCode code = result.getStatus().getCode();
        if (code != StatusCode.OK) {
            System.out.println("Error [" + code + "]: " + result.getStatus().getMessage());
            return;
        }

        boolean hasRows = result.isSetRows() && !result.getRows().isEmpty();
        boolean hasColumns = result.isSetColumnNames() && !result.getColumnNames().isEmpty();

        if (hasColumns) {
            // Print header
            List<String> cols = result.getColumnNames();
            String header = String.join(" | ", cols);
            System.out.println(header);
            System.out.println("-".repeat(header.length()));
        }

        if (hasRows) {
            for (Row row : result.getRows()) {
                System.out.println(String.join(" | ", row.getValues()));
            }
            System.out.printf("(%d row%s)%n", result.getRows().size(),
                    result.getRows().size() == 1 ? "" : "s");
        } else if (result.isSetAffectedRows()) {
            System.out.printf("(%d row%s affected)%n", result.getAffectedRows(),
                    result.getAffectedRows() == 1 ? "" : "s");
        } else {
            System.out.println("OK");
        }
    }

    private static void printResponse(Response r) {
        if (r == null) { System.out.println("(null response)"); return; }
        if (r.getCode() == StatusCode.OK) {
            String msg = r.isSetMessage() ? r.getMessage() : "OK";
            System.out.println(msg);
        } else {
            System.out.println("Error [" + r.getCode() + "]: " + r.getMessage());
        }
    }

    private static void printPrompt() {
        System.out.print("SuperSQL> ");
        System.out.flush();
    }

    private static void closeQuietly(AutoCloseable closeable, String name) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            log.debug("Failed to close {}: {}", name, e.getMessage());
        }
    }

    // ─────────────────────── helpers ──────────────────────────────────────────

    private static final Pattern INDEX_NAME_PATTERN =
            Pattern.compile("(?i)\\bdrop\\s+index\\s+([a-zA-Z_][a-zA-Z0-9_]*)");

    private static String extractIndexName(String sql) {
        Matcher m = INDEX_NAME_PATTERN.matcher(sql.trim());
        return m.find() ? m.group(1) : null;
    }

    static String extractExecFilePath(String command) {
        if (command == null) {
            return null;
        }
        Matcher matcher = Pattern.compile("(?i)^\\s*execfile\\s+(.+?)\\s*;?\\s*$").matcher(command);
        if (!matcher.matches()) {
            return null;
        }
        String rawPath = matcher.group(1).trim();
        if ((rawPath.startsWith("\"") && rawPath.endsWith("\""))
                || (rawPath.startsWith("'") && rawPath.endsWith("'"))) {
            rawPath = rawPath.substring(1, rawPath.length() - 1).trim();
        }
        return rawPath.isBlank() ? null : rawPath;
    }

    static List<String> parseSqlStatements(String script) {
        if (script == null || script.isBlank()) {
            return List.of();
        }
        StringBuilder noComment = new StringBuilder(script.length());
        for (String line : script.split("\\R")) {
            String trimmed = line.stripLeading();
            if (trimmed.startsWith("--") || trimmed.startsWith("#")) {
                continue;
            }
            noComment.append(line).append('\n');
        }

        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        for (int i = 0; i < noComment.length(); i++) {
            char c = noComment.charAt(i);
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            }

            if (c == ';' && !inSingleQuote && !inDoubleQuote) {
                String statement = current.toString().trim();
                if (!statement.isEmpty()) {
                    statements.add(statement + ";");
                }
                current.setLength(0);
                continue;
            }
            current.append(c);
        }

        String tail = current.toString().trim();
        if (!tail.isEmpty()) {
            statements.add(tail);
        }
        return statements;
    }

    @FunctionalInterface
    interface MasterResponseCall {
        Response call(MasterRpcClient master) throws Exception;
    }

    @FunctionalInterface
    interface LocationResolver {
        TableLocation resolve(String tableName,
                              String activeMaster,
                              RouteCache routeCache,
                              ClientConfig config) throws Exception;
    }

    @FunctionalInterface
    interface RegionClientFactory {
        RegionClientSession open(RegionServerInfo target, ClientConfig config) throws Exception;
    }

    interface RegionClientSession extends AutoCloseable {
        QueryResult execute(String tableName, String sql) throws Exception;

        default QueryResult executeBatch(String tableName, List<String> sqls) throws Exception {
            QueryResult last = null;
            if (sqls == null || sqls.isEmpty()) {
                Response status = new Response(StatusCode.OK);
                status.setMessage("No statements");
                return new QueryResult(status);
            }
            for (String sql : sqls) {
                last = execute(tableName, sql);
                if (last == null || !last.isSetStatus() || last.getStatus().getCode() != StatusCode.OK) {
                    return last;
                }
            }
            return last;
        }

        @Override
        void close() throws Exception;
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long ms) throws InterruptedException;
    }

    private static RegionClientSession openRegionSession(RegionServerInfo target,
                                                         ClientConfig config) throws Exception {
        RegionRpcClient client = RegionRpcClient.fromInfo(target, config.regionRpcTimeoutMs());
        return new RegionClientSession() {
            @Override
            public QueryResult execute(String tableName, String sql) throws Exception {
                return client.execute(tableName, sql);
            }

            @Override
            public QueryResult executeBatch(String tableName, List<String> sqls) throws Exception {
                return client.executeBatch(tableName, sqls);
            }

            @Override
            public void close() {
                client.close();
            }
        };
    }

    static Response invokeMasterResponseWithRedirect(String activeMaster,
                                                     ClientConfig config,
                                                     MasterResponseCall call) throws Exception {
        String currentMaster = activeMaster;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (MasterRpcClient master = MasterRpcClient.fromAddress(currentMaster, config.masterRpcTimeoutMs())) {
                Response response = call.call(master);
                if (response != null
                        && response.getCode() == StatusCode.NOT_LEADER
                        && response.isSetRedirectTo()
                        && !response.getRedirectTo().isBlank()) {
                    currentMaster = response.getRedirectTo();
                    continue;
                }
                return response;
            }
        }
        throw new IOException("Master redirect retries exhausted for " + activeMaster);
    }

    static TableLocation fetchTableLocationWithRedirect(String activeMaster,
                                                        String tableName,
                                                        ClientConfig config) throws Exception {
        String currentMaster = activeMaster;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (MasterRpcClient master = MasterRpcClient.fromAddress(currentMaster, config.masterRpcTimeoutMs())) {
                TableLocation location = master.getTableLocation(tableName);
                if (location != null && "NOT_LEADER".equals(location.getTableStatus())) {
                    String redirect = location.isSetPrimaryRS() ? location.getPrimaryRS().getHost() : null;
                    if (redirect != null && !redirect.isBlank()) {
                        currentMaster = redirect;
                        continue;
                    }
                }
                return location;
            }
        }
        throw new IOException("Table location redirect retries exhausted for " + tableName);
    }
}
