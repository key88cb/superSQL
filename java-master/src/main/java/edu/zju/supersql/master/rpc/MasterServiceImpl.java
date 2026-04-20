package edu.zju.supersql.master.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.MasterConfig;
import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.master.ZkPaths;
import edu.zju.supersql.master.balance.LoadBalancer;
import edu.zju.supersql.master.migration.RegionMigrator;
import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.rpc.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Master-side RPC implementation for table metadata, DDL orchestration and rebalance.
 */
public class MasterServiceImpl implements MasterService.Iface {

    public record RouteRepairSnapshot(long runCount,
                                      long totalRepairedTables,
                                      long lastRunAtMs,
                                      long lastRunRepairedCount,
                                      long lastRunTotalTables,
                                      long lastRunCandidateTables,
                                      String lastRunFilterRegionServerId,
                                      String lastRepairedTable,
                                      String lastError,
                                      long recentWindowSize,
                                      long recentObservedRuns,
                                      double recentSuccessRate,
                                      double recentAvgRepairedCount) {
    }

    public record ReplicaDecisionSnapshot(long observedRegionServers,
                                          long manualInterventionRegionServers,
                                          long terminalQueueRegionServers,
                                          long decisionReadyRegionServers,
                                          long decisionCandidateRegionServers,
                                          long totalTerminalQueueCount,
                                          long totalActiveDecisionReadyCount,
                                          long totalActiveDecisionCandidateCount,
                                          List<String> affectedRegionServers,
                                          String lastError) {
    }

    private record RouteRepairRun(boolean success, long repairedCount) {
    }

    private record RouteRepairWindowStats(long observedRuns,
                                          double successRate,
                                          double avgRepairedCount) {
    }

    private static final Logger log = LoggerFactory.getLogger(MasterServiceImpl.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final long DEFAULT_ROUTE_HEAL_MIN_GAP_MS = 1_000L;
    private static final int DEFAULT_ROUTE_REPAIR_WINDOW_SIZE = 10;
    private static final long DEFAULT_MIGRATION_STUCK_TIMEOUT_MS = 60_000L;
    private static final String STATUS_ACTIVE = "ACTIVE";
    private static final String STATUS_UNAVAILABLE = "UNAVAILABLE";
    private static final Pattern CREATE_TABLE_PATTERN =
            Pattern.compile("(?i)^\\s*create\\s+table\\s+([a-zA-Z_][a-zA-Z0-9_]*)");

    private final MetaManager metaManager;
    private final AssignmentManager assignmentManager;
    private final LoadBalancer loadBalancer;
    private final RegionDdlExecutor regionDdlExecutor;
    private final RegionAdminExecutor regionAdminExecutor;
    private final RegionMigrator regionMigrator;
    private final LongSupplier clockMs;
    private final long routeHealMinGapMs;
    private final long migrationStuckTimeoutMs;
    private final ConcurrentMap<String, HealPersistState> lastHealPersistByTable = new ConcurrentHashMap<>();
    private final AtomicLong routeRepairRunCount = new AtomicLong(0L);
    private final AtomicLong routeRepairTotalRepairedTables = new AtomicLong(0L);
    private final AtomicLong routeRepairLastRunAtMs = new AtomicLong(-1L);
    private final AtomicLong routeRepairLastRunRepairedCount = new AtomicLong(0L);
    private final AtomicLong routeRepairLastRunTotalTables = new AtomicLong(0L);
    private final AtomicLong routeRepairLastRunCandidateTables = new AtomicLong(0L);
    private final AtomicLong rebalanceCandidateCursor = new AtomicLong(0L);
    private final int routeRepairWindowSize;
    private final Object routeRepairWindowLock = new Object();
    private final ArrayDeque<RouteRepairRun> routeRepairRecentRuns = new ArrayDeque<>();
    private volatile String routeRepairLastRunFilterRegionServerId;
    private volatile String routeRepairLastRepairedTable;
    private volatile String routeRepairLastError;

    public MasterServiceImpl() {
        this(new MetaManager(MasterRuntimeContext.getZkClient()),
                new AssignmentManager(MasterRuntimeContext.getZkClient()),
                new LoadBalancer(),
                new RegionServiceDdlExecutor(10_000),
                new ThriftRegionAdminExecutor(10_000),
                MasterConfig.fromSystemEnv().migrationStuckTimeoutMs());
    }

    public MasterServiceImpl(MetaManager metaManager,
                             AssignmentManager assignmentManager,
                             LoadBalancer loadBalancer,
                             RegionDdlExecutor regionDdlExecutor,
                             RegionAdminExecutor regionAdminExecutor) {
        this(metaManager,
                assignmentManager,
                loadBalancer,
                regionDdlExecutor,
                regionAdminExecutor,
                DEFAULT_MIGRATION_STUCK_TIMEOUT_MS);
    }

    public MasterServiceImpl(MetaManager metaManager,
                             AssignmentManager assignmentManager,
                             LoadBalancer loadBalancer,
                             RegionDdlExecutor regionDdlExecutor,
                             RegionAdminExecutor regionAdminExecutor,
                             long migrationStuckTimeoutMs) {
        this(metaManager,
                assignmentManager,
                loadBalancer,
                regionDdlExecutor,
                regionAdminExecutor,
                System::currentTimeMillis,
                DEFAULT_ROUTE_HEAL_MIN_GAP_MS,
                DEFAULT_ROUTE_REPAIR_WINDOW_SIZE,
                migrationStuckTimeoutMs);
    }

    MasterServiceImpl(MetaManager metaManager,
                      AssignmentManager assignmentManager,
                      LoadBalancer loadBalancer,
                      RegionDdlExecutor regionDdlExecutor,
                      RegionAdminExecutor regionAdminExecutor,
                      LongSupplier clockMs,
                      long routeHealMinGapMs) {
        this(metaManager,
                assignmentManager,
                loadBalancer,
                regionDdlExecutor,
                regionAdminExecutor,
                clockMs,
                routeHealMinGapMs,
                DEFAULT_ROUTE_REPAIR_WINDOW_SIZE,
                DEFAULT_MIGRATION_STUCK_TIMEOUT_MS);
    }

    MasterServiceImpl(MetaManager metaManager,
                      AssignmentManager assignmentManager,
                      LoadBalancer loadBalancer,
                      RegionDdlExecutor regionDdlExecutor,
                      RegionAdminExecutor regionAdminExecutor,
                      LongSupplier clockMs,
                      long routeHealMinGapMs,
                      int routeRepairWindowSize) {
        this(metaManager,
                assignmentManager,
                loadBalancer,
                regionDdlExecutor,
                regionAdminExecutor,
                clockMs,
                routeHealMinGapMs,
                routeRepairWindowSize,
                DEFAULT_MIGRATION_STUCK_TIMEOUT_MS);
    }

    MasterServiceImpl(MetaManager metaManager,
                      AssignmentManager assignmentManager,
                      LoadBalancer loadBalancer,
                      RegionDdlExecutor regionDdlExecutor,
                      RegionAdminExecutor regionAdminExecutor,
                      LongSupplier clockMs,
                      long routeHealMinGapMs,
                      int routeRepairWindowSize,
                      long migrationStuckTimeoutMs) {
        this.metaManager = metaManager;
        this.assignmentManager = assignmentManager;
        this.loadBalancer = loadBalancer;
        this.regionDdlExecutor = regionDdlExecutor;
        this.regionAdminExecutor = regionAdminExecutor;
        this.clockMs = clockMs;
        this.regionMigrator = new RegionMigrator(
                metaManager,
                assignmentManager,
                regionAdminExecutor,
                clockMs,
                this::setMigrationContext,
                this::clearMigrationContext,
                this::touchStatusUpdatedAtBestEffort);
        this.routeHealMinGapMs = Math.max(0L, routeHealMinGapMs);
        this.routeRepairWindowSize = Math.max(1, routeRepairWindowSize);
        this.migrationStuckTimeoutMs = Math.max(0L, migrationStuckTimeoutMs);
    }

    private static Response notLeaderResponse(String method) {
        String redirect = MasterRuntimeContext.readActiveMasterAddress();
        log.warn("MasterService.{} rejected: NOT_LEADER, redirectTo={}", method, redirect);
        Response r = new Response(StatusCode.NOT_LEADER);
        r.setMessage("Current master is not active leader");
        if (redirect != null && !redirect.isBlank()) {
            r.setRedirectTo(redirect);
        }
        return r;
    }

    private static CuratorFramework zk() {
        return MasterRuntimeContext.getZkClient();
    }

    private static boolean isZkUnavailable(CuratorFramework client) {
        if (client == null) {
            return true;
        }
        try {
            return !client.getZookeeperClient().isConnected();
        } catch (Exception e) {
            return true;
        }
    }

    private static boolean isLeader() {
        return MasterRuntimeContext.isActiveMaster();
    }

    static String parseTableNameFromCreateDdl(String ddl) {
        if (ddl == null) {
            return null;
        }
        Matcher m = CREATE_TABLE_PATTERN.matcher(ddl.trim());
        if (!m.find()) {
            return null;
        }
        return m.group(1);
    }

    private static String tableMetaPath(String tableName) {
        return ZkPaths.tableMeta(tableName);
    }

    private static String stringifyMap(Map<String, Object> map) throws Exception {
        return MAPPER.writeValueAsString(map);
    }

    private static RegionServerInfo mapToRegionServerInfo(Map<?, ?> node) {
        String id = String.valueOf(node.containsKey("id") ? node.get("id") : "unknown");
        String host = String.valueOf(node.containsKey("host") ? node.get("host") : "127.0.0.1");
        int port = toInt(node.get("port"), 0);
        RegionServerInfo info = new RegionServerInfo(id, host, port);
        if (node.containsKey("tableCount")) {
            info.setTableCount(toInt(node.get("tableCount"), 0));
        }
        if (node.containsKey("qps1min")) {
            info.setQps1min(toDouble(node.get("qps1min"), 0.0));
        }
        if (node.containsKey("cpuUsage")) {
            info.setCpuUsage(toDouble(node.get("cpuUsage"), 0.0));
        }
        if (node.containsKey("memUsage")) {
            info.setMemUsage(toDouble(node.get("memUsage"), 0.0));
        }
        if (node.containsKey("lastHeartbeat")) {
            info.setLastHeartbeat(toLong(node.get("lastHeartbeat"), 0L));
        }
        return info;
    }

    private static int toInt(Object value, int fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static long toLong(Object value, long fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static double toDouble(Object value, double fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static boolean toBoolean(Object value, boolean fallback) {
        if (value == null) {
            return fallback;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        String text = String.valueOf(value).trim().toLowerCase();
        if ("true".equals(text)) {
            return true;
        }
        if ("false".equals(text)) {
            return false;
        }
        return fallback;
    }

    public int repairTableRoutesWithConfirmation() {
        return repairTableRoutesWithConfirmationInternal(null);
    }

    public int repairTableRoutesForRegionServerWithConfirmation(String regionServerId) {
        if (regionServerId == null || regionServerId.isBlank()) {
            return repairTableRoutesWithConfirmationInternal(null);
        }
        return repairTableRoutesWithConfirmationInternal(regionServerId.trim());
    }

    public int repairTableRoutesBestEffort() {
        return repairTableRoutesWithConfirmation();
    }

    public int repairTableRoutesForRegionServerBestEffort(String regionServerId) {
        return repairTableRoutesForRegionServerWithConfirmation(regionServerId);
    }

    private int repairTableRoutesWithConfirmationInternal(String regionServerId) {
        if (!isLeader() || isZkUnavailable(zk())) {
            return 0;
        }
        long runAt = clockMs.getAsLong();
        routeRepairRunCount.incrementAndGet();
        routeRepairLastRunAtMs.set(runAt);
        try {
            int repaired = 0;
            String lastRepairedTable = null;
            List<TableLocation> tables = metaManager.listTables();
            int totalTables = tables.size();
            int candidateTables = 0;
            for (TableLocation table : tables) {
                if (table == null) {
                    continue;
                }
                if (regionServerId != null
                        && !isTableRelatedToRegionServer(table, regionServerId)) {
                    continue;
                }
                candidateTables++;
                String before = healSignature(table);
                TableLocation recovered = regionMigrator.recoverStuckMigrationWithConfirmation(
                    table,
                    migrationStuckTimeoutMs,
                    this::readMigrationContext,
                    this::resolveRegionServerForRecovery,
                    this::resolveCrossNodeRecoveryCandidates);
                TableLocation healed = healTableLocationBestEffort(recovered);
                String after = healSignature(healed);
                if (!Objects.equals(before, after)) {
                    repaired++;
                    lastRepairedTable = table.getTableName();
                }
            }
            routeRepairTotalRepairedTables.addAndGet(repaired);
            routeRepairLastRunRepairedCount.set(repaired);
            routeRepairLastRunTotalTables.set(totalTables);
            routeRepairLastRunCandidateTables.set(candidateTables);
            routeRepairLastRunFilterRegionServerId = regionServerId;
            routeRepairLastRepairedTable = lastRepairedTable;
            routeRepairLastError = null;
            recordRouteRepairRun(true, repaired);
            if (repaired > 0) {
                log.info("repairTableRoutesWithConfirmation repaired={} candidate={} total={} filterRsId={}",
                        repaired,
                        candidateTables,
                        totalTables,
                        regionServerId == null ? "*" : regionServerId);
            }
            return repaired;
        } catch (Exception e) {
            routeRepairLastRunRepairedCount.set(0L);
            routeRepairLastRunTotalTables.set(0L);
            routeRepairLastRunCandidateTables.set(0L);
            routeRepairLastRunFilterRegionServerId = regionServerId;
            routeRepairLastRepairedTable = null;
            routeRepairLastError = e.getMessage();
            recordRouteRepairRun(false, 0L);
            log.warn("repairTableRoutesWithConfirmation failed: {}", e.getMessage());
            return 0;
        }
    }

    private static boolean isTableRelatedToRegionServer(TableLocation table, String regionServerId) {
        if (table == null || regionServerId == null || regionServerId.isBlank()) {
            return false;
        }
        if (table.isSetPrimaryRS()
                && table.getPrimaryRS() != null
                && table.getPrimaryRS().isSetId()
                && regionServerId.equals(table.getPrimaryRS().getId())) {
            return true;
        }
        if (!table.isSetReplicas() || table.getReplicas() == null) {
            return false;
        }
        for (RegionServerInfo replica : table.getReplicas()) {
            if (replica != null
                    && replica.isSetId()
                    && regionServerId.equals(replica.getId())) {
                return true;
            }
        }
        return false;
    }

    public RouteRepairSnapshot routeRepairSnapshot() {
        RouteRepairWindowStats windowStats = readRouteRepairWindowStats();
        return new RouteRepairSnapshot(
                routeRepairRunCount.get(),
                routeRepairTotalRepairedTables.get(),
                routeRepairLastRunAtMs.get(),
                routeRepairLastRunRepairedCount.get(),
                routeRepairLastRunTotalTables.get(),
                routeRepairLastRunCandidateTables.get(),
                routeRepairLastRunFilterRegionServerId,
                routeRepairLastRepairedTable,
                routeRepairLastError,
                routeRepairWindowSize,
                windowStats.observedRuns(),
                windowStats.successRate(),
                windowStats.avgRepairedCount());
    }

    public RegionMigrator.MigrationSnapshot migrationSnapshot() {
        return regionMigrator.snapshot();
    }

    public ReplicaDecisionSnapshot replicaDecisionSnapshot() {
        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            return null;
        }

        try {
            List<String> children = zk.getChildren().forPath(ZkPaths.REGION_SERVERS);
            long observedRegionServers = 0L;
            long manualInterventionRegionServers = 0L;
            long terminalQueueRegionServers = 0L;
            long decisionReadyRegionServers = 0L;
            long decisionCandidateRegionServers = 0L;
            long totalTerminalQueueCount = 0L;
            long totalActiveDecisionReadyCount = 0L;
            long totalActiveDecisionCandidateCount = 0L;
            List<String> affectedRegionServers = new ArrayList<>();

            for (String child : children) {
                String path = ZkPaths.regionServer(child);
                byte[] bytes = zk.getData().forPath(path);
                if (bytes == null || bytes.length == 0) {
                    continue;
                }
                Map<?, ?> payload = MAPPER.readValue(bytes, Map.class);
                observedRegionServers++;
                String rsId = String.valueOf(payload.containsKey("id") ? payload.get("id") : child);
                boolean manualInterventionRequired = toBoolean(payload.get("manualInterventionRequired"), false);
                long terminalQueueCount = toLong(payload.get("terminalQueueCount"), 0L);
                long activeDecisionReadyCount = toLong(payload.get("activeDecisionReadyCount"), 0L);
                long activeDecisionCandidateCount = toLong(payload.get("activeDecisionCandidateCount"), 0L);
                if (manualInterventionRequired) {
                    manualInterventionRegionServers++;
                }
                if (terminalQueueCount > 0L) {
                    terminalQueueRegionServers++;
                }
                if (activeDecisionReadyCount > 0L) {
                    decisionReadyRegionServers++;
                }
                if (activeDecisionCandidateCount > 0L) {
                    decisionCandidateRegionServers++;
                }
                totalTerminalQueueCount += Math.max(0L, terminalQueueCount);
                totalActiveDecisionReadyCount += Math.max(0L, activeDecisionReadyCount);
                totalActiveDecisionCandidateCount += Math.max(0L, activeDecisionCandidateCount);
                if (manualInterventionRequired
                        || terminalQueueCount > 0L
                        || activeDecisionReadyCount > 0L
                        || activeDecisionCandidateCount > 0L) {
                    affectedRegionServers.add(rsId);
                }
            }

            return new ReplicaDecisionSnapshot(
                    observedRegionServers,
                    manualInterventionRegionServers,
                    terminalQueueRegionServers,
                    decisionReadyRegionServers,
                    decisionCandidateRegionServers,
                    totalTerminalQueueCount,
                    totalActiveDecisionReadyCount,
                    totalActiveDecisionCandidateCount,
                    List.copyOf(affectedRegionServers),
                    null);
        } catch (Exception e) {
            log.warn("replicaDecisionSnapshot failed: {}", e.getMessage());
            return new ReplicaDecisionSnapshot(0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, List.of(), e.getMessage());
        }
    }

    private void recordRouteRepairRun(boolean success, long repairedCount) {
        synchronized (routeRepairWindowLock) {
            routeRepairRecentRuns.addLast(new RouteRepairRun(success, Math.max(0L, repairedCount)));
            while (routeRepairRecentRuns.size() > routeRepairWindowSize) {
                routeRepairRecentRuns.removeFirst();
            }
        }
    }

    private RouteRepairWindowStats readRouteRepairWindowStats() {
        synchronized (routeRepairWindowLock) {
            if (routeRepairRecentRuns.isEmpty()) {
                return new RouteRepairWindowStats(0L, 0.0, 0.0);
            }
            long observed = routeRepairRecentRuns.size();
            long successCount = 0L;
            long repairedTotal = 0L;
            for (RouteRepairRun run : routeRepairRecentRuns) {
                if (run.success()) {
                    successCount++;
                }
                repairedTotal += run.repairedCount();
            }
            double successRate = successCount / (double) observed;
            double avgRepairedCount = repairedTotal / (double) observed;
            return new RouteRepairWindowStats(observed, successRate, avgRepairedCount);
        }
    }

    @Override
    public TableLocation getTableLocation(String tableName) throws TException {
        if (!isLeader()) {
            String redirect = MasterRuntimeContext.readActiveMasterAddress();
            RegionServerInfo redirectRegion = new RegionServerInfo("redirect", redirect, 0);
            TableLocation location = new TableLocation(tableName,
                    redirectRegion,
                    Collections.singletonList(redirectRegion));
            location.setTableStatus("NOT_LEADER");
            location.setVersion(-1L);
            return location;
        }

        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            RegionServerInfo unavailable = new RegionServerInfo("unavailable", "0.0.0.0", 0);
            TableLocation location = new TableLocation(tableName, unavailable,
                Collections.singletonList(unavailable));
            location.setTableStatus("ZK_UNAVAILABLE");
            location.setVersion(-1L);
            return location;
        }

        try {
            TableLocation location = metaManager.getTableLocation(tableName);
            if (location == null) {
                RegionServerInfo none = new RegionServerInfo("none", "0.0.0.0", 0);
                TableLocation notFound = new TableLocation(tableName, none, Collections.singletonList(none));
                notFound.setTableStatus("TABLE_NOT_FOUND");
                notFound.setVersion(-1L);
                return notFound;
            }
            TableLocation recovered = regionMigrator.recoverStuckMigrationWithConfirmation(
                location,
                migrationStuckTimeoutMs,
                this::readMigrationContext,
                this::resolveRegionServerForRecovery,
                this::resolveCrossNodeRecoveryCandidates);
            return healTableLocationBestEffort(recovered);
        } catch (Exception e) {
            throw new TException("Failed to resolve table location: " + tableName, e);
        }
    }

    @Override
    public Response createTable(String ddl) throws TException {
        if (!isLeader()) {
            return notLeaderResponse("createTable");
        }

        String tableName = parseTableNameFromCreateDdl(ddl);
        if (tableName == null) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("Invalid CREATE TABLE DDL");
            return r;
        }

        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("ZooKeeper is unavailable");
            return r;
        }

        try {
            if (metaManager.tableExists(tableName)) {
                Response r = new Response(StatusCode.TABLE_EXISTS);
                r.setMessage("Table already exists: " + tableName);
                return r;
            }

            List<RegionServerInfo> rsList = listRegionServers();
            if (rsList.isEmpty()) {
                Response r = new Response(StatusCode.RS_NOT_FOUND);
                r.setMessage("No region server available");
                return r;
            }

            List<RegionServerInfo> replicas = loadBalancer.selectReplicas(rsList, Math.min(3, rsList.size()));
            List<RegionServerInfo> createdReplicas = new ArrayList<>();
            for (RegionServerInfo replica : replicas) {
                Response ddlResponse = regionDdlExecutor.execute(replica, tableName, ddl);
                if (ddlResponse.getCode() != StatusCode.OK) {
                    rollbackCreatedReplicas(tableName, createdReplicas);
                    Response error = new Response(StatusCode.ERROR);
                    error.setMessage("Failed to create table on replica " + replica.getId()
                            + ": " + ddlResponse.getMessage());
                    return error;
                }
                createdReplicas.add(replica);
            }

            RegionServerInfo primary = replicas.get(0);
            TableLocation location = new TableLocation(tableName, primary, replicas);
            location.setTableStatus("ACTIVE");
            location.setVersion(System.currentTimeMillis());

            try {
                metaManager.saveTableLocation(location);
                assignmentManager.saveAssignment(tableName, replicas);
                touchStatusUpdatedAtBestEffort(tableName);
            } catch (Exception persistException) {
                rollbackCreateTableMetadata(tableName);
                rollbackCreatedReplicas(tableName, createdReplicas);
                Response error = new Response(StatusCode.ERROR);
                error.setMessage("Failed to persist table metadata for " + tableName
                        + ": " + persistException.getMessage());
                return error;
            }

            Response r = new Response(StatusCode.OK);
            r.setMessage("Table metadata created: " + tableName);
            return r;
        } catch (Exception e) {
            throw new TException("Failed to create table metadata", e);
        }
    }

    @Override
    public Response dropTable(String tableName) throws TException {
        if (!isLeader()) {
            return notLeaderResponse("dropTable");
        }

        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("ZooKeeper is unavailable");
            return r;
        }

        try {
            if (!metaManager.tableExists(tableName)) {
                Response r = new Response(StatusCode.TABLE_NOT_FOUND);
                r.setMessage("Table not found: " + tableName);
                return r;
            }
            List<RegionServerInfo> replicas = assignmentManager.getAssignment(tableName);
            if (replicas.isEmpty()) {
                TableLocation location = metaManager.getTableLocation(tableName);
                if (location != null && location.isSetReplicas()) {
                    replicas = location.getReplicas();
                }
            }
            String ddl = "drop table " + tableName + ";";
            List<String> failedReplicas = new ArrayList<>();
            for (RegionServerInfo replica : replicas) {
                Response ddlResponse = regionDdlExecutor.execute(replica, tableName, ddl);
                if (ddlResponse.getCode() != StatusCode.OK
                        && ddlResponse.getCode() != StatusCode.TABLE_NOT_FOUND) {
                    failedReplicas.add(replica.getId() + "(" + ddlResponse.getCode() + ")");
                }
            }
            if (!failedReplicas.isEmpty()) {
                Response error = new Response(StatusCode.ERROR);
                error.setMessage("Failed to drop table on replicas " + failedReplicas
                        + "; metadata retained for retry");
                return error;
            }
            metaManager.deleteTableLocation(tableName);
            assignmentManager.deleteAssignment(tableName);

            Response r = new Response(StatusCode.OK);
            r.setMessage("Table dropped: " + tableName);
            return r;
        } catch (Exception e) {
            throw new TException("Failed to drop table metadata: " + tableName, e);
        }
    }

    @Override
    public String getActiveMaster() throws TException {
        return MasterRuntimeContext.readActiveMasterAddress();
    }

    @Override
    public List<RegionServerInfo> listRegionServers() throws TException {
        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            return Collections.emptyList();
        }

        try {
            List<String> children = zk.getChildren().forPath(ZkPaths.REGION_SERVERS);
            List<RegionServerInfo> infos = new ArrayList<>();
            for (String child : children) {
                String path = ZkPaths.regionServer(child);
                byte[] bytes = zk.getData().forPath(path);
                if (bytes == null || bytes.length == 0) {
                    continue;
                }
                Map<?, ?> node = MAPPER.readValue(bytes, Map.class);
                infos.add(mapToRegionServerInfo(node));
            }
            return infos;
        } catch (Exception e) {
            throw new TException("Failed to list region servers", e);
        }
    }

    @Override
    public List<TableLocation> listTables() throws TException {
        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            return Collections.emptyList();
        }

        try {
            List<TableLocation> locations = metaManager.listTables();
            List<TableLocation> healed = new ArrayList<>();
            for (TableLocation location : locations) {
                TableLocation recovered = regionMigrator.recoverStuckMigrationWithConfirmation(
                    location,
                    migrationStuckTimeoutMs,
                    this::readMigrationContext,
                    this::resolveRegionServerForRecovery,
                    this::resolveCrossNodeRecoveryCandidates);
                healed.add(healTableLocationBestEffort(recovered));
            }
            return healed;
        } catch (Exception e) {
            throw new TException("Failed to list table metadata", e);
        }
    }

    @Override
    public Response triggerRebalance() throws TException {
        if (!isLeader()) {
            return notLeaderResponse("triggerRebalance");
        }
        if (isZkUnavailable(zk())) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("ZooKeeper is unavailable");
            return r;
        }
        try {
            List<RegionServerInfo> regionServers = listRegionServers();
            log.info("triggerRebalance start onlineRs={} rsLoads={}",
                    regionServers.size(),
                    regionServers.stream()
                            .map(rs -> rs.getId() + ":" + (rs.isSetTableCount() ? rs.getTableCount() : 0))
                            .toList());
            if (regionServers.size() < 2) {
                log.info("triggerRebalance skipped: fewer than 2 region servers");
                Response r = new Response(StatusCode.OK);
                r.setMessage("Rebalance skipped: fewer than 2 region servers");
                return r;
            }

            int recoveredBeforeSchedule = recoverStuckMigrationsForRebalanceWithConfirmation();

            if (loadBalancer.isBalanced(regionServers, MasterConfig.fromSystemEnv().rebalanceRatio())) {
                log.info("triggerRebalance skipped: cluster already balanced");
                Response r = new Response(StatusCode.OK);
                r.setMessage(skipMessageWithRecovered("Cluster already balanced", recoveredBeforeSchedule));
                return r;
            }
            TableLocation candidate = selectRebalanceCandidate(regionServers);
            if (candidate == null) {
                log.info("triggerRebalance skipped: no migratable replica found");
                Response r = new Response(StatusCode.OK);
                r.setMessage(skipMessageWithRecovered("Rebalance skipped: no migratable replica found", recoveredBeforeSchedule));
                return r;
            }
            RegionServerInfo source = findNonPrimaryReplicaOnHotNode(candidate, regionServers);
            if (source == null) {
                log.info("triggerRebalance skipped: only primary replicas on hotspot table={}",
                        candidate.getTableName());
                Response r = new Response(StatusCode.OK);
                r.setMessage(skipMessageWithRecovered("Rebalance skipped: only primary replicas on hotspot", recoveredBeforeSchedule));
                return r;
            }
            RegionServerInfo target = loadBalancer.leastLoadedExcluding(regionServers,
                    candidate.getReplicas().stream().map(RegionServerInfo::getId).toList());
            if (target == null) {
                log.info("triggerRebalance skipped: no eligible target region server table={}",
                        candidate.getTableName());
                Response r = new Response(StatusCode.OK);
                r.setMessage(skipMessageWithRecovered("Rebalance skipped: no eligible target region server", recoveredBeforeSchedule));
                return r;
            }
            int sourceLoad = source.isSetTableCount() ? source.getTableCount() : 0;
            int targetLoad = target.isSetTableCount() ? target.getTableCount() : 0;
            if (targetLoad >= sourceLoad) {
                log.info("triggerRebalance skipped: target not lighter table={} source={}({}) target={}({})",
                        candidate.getTableName(), source.getId(), sourceLoad, target.getId(), targetLoad);
                Response r = new Response(StatusCode.OK);
                r.setMessage(skipMessageWithRecovered("Rebalance skipped: no lighter target region server available", recoveredBeforeSchedule));
                return r;
            }

            return regionMigrator.rebalanceReplica(candidate, source, target);
        } catch (Exception e) {
            throw new TException("Failed to trigger rebalance", e);
        }
    }

    private int recoverStuckMigrationsForRebalanceWithConfirmation() {
        try {
            int recovered = 0;
            for (TableLocation location : metaManager.listTables()) {
                TableLocation recoveredLocation = regionMigrator.recoverStuckMigrationWithConfirmation(
                    location,
                    migrationStuckTimeoutMs,
                    this::readMigrationContext,
                    this::resolveRegionServerForRecovery,
                    this::resolveCrossNodeRecoveryCandidates);
                if (!Objects.equals(location.getTableStatus(), recoveredLocation.getTableStatus())
                        && "ACTIVE".equalsIgnoreCase(recoveredLocation.getTableStatus())) {
                    recovered++;
                }
            }
            if (recovered > 0) {
                log.warn("triggerRebalance proactively recovered {} stuck migration table(s)", recovered);
            }
            return recovered;
        } catch (Exception e) {
            log.warn("triggerRebalance stuck migration pre-recovery failed: {}", e.getMessage());
            return 0;
        }
    }

    private static String skipMessageWithRecovered(String baseMessage, int recoveredCount) {
        if (recoveredCount <= 0) {
            return baseMessage;
        }
        return baseMessage + " (recovered " + recoveredCount + " stuck migration(s))";
    }

    private void rollbackCreatedReplicas(String tableName, List<RegionServerInfo> createdReplicas) {
        String rollbackDdl = "drop table " + tableName + ";";
        for (RegionServerInfo replica : createdReplicas) {
            try {
                regionDdlExecutor.execute(replica, tableName, rollbackDdl);
            } catch (Exception e) {
                log.warn("Rollback dropTable failed on replica {}: {}",
                        replica.getId(), e.getMessage());
            }
        }
    }

    private void rollbackCreateTableMetadata(String tableName) {
        try {
            assignmentManager.deleteAssignment(tableName);
        } catch (Exception e) {
            log.warn("Rollback assignment delete failed for table {}: {}", tableName, e.getMessage());
        }
        try {
            metaManager.deleteTableLocation(tableName);
        } catch (Exception e) {
            log.warn("Rollback metadata delete failed for table {}: {}", tableName, e.getMessage());
        }
    }

    private TableLocation selectRebalanceCandidate(List<RegionServerInfo> regionServers) throws Exception {
        RegionServerInfo hot = loadBalancer.hottest(regionServers);
        if (hot == null) {
            return null;
        }
        List<TableLocation> tables = metaManager.listTables();
        if (tables.isEmpty()) {
            return null;
        }
        int startIndex = (int) Math.floorMod(rebalanceCandidateCursor.getAndIncrement(), tables.size());
        for (int i = 0; i < tables.size(); i++) {
            TableLocation table = tables.get((startIndex + i) % tables.size());
            if (!STATUS_ACTIVE.equalsIgnoreCase(table.getTableStatus())) {
                log.info("triggerRebalance skip non-active table={} status={}",
                        table.getTableName(), table.getTableStatus());
                continue;
            }
            if (table.getReplicas().stream().anyMatch(rs -> hot.getId().equals(rs.getId()))
                    && !hot.getId().equals(table.getPrimaryRS().getId())) {
                return table;
            }
        }
        return null;
    }

    private RegionServerInfo findNonPrimaryReplicaOnHotNode(TableLocation location, List<RegionServerInfo> regionServers) {
        RegionServerInfo hot = loadBalancer.hottest(regionServers);
        if (hot == null) {
            return null;
        }
        boolean presentAsNonPrimary = location.getReplicas().stream()
                .anyMatch(rs -> hot.getId().equals(rs.getId()) && !location.getPrimaryRS().getId().equals(rs.getId()));
        return presentAsNonPrimary ? hot : null;
    }

    private void touchStatusUpdatedAtBestEffort(String tableName) {
        try {
            metaManager.touchStatusUpdatedAt(tableName);
        } catch (Exception e) {
            log.warn("triggerRebalance touch statusUpdatedAt failed table={} cause={}",
                    tableName, e.getMessage());
        }
    }

    private static final int HEAL_CLEANUP_MAX_ATTEMPTS = 3;

    private boolean cleanupTargetReplicaWithConfirmation(RegionServerInfo target, String tableName, String reason) {
        return cleanupReplicaWithConfirmation(target, tableName, reason, "target");
    }

    private boolean cleanupReplicaWithConfirmation(RegionServerInfo replica,
                                                   String tableName,
                                                   String reason,
                                                   String role) {
        if (replica == null || !replica.isSetId()) {
            return true;
        }
        String lastError = null;
        for (int attempt = 1; attempt <= HEAL_CLEANUP_MAX_ATTEMPTS; attempt++) {
            try {
                Response response = regionAdminExecutor.deleteLocalTable(replica, tableName);
                if (response.getCode() == StatusCode.OK || response.getCode() == StatusCode.TABLE_NOT_FOUND) {
                    log.info("healTableLocation cleanup {} replica confirmed table={} replica={} reason={} code={} attempt={}/{}",
                            role, tableName, replica.getId(), reason, response.getCode(), attempt, HEAL_CLEANUP_MAX_ATTEMPTS);
                    return true;
                }
                lastError = "code=" + response.getCode() + " msg=" + response.getMessage();
                log.warn("healTableLocation cleanup {} replica failed table={} replica={} reason={} attempt={}/{} {}",
                        role,
                        tableName,
                        replica.getId(),
                        reason,
                        attempt,
                        HEAL_CLEANUP_MAX_ATTEMPTS,
                        lastError);
            } catch (Exception e) {
                lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
                log.warn("healTableLocation cleanup {} replica exception table={} replica={} reason={} attempt={}/{} cause={}",
                        role,
                        tableName,
                        replica.getId(),
                        reason,
                        attempt,
                        HEAL_CLEANUP_MAX_ATTEMPTS,
                        e.getMessage());
            }
        }
        log.warn("healTableLocation cleanup {} replica exhausted retries table={} replica={} reason={} lastError={}",
                role,
                tableName,
                replica.getId(),
                reason,
                lastError == null ? "unknown" : lastError);
        return false;
    }

    private TableLocation healTableLocationBestEffort(TableLocation location) {
        if (location == null
                || !location.isSetPrimaryRS()
                || !location.isSetReplicas()
                || location.getReplicas().isEmpty()) {
            return location;
        }

        String currentStatus = (location.isSetTableStatus() && !location.getTableStatus().isBlank())
            ? location.getTableStatus().toUpperCase()
            : STATUS_ACTIVE;
        if (!STATUS_ACTIVE.equals(currentStatus) && !STATUS_UNAVAILABLE.equals(currentStatus)) {
            return location;
        }

        String currentPrimaryId = location.getPrimaryRS().getId();
        if (currentPrimaryId == null || currentPrimaryId.isBlank()) {
            return location;
        }

        List<RegionServerInfo> onlineServers;
        try {
            onlineServers = listRegionServers();
        } catch (Exception e) {
            log.warn("getTableLocation failed to check online region servers table={} cause={}",
                    location.getTableName(), e.getMessage());
            return location;
        }

        Set<String> onlineIds = new HashSet<>();
        for (RegionServerInfo regionServer : onlineServers) {
            if (regionServer != null && regionServer.isSetId()) {
                onlineIds.add(regionServer.getId());
            }
        }

        List<RegionServerInfo> onlineReplicas = new ArrayList<>();
        for (RegionServerInfo replica : location.getReplicas()) {
            if (replica != null && replica.isSetId() && onlineIds.contains(replica.getId())) {
                onlineReplicas.add(replica);
            }
        }

        if (onlineReplicas.isEmpty()) {
            if (!STATUS_UNAVAILABLE.equals(currentStatus)) {
                TableLocation unavailable = new TableLocation(location);
                unavailable.setTableStatus(STATUS_UNAVAILABLE);
                unavailable.setVersion(System.currentTimeMillis());
                long now = clockMs.getAsLong();
                String signature = healSignature(unavailable);
                if (isHealPersistThrottled(unavailable.getTableName(), signature, now)) {
                    return unavailable;
                }
                try {
                    metaManager.saveTableLocation(unavailable);
                    assignmentManager.saveAssignment(unavailable.getTableName(), unavailable.getReplicas());
                    touchStatusUpdatedAtBestEffort(unavailable.getTableName());
                    recordHealPersist(unavailable.getTableName(), signature, now);
                    log.warn("healTableLocation marked table unavailable table={} primary={}",
                            unavailable.getTableName(), currentPrimaryId);
                    return unavailable;
                } catch (Exception e) {
                    log.error("healTableLocation failed to persist unavailable status table={} cause={}",
                            location.getTableName(), e.getMessage());
                }
            }
            log.warn("getTableLocation found no online replicas table={} primary={}",
                    location.getTableName(), currentPrimaryId);
            return location;
        }

        boolean changed = false;
        RegionServerInfo newPrimary = location.getPrimaryRS();
        if (!onlineIds.contains(currentPrimaryId)) {
            newPrimary = onlineReplicas.get(0);
            changed = true;
            log.warn("healTableLocation promoted primary table={} from {} to {}",
                    location.getTableName(), currentPrimaryId, newPrimary.getId());
        }

        List<RegionServerInfo> healedReplicas = new ArrayList<>(onlineReplicas);
        int targetReplicaCount = Math.min(3, onlineIds.size());
        List<RegionServerInfo> refillTargets = List.of();
        if (healedReplicas.size() < targetReplicaCount) {
            List<RegionServerInfo> candidates = new ArrayList<>();
            Set<String> existingIds = new HashSet<>();
            for (RegionServerInfo replica : healedReplicas) {
                existingIds.add(replica.getId());
            }
            for (RegionServerInfo online : onlineServers) {
                if (online != null && online.isSetId() && !existingIds.contains(online.getId())) {
                    candidates.add(online);
                }
            }
            refillTargets = loadBalancer.selectReplicas(candidates,
                    targetReplicaCount - healedReplicas.size());
            if (!refillTargets.isEmpty()) {
                changed = true;
            }
        }

        final String primaryId = newPrimary.getId();
        if (!healedReplicas.stream().anyMatch(rs -> rs.getId().equals(primaryId))) {
            healedReplicas.add(0, newPrimary);
            changed = true;
        }

        if (STATUS_UNAVAILABLE.equals(currentStatus)) {
            changed = true;
        }

        if (!changed) {
            return location;
        }

        long now = clockMs.getAsLong();
        List<RegionServerInfo> plannedReplicas = new ArrayList<>(healedReplicas);
        for (RegionServerInfo target : refillTargets) {
            if (target != null && target.isSetId()) {
                plannedReplicas.add(target);
            }
        }
        ensurePrimaryIncluded(plannedReplicas, newPrimary);
        TableLocation plannedLocation = new TableLocation(location.getTableName(), newPrimary, plannedReplicas);
        plannedLocation.setVersion(System.currentTimeMillis());
        plannedLocation.setTableStatus("ACTIVE");
        String signature = healSignature(plannedLocation);
        if (isHealPersistThrottled(plannedLocation.getTableName(), signature, now)) {
            TableLocation transientLocation = new TableLocation(location.getTableName(), newPrimary, healedReplicas);
            transientLocation.setVersion(System.currentTimeMillis());
            transientLocation.setTableStatus("ACTIVE");
            return transientLocation;
        }

        List<RegionServerInfo> clonedTargets = new ArrayList<>();
        for (RegionServerInfo target : refillTargets) {
            RegionServerInfo source = chooseReplicaCloneSource(newPrimary, healedReplicas, target);
            if (source == null) {
                log.warn("healTableLocation skipped replica refill table={} target={} reason=no_online_source",
                        location.getTableName(), target.getId());
                continue;
            }
            if (cloneReplicaBestEffort(location.getTableName(), source, target)) {
                healedReplicas.add(target);
                clonedTargets.add(target);
                log.info("healTableLocation refilled replica table={} source={} target={}",
                        location.getTableName(), source.getId(), target.getId());
            }
        }

        ensurePrimaryIncluded(healedReplicas, newPrimary);

        TableLocation promotedLocation = new TableLocation(location.getTableName(), newPrimary, healedReplicas);
        promotedLocation.setVersion(System.currentTimeMillis());
        promotedLocation.setTableStatus("ACTIVE");

        if (promotedLocation.getReplicas().size() < targetReplicaCount) {
            log.warn("healTableLocation recovered with reduced replicas table={} expected={} actual={}",
                    promotedLocation.getTableName(), targetReplicaCount, promotedLocation.getReplicas().size());
        }

        try {
            metaManager.saveTableLocation(promotedLocation);
            assignmentManager.saveAssignment(promotedLocation.getTableName(), healedReplicas);
            touchStatusUpdatedAtBestEffort(promotedLocation.getTableName());
            recordHealPersist(promotedLocation.getTableName(), signature, now);
            log.info("healTableLocation updated table={} primary={} replicas={}",
                    promotedLocation.getTableName(),
                    promotedLocation.getPrimaryRS().getId(),
                    healedReplicas.stream().map(RegionServerInfo::getId).toList());
            return promotedLocation;
        } catch (Exception e) {
            for (RegionServerInfo clonedTarget : clonedTargets) {
                cleanupTargetReplicaWithConfirmation(clonedTarget,
                        promotedLocation.getTableName(),
                        "heal_metadata_persist_failed");
            }
            log.error("healTableLocation failed to persist update table={} primary={} cause={}",
                    promotedLocation.getTableName(), promotedLocation.getPrimaryRS().getId(), e.getMessage());
            return location;
        }
    }

    private void ensurePrimaryIncluded(List<RegionServerInfo> replicas, RegionServerInfo primary) {
        if (replicas == null || primary == null || !primary.isSetId()) {
            return;
        }
        String primaryId = primary.getId();
        boolean hasPrimary = replicas.stream()
                .anyMatch(rs -> rs != null && rs.isSetId() && primaryId.equals(rs.getId()));
        if (!hasPrimary) {
            replicas.add(0, primary);
        }
    }

    private RegionServerInfo chooseReplicaCloneSource(RegionServerInfo preferredPrimary,
                                                      List<RegionServerInfo> onlineReplicas,
                                                      RegionServerInfo target) {
        if (target == null || !target.isSetId()) {
            return null;
        }
        if (preferredPrimary != null && preferredPrimary.isSetId()) {
            String preferredPrimaryId = preferredPrimary.getId();
            if (!preferredPrimaryId.equals(target.getId())) {
                boolean hasPreferred = onlineReplicas.stream()
                        .anyMatch(rs -> rs != null && rs.isSetId() && preferredPrimaryId.equals(rs.getId()));
                if (hasPreferred) {
                    return preferredPrimary;
                }
            }
        }
        for (RegionServerInfo replica : onlineReplicas) {
            if (replica != null
                    && replica.isSetId()
                    && !replica.getId().equals(target.getId())) {
                return replica;
            }
        }
        return null;
    }

    private boolean cloneReplicaBestEffort(String tableName,
                                           RegionServerInfo source,
                                           RegionServerInfo target) {
        boolean writePaused = false;
        boolean transferAttempted = false;
        boolean transferSucceeded = false;
        boolean resumeSucceeded = true;
        try {
            Response pause = regionAdminExecutor.pauseTableWrite(source, tableName);
            if (pause.getCode() != StatusCode.OK) {
                log.warn("healTableLocation pause failed table={} source={} target={} code={} msg={}",
                        tableName,
                        source.getId(),
                        target.getId(),
                        pause.getCode(),
                        pause.getMessage());
                return false;
            }
            writePaused = true;

            transferAttempted = true;
            Response transfer = regionAdminExecutor.transferTable(source, tableName, target);
            if (transfer.getCode() == StatusCode.OK) {
                transferSucceeded = true;
            } else {
                log.warn("healTableLocation transfer failed table={} source={} target={} code={} msg={}",
                        tableName,
                        source.getId(),
                        target.getId(),
                        transfer.getCode(),
                        transfer.getMessage());
            }
        } catch (Exception e) {
            log.warn("healTableLocation transfer exception table={} source={} target={} cause={}",
                    tableName,
                    source.getId(),
                    target.getId(),
                    e.getMessage());
        } finally {
            if (writePaused) {
                try {
                    Response resume = regionAdminExecutor.resumeTableWrite(source, tableName);
                    if (resume.getCode() != StatusCode.OK) {
                        resumeSucceeded = false;
                        log.warn("healTableLocation resume failed table={} source={} target={} code={} msg={}",
                                tableName,
                                source.getId(),
                                target.getId(),
                                resume.getCode(),
                                resume.getMessage());
                    }
                } catch (Exception resumeException) {
                    resumeSucceeded = false;
                    log.warn("healTableLocation resume exception table={} source={} target={} cause={}",
                            tableName,
                            source.getId(),
                            target.getId(),
                            resumeException.getMessage());
                }
            }
        }
        if (transferSucceeded && resumeSucceeded) {
            return true;
        }
        if (transferSucceeded) {
            log.warn("healTableLocation transfer reverted due to resume failure table={} source={} target={}",
                    tableName,
                    source.getId(),
                    target.getId());
        }
        if (transferAttempted) {
            cleanupTargetReplicaWithConfirmation(target, tableName, "heal_transfer_failed");
        }
        return false;
    }

    private String healSignature(TableLocation location) {
        List<String> replicaIds = new ArrayList<>();
        if (location.isSetReplicas()) {
            for (RegionServerInfo replica : location.getReplicas()) {
                if (replica != null && replica.isSetId()) {
                    replicaIds.add(replica.getId());
                }
            }
        }
        replicaIds.sort(String::compareTo);
        String primary = location.isSetPrimaryRS() && location.getPrimaryRS().isSetId()
                ? location.getPrimaryRS().getId()
                : "";
        String status = location.isSetTableStatus() ? location.getTableStatus() : "";
        return status + "|" + primary + "|" + String.join(",", replicaIds);
    }

    private boolean isHealPersistThrottled(String tableName, String signature, long now) {
        HealPersistState previous = lastHealPersistByTable.get(tableName);
        if (previous == null || !Objects.equals(previous.signature, signature)) {
            return false;
        }
        if (now - previous.persistedAtMs < routeHealMinGapMs) {
            log.debug("healTableLocation throttled table={} signature={} gap={}ms < minGap={}ms",
                    tableName, signature, now - previous.persistedAtMs, routeHealMinGapMs);
            return true;
        }
        return false;
    }

    private void recordHealPersist(String tableName, String signature, long now) {
        lastHealPersistByTable.put(tableName, new HealPersistState(signature, now));
    }

    private static final class HealPersistState {
        private final String signature;
        private final long persistedAtMs;

        private HealPersistState(String signature, long persistedAtMs) {
            this.signature = signature;
            this.persistedAtMs = persistedAtMs;
        }
    }

    private RegionServerInfo resolveRegionServerForRecovery(TableLocation location, String replicaId) {
        if (replicaId == null || replicaId.isBlank()) {
            return null;
        }
        if (location != null && location.isSetReplicas()) {
            for (RegionServerInfo replica : location.getReplicas()) {
                if (replica != null
                        && replica.isSetId()
                        && replicaId.equals(replica.getId())) {
                    return replica;
                }
            }
        }
        try {
            for (RegionServerInfo regionServerInfo : listRegionServers()) {
                if (regionServerInfo != null
                        && regionServerInfo.isSetId()
                        && replicaId.equals(regionServerInfo.getId())) {
                    return regionServerInfo;
                }
            }
        } catch (Exception e) {
            log.warn("resolveRegionServerForRecovery failed table={} replicaId={} cause={}",
                    location == null ? "" : location.getTableName(),
                    replicaId,
                    e.getMessage());
        }
        return null;
    }

    private List<RegionServerInfo> resolveCrossNodeRecoveryCandidates(TableLocation location,
                                                                       String compensationRole,
                                                                       String sourceReplicaId,
                                                                       String targetReplicaId) {
        Set<String> assignedReplicaIds = new HashSet<>();
        if (location != null && location.isSetReplicas() && location.getReplicas() != null) {
            for (RegionServerInfo replica : location.getReplicas()) {
                if (replica != null && replica.isSetId() && !replica.getId().isBlank()) {
                    assignedReplicaIds.add(replica.getId());
                }
            }
        }

        List<RegionServerInfo> candidates = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        try {
            for (RegionServerInfo regionServerInfo : listRegionServers()) {
                if (regionServerInfo == null || !regionServerInfo.isSetId() || regionServerInfo.getId().isBlank()) {
                    continue;
                }
                String replicaId = regionServerInfo.getId();
                if (!seen.add(replicaId)) {
                    continue;
                }
                if (assignedReplicaIds.contains(replicaId)) {
                    continue;
                }
                candidates.add(regionServerInfo);
            }
        } catch (Exception e) {
            log.warn("resolveCrossNodeRecoveryCandidates failed table={} role={} source={} target={} cause={}",
                    location == null ? "" : location.getTableName(),
                    compensationRole,
                    sourceReplicaId,
                    targetReplicaId,
                    e.getMessage());
        }
        return candidates;
    }

    @SuppressWarnings("unchecked")
    private boolean setMigrationContext(String tableName,
                                        String migrationAttemptId,
                                        String sourceReplicaId,
                                        String targetReplicaId,
                                        String compensationRole,
                                        boolean compensationBlocked,
                                        String compensationLastError,
                                        long compensationUpdatedAtMs) {
        CuratorFramework zk = zk();
        if (migrationAttemptId == null || migrationAttemptId.isBlank() || isZkUnavailable(zk)) {
            return false;
        }
        try {
            String path = tableMetaPath(tableName);
            if (zk.checkExists().forPath(path) == null) {
                return false;
            }
            byte[] data = zk.getData().forPath(path);
            if (data == null || data.length == 0) {
                return false;
            }
            Map<String, Object> root = MAPPER.readValue(data, Map.class);
            Object existingAttempt = root.get("migrationAttemptId");
            Object existingSource = root.get("migrationSourceReplicaId");
            Object existingTarget = root.get("migrationTargetReplicaId");
            Object existingCompensationRole = root.get("migrationCompensationRole");
            Object existingCompensationBlocked = root.get("migrationCompensationBlocked");
            Object existingCompensationError = root.get("migrationCompensationLastError");
            Object existingCompensationUpdatedAt = root.get("migrationCompensationUpdatedAtMs");
            if (!Objects.equals(existingAttempt, migrationAttemptId)
                    || !Objects.equals(existingSource, sourceReplicaId)
                    || !Objects.equals(existingTarget, targetReplicaId)
                    || !Objects.equals(existingCompensationRole, compensationRole)
                    || !Objects.equals(Boolean.parseBoolean(String.valueOf(existingCompensationBlocked)), compensationBlocked)
                    || !Objects.equals(existingCompensationError, compensationLastError)
                    || !Objects.equals(toLong(existingCompensationUpdatedAt, 0L), compensationUpdatedAtMs)) {
                root.put("migrationAttemptId", migrationAttemptId);
                if (sourceReplicaId != null && !sourceReplicaId.isBlank()) {
                    root.put("migrationSourceReplicaId", sourceReplicaId);
                } else {
                    root.remove("migrationSourceReplicaId");
                }
                if (targetReplicaId != null && !targetReplicaId.isBlank()) {
                    root.put("migrationTargetReplicaId", targetReplicaId);
                } else {
                    root.remove("migrationTargetReplicaId");
                }
                if (compensationRole != null && !compensationRole.isBlank()) {
                    root.put("migrationCompensationRole", compensationRole);
                } else {
                    root.remove("migrationCompensationRole");
                }
                if (compensationBlocked) {
                    root.put("migrationCompensationBlocked", true);
                } else {
                    root.remove("migrationCompensationBlocked");
                }
                if (compensationLastError != null && !compensationLastError.isBlank()) {
                    root.put("migrationCompensationLastError", compensationLastError);
                } else {
                    root.remove("migrationCompensationLastError");
                }
                if (compensationUpdatedAtMs > 0L) {
                    root.put("migrationCompensationUpdatedAtMs", compensationUpdatedAtMs);
                } else {
                    root.remove("migrationCompensationUpdatedAtMs");
                }
                zk.setData().forPath(path, stringifyMap(root).getBytes(StandardCharsets.UTF_8));
            }
            return true;
        } catch (Exception e) {
            log.warn("triggerRebalance set migrationAttemptId failed table={} cause={}",
                    tableName, e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private boolean clearMigrationContext(String tableName) {
        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            return false;
        }
        try {
            String path = tableMetaPath(tableName);
            if (zk.checkExists().forPath(path) == null) {
                return false;
            }
            byte[] data = zk.getData().forPath(path);
            if (data == null || data.length == 0) {
                return false;
            }
            Map<String, Object> root = MAPPER.readValue(data, Map.class);
            Object removedAttempt = root.remove("migrationAttemptId");
            Object removedSource = root.remove("migrationSourceReplicaId");
            Object removedTarget = root.remove("migrationTargetReplicaId");
            Object removedCompensationRole = root.remove("migrationCompensationRole");
            Object removedCompensationBlocked = root.remove("migrationCompensationBlocked");
            Object removedCompensationError = root.remove("migrationCompensationLastError");
            Object removedCompensationUpdatedAt = root.remove("migrationCompensationUpdatedAtMs");
            if (removedAttempt != null
                    || removedSource != null
                    || removedTarget != null
                    || removedCompensationRole != null
                    || removedCompensationBlocked != null
                    || removedCompensationError != null
                    || removedCompensationUpdatedAt != null) {
                zk.setData().forPath(path, stringifyMap(root).getBytes(StandardCharsets.UTF_8));
            }
            return true;
        } catch (Exception e) {
            log.warn("triggerRebalance clear migrationAttemptId failed table={} cause={}",
                    tableName, e.getMessage());
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    private RegionMigrator.MigrationContext readMigrationContext(String tableName) {
        CuratorFramework zk = zk();
        if (isZkUnavailable(zk)) {
            return null;
        }
        try {
            String path = tableMetaPath(tableName);
            if (zk.checkExists().forPath(path) == null) {
                return null;
            }
            byte[] data = zk.getData().forPath(path);
            if (data == null || data.length == 0) {
                return null;
            }
            Map<String, Object> root = MAPPER.readValue(data, Map.class);
            Object attempt = root.get("migrationAttemptId");
            if (attempt == null) {
                return null;
            }
            Object source = root.get("migrationSourceReplicaId");
            Object target = root.get("migrationTargetReplicaId");
            Object compensationRole = root.get("migrationCompensationRole");
            Object compensationBlocked = root.get("migrationCompensationBlocked");
            Object compensationError = root.get("migrationCompensationLastError");
            Object compensationUpdatedAt = root.get("migrationCompensationUpdatedAtMs");
            return new RegionMigrator.MigrationContext(
                    String.valueOf(attempt),
                    source == null ? null : String.valueOf(source),
                    target == null ? null : String.valueOf(target),
                    compensationRole == null ? null : String.valueOf(compensationRole),
                    compensationBlocked != null && Boolean.parseBoolean(String.valueOf(compensationBlocked)),
                    compensationError == null ? null : String.valueOf(compensationError),
                    toLong(compensationUpdatedAt, 0L));
        } catch (Exception e) {
            log.warn("read migration context failed table={} cause={}", tableName, e.getMessage());
            return null;
        }
    }
}
