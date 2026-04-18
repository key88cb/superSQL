package edu.zju.supersql.master.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.MasterConfig;
import edu.zju.supersql.master.MasterRuntimeContext;
import edu.zju.supersql.master.ZkPaths;
import edu.zju.supersql.master.balance.LoadBalancer;
import edu.zju.supersql.master.meta.AssignmentManager;
import edu.zju.supersql.master.meta.MetaManager;
import edu.zju.supersql.rpc.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * MasterService stub implementation.
 * All methods return an ERROR response until the real logic is implemented.
 */
public class MasterServiceImpl implements MasterService.Iface {

    private static final Logger log = LoggerFactory.getLogger(MasterServiceImpl.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Pattern CREATE_TABLE_PATTERN =
            Pattern.compile("(?i)^\\s*create\\s+table\\s+([a-zA-Z_][a-zA-Z0-9_]*)");

    private final MetaManager metaManager;
    private final AssignmentManager assignmentManager;
    private final LoadBalancer loadBalancer;
    private final RegionDdlExecutor regionDdlExecutor;
    private final RegionAdminExecutor regionAdminExecutor;

    public MasterServiceImpl() {
        this(new MetaManager(MasterRuntimeContext.getZkClient()),
                new AssignmentManager(MasterRuntimeContext.getZkClient()),
                new LoadBalancer(),
                new RegionServiceDdlExecutor(10_000),
                new ThriftRegionAdminExecutor(10_000));
    }

    public MasterServiceImpl(MetaManager metaManager,
                             AssignmentManager assignmentManager,
                             LoadBalancer loadBalancer,
                             RegionDdlExecutor regionDdlExecutor,
                             RegionAdminExecutor regionAdminExecutor) {
        this.metaManager = metaManager;
        this.assignmentManager = assignmentManager;
        this.loadBalancer = loadBalancer;
        this.regionDdlExecutor = regionDdlExecutor;
        this.regionAdminExecutor = regionAdminExecutor;
    }

    private static Response notImplemented(String method) {
        log.warn("MasterService.{} called — not yet implemented", method);
        Response r = new Response(StatusCode.ERROR);
        r.setMessage("Not implemented: " + method);
        return r;
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

    private static String assignmentPath(String tableName) {
        return ZkPaths.assignment(tableName);
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

    @SuppressWarnings("unchecked")
    private static TableLocation bytesToLocation(byte[] bytes, String fallbackTableName) throws Exception {
        Map<String, Object> raw = MAPPER.readValue(bytes, Map.class);
        String tableName = (String) raw.getOrDefault("tableName", fallbackTableName);
        Map<String, Object> primaryRaw = (Map<String, Object>) raw.get("primaryRS");
        List<Map<String, Object>> replicasRaw = (List<Map<String, Object>>) raw.get("replicas");

        RegionServerInfo primary = primaryRaw == null
                ? new RegionServerInfo("unknown", "127.0.0.1", 0)
                : mapToRegionServerInfo(primaryRaw);

        List<RegionServerInfo> replicas = new ArrayList<>();
        if (replicasRaw != null) {
            for (Map<String, Object> item : replicasRaw) {
                replicas.add(mapToRegionServerInfo(item));
            }
        }
        if (replicas.isEmpty()) {
            replicas.add(primary);
        }

        TableLocation location = new TableLocation(tableName, primary, replicas);
        Object status = raw.get("tableStatus");
        if (status != null) {
            location.setTableStatus(String.valueOf(status));
        }
        Object version = raw.get("version");
        if (version != null) {
            location.setVersion(toLong(version, 0L));
        }
        return location;
    }

    private static byte[] locationToBytes(TableLocation location) throws Exception {
        Map<String, Object> root = new HashMap<>();
        root.put("tableName", location.getTableName());
        root.put("tableStatus", location.getTableStatus());
        root.put("version", location.getVersion());
        root.put("primaryRS", regionToMap(location.getPrimaryRS()));

        List<Map<String, Object>> replicaMaps = new ArrayList<>();
        for (RegionServerInfo rs : location.getReplicas()) {
            replicaMaps.add(regionToMap(rs));
        }
        root.put("replicas", replicaMaps);

        return stringifyMap(root).getBytes(StandardCharsets.UTF_8);
    }

    private static Map<String, Object> regionToMap(RegionServerInfo info) {
        Map<String, Object> m = new HashMap<>();
        m.put("id", info.getId());
        m.put("host", info.getHost());
        m.put("port", info.getPort());
        if (info.isSetTableCount()) {
            m.put("tableCount", info.getTableCount());
        }
        if (info.isSetQps1min()) {
            m.put("qps1min", info.getQps1min());
        }
        if (info.isSetCpuUsage()) {
            m.put("cpuUsage", info.getCpuUsage());
        }
        if (info.isSetMemUsage()) {
            m.put("memUsage", info.getMemUsage());
        }
        if (info.isSetLastHeartbeat()) {
            m.put("lastHeartbeat", info.getLastHeartbeat());
        }
        return m;
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

    @Override
    public TableLocation getTableLocation(String tableName) throws TException {
        if (!isLeader()) {
            String redirect = MasterRuntimeContext.readActiveMasterAddress();
            RegionServerInfo placeholder = new RegionServerInfo("redirect", redirect, 0);
            TableLocation location = new TableLocation(tableName, placeholder, Collections.singletonList(placeholder));
            location.setTableStatus("NOT_LEADER");
            location.setVersion(-1L);
            return location;
        }

        CuratorFramework zk = zk();
        if (zk == null) {
            RegionServerInfo placeholderPrimary = new RegionServerInfo("local-stub", "127.0.0.1", 9090);
            TableLocation location = new TableLocation(tableName, placeholderPrimary,
                    Collections.singletonList(placeholderPrimary));
            location.setTableStatus("ACTIVE");
            location.setVersion(0L);
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
            return healTableLocationBestEffort(location);
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
        if (zk == null) {
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

            metaManager.saveTableLocation(location);
            assignmentManager.saveAssignment(tableName, replicas);
            touchStatusUpdatedAtBestEffort(tableName);

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
        if (zk == null) {
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
            for (RegionServerInfo replica : replicas) {
                Response ddlResponse = regionDdlExecutor.execute(replica, tableName, ddl);
                if (ddlResponse.getCode() != StatusCode.OK
                        && ddlResponse.getCode() != StatusCode.TABLE_NOT_FOUND) {
                    Response error = new Response(StatusCode.ERROR);
                    error.setMessage("Failed to drop table on replica " + replica.getId()
                            + ": " + ddlResponse.getMessage());
                    return error;
                }
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
        if (zk == null) {
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
        if (zk() == null) {
            return Collections.emptyList();
        }

        try {
            List<TableLocation> locations = metaManager.listTables();
            List<TableLocation> healed = new ArrayList<>();
            for (TableLocation location : locations) {
                healed.add(healTableLocationBestEffort(location));
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
        if (zk() == null) {
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
            if (loadBalancer.isBalanced(regionServers, MasterConfig.fromSystemEnv().rebalanceRatio())) {
                log.info("triggerRebalance skipped: cluster already balanced");
                Response r = new Response(StatusCode.OK);
                r.setMessage("Cluster already balanced");
                return r;
            }
            TableLocation candidate = selectRebalanceCandidate(regionServers);
            if (candidate == null) {
                log.info("triggerRebalance skipped: no migratable replica found");
                Response r = new Response(StatusCode.OK);
                r.setMessage("Rebalance skipped: no migratable replica found");
                return r;
            }
            RegionServerInfo source = findNonPrimaryReplicaOnHotNode(candidate, regionServers);
            if (source == null) {
                log.info("triggerRebalance skipped: only primary replicas on hotspot table={}",
                        candidate.getTableName());
                Response r = new Response(StatusCode.OK);
                r.setMessage("Rebalance skipped: only primary replicas on hotspot");
                return r;
            }
            RegionServerInfo target = loadBalancer.leastLoadedExcluding(regionServers,
                    candidate.getReplicas().stream().map(RegionServerInfo::getId).toList());
            if (target == null) {
                log.info("triggerRebalance skipped: no eligible target region server table={}",
                        candidate.getTableName());
                Response r = new Response(StatusCode.OK);
                r.setMessage("Rebalance skipped: no eligible target region server");
                return r;
            }
            int sourceLoad = source.isSetTableCount() ? source.getTableCount() : 0;
            int targetLoad = target.isSetTableCount() ? target.getTableCount() : 0;
            if (targetLoad >= sourceLoad) {
                log.info("triggerRebalance skipped: target not lighter table={} source={}({}) target={}({})",
                        candidate.getTableName(), source.getId(), sourceLoad, target.getId(), targetLoad);
                Response r = new Response(StatusCode.OK);
                r.setMessage("Rebalance skipped: no lighter target region server available");
                return r;
            }

            return rebalanceReplica(candidate, source, target);
        } catch (Exception e) {
            throw new TException("Failed to trigger rebalance", e);
        }
    }

    private void rollbackCreatedReplicas(String tableName, List<RegionServerInfo> createdReplicas) {
        String rollbackDdl = "drop table " + tableName + ";";
        for (RegionServerInfo replica : createdReplicas) {
            try {
                regionDdlExecutor.execute(replica, tableName, rollbackDdl);
            } catch (Exception ignored) {
                log.warn("Rollback dropTable failed on replica {}", replica.getId());
            }
        }
    }

    private TableLocation selectRebalanceCandidate(List<RegionServerInfo> regionServers) throws Exception {
        RegionServerInfo hot = loadBalancer.hottest(regionServers);
        if (hot == null) {
            return null;
        }
        for (TableLocation table : metaManager.listTables()) {
            if (!"ACTIVE".equalsIgnoreCase(table.getTableStatus())) {
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

    private Response rebalanceReplica(TableLocation location, RegionServerInfo source, RegionServerInfo target) throws Exception {
        RegionServerInfo primary = location.getPrimaryRS();
        String tableName = location.getTableName();
        String migrationAttemptId = buildMigrationAttemptId(tableName, source, target);
        TableLocation originalLocation = new TableLocation(location);
        List<RegionServerInfo> originalReplicas = new ArrayList<>(location.getReplicas());
        List<String> beforeReplicas = location.getReplicas().stream().map(RegionServerInfo::getId).toList();
        log.info("triggerRebalance executing table={} primary={} source={} target={} replicasBefore={}",
                tableName, primary.getId(), source.getId(), target.getId(), beforeReplicas);

        if (!markTableStatus(tableName, primary, location.getReplicas(), "PREPARING", migrationAttemptId)) {
            Response error = new Response(StatusCode.ERROR);
            error.setMessage("Failed to mark table as PREPARING before pause: " + tableName);
            return error;
        }

        Response pause = regionAdminExecutor.pauseTableWrite(primary, tableName);
        if (pause.getCode() != StatusCode.OK) {
            log.warn("triggerRebalance pause failed table={} primary={} code={}",
                    tableName, primary.getId(), pause.getCode());
            rollbackRebalanceMetadata(tableName, originalLocation, originalReplicas);
            return pause;
        }

        try {
            if (!markTableStatus(tableName, primary, location.getReplicas(), "MOVING", migrationAttemptId)) {
                rollbackRebalanceMetadata(tableName, originalLocation, originalReplicas);
                Response error = new Response(StatusCode.ERROR);
                error.setMessage("Failed to mark table as MOVING before transfer: " + tableName);
                return error;
            }

            Response transfer = regionAdminExecutor.transferTable(source, tableName, target);
            if (transfer.getCode() != StatusCode.OK) {
                rollbackRebalanceMetadata(tableName, originalLocation, originalReplicas);
                cleanupTargetReplicaBestEffort(target, tableName, "transfer_failed");
                return transfer;
            }

            List<RegionServerInfo> updatedReplicas = new ArrayList<>();
            for (RegionServerInfo replica : location.getReplicas()) {
                updatedReplicas.add(replica.getId().equals(source.getId()) ? target : replica);
            }
            TableLocation updatedLocation = new TableLocation(tableName, primary, updatedReplicas);
            updatedLocation.setTableStatus("ACTIVE");
            updatedLocation.setVersion(System.currentTimeMillis());
            try {
                metaManager.saveTableLocation(updatedLocation);
                assignmentManager.saveAssignment(tableName, updatedReplicas);
                touchStatusUpdatedAtBestEffort(tableName);
                clearMigrationAttemptIdBestEffort(tableName);
            } catch (Exception metadataError) {
                log.error("triggerRebalance metadata persist failed table={}, rolling back to original replicas={} cause={}",
                        tableName,
                        originalReplicas.stream().map(RegionServerInfo::getId).toList(),
                        metadataError.getMessage());
                rollbackRebalanceMetadata(tableName, originalLocation, originalReplicas);
                cleanupTargetReplicaBestEffort(target, tableName, "metadata_persist_failed");
                throw metadataError;
            }
            log.info("triggerRebalance metadata updated table={} replicasAfter={}",
                    tableName, updatedReplicas.stream().map(RegionServerInfo::getId).toList());

            Response delete = regionAdminExecutor.deleteLocalTable(source, tableName);
            if (delete.getCode() != StatusCode.OK) {
                log.warn("triggerRebalance deleteLocalTable failed table={} source={} code={}",
                        tableName, source.getId(), delete.getCode());
                rollbackRebalanceMetadata(tableName, originalLocation, originalReplicas);
                cleanupTargetReplicaBestEffort(target, tableName, "source_delete_failed");
                return delete;
            }

            invalidateClientCacheBestEffort(primary, tableName);
            invalidateClientCacheBestEffort(target, tableName);
            invalidateClientCacheBestEffort(source, tableName);

            Response ok = new Response(StatusCode.OK);
            ok.setMessage("Rebalanced table " + tableName + " from " + source.getId() + " to " + target.getId());
            log.info("triggerRebalance success table={} source={} target={}", tableName, source.getId(), target.getId());
            return ok;
        } finally {
            resumeWriteBestEffort(primary, tableName);
        }
    }

    private boolean markTableStatus(String tableName,
                                    RegionServerInfo primary,
                                    List<RegionServerInfo> replicas,
                        String status,
                        String migrationAttemptId) {
        try {
            TableLocation intermediate = new TableLocation(tableName, primary, replicas);
            intermediate.setTableStatus(status);
            intermediate.setVersion(System.currentTimeMillis());
            metaManager.saveTableLocation(intermediate);
            touchStatusUpdatedAtBestEffort(tableName);
            setMigrationAttemptIdBestEffort(tableName, migrationAttemptId);
            log.info("triggerRebalance marked table {} table={} replicas={}",
                    status, tableName, replicas.stream().map(RegionServerInfo::getId).toList());
            return true;
        } catch (Exception e) {
            log.error("triggerRebalance failed to mark {} table={} cause={}",
                    status, tableName, e.getMessage(), e);
            return false;
        }
    }

    private void rollbackRebalanceMetadata(String tableName,
                                           TableLocation originalLocation,
                                           List<RegionServerInfo> originalReplicas) {
        try {
            metaManager.saveTableLocation(originalLocation);
            assignmentManager.saveAssignment(tableName, originalReplicas);
            touchStatusUpdatedAtBestEffort(tableName);
                clearMigrationAttemptIdBestEffort(tableName);
            log.info("triggerRebalance metadata rollback completed table={} replicasRestored={}",
                    tableName,
                    originalReplicas.stream().map(RegionServerInfo::getId).toList());
        } catch (Exception rollbackError) {
            log.error("triggerRebalance metadata rollback failed table={} replicas={} cause={}",
                    tableName,
                    originalReplicas.stream().map(RegionServerInfo::getId).toList(),
                    rollbackError.getMessage(),
                    rollbackError);
        }
    }

    private void touchStatusUpdatedAtBestEffort(String tableName) {
        try {
            metaManager.touchStatusUpdatedAt(tableName);
        } catch (Exception e) {
            log.warn("triggerRebalance touch statusUpdatedAt failed table={} cause={}",
                    tableName, e.getMessage());
        }
    }

    private void invalidateClientCacheBestEffort(RegionServerInfo regionServer, String tableName) {
        try {
            Response response = regionAdminExecutor.invalidateClientCache(regionServer, tableName);
            if (response.getCode() != StatusCode.OK) {
                log.warn("triggerRebalance cache invalidation failed table={} rs={} code={} msg={}",
                        tableName, regionServer.getId(), response.getCode(), response.getMessage());
            }
        } catch (Exception e) {
            log.warn("triggerRebalance cache invalidation exception table={} rs={} cause={}",
                    tableName, regionServer.getId(), e.getMessage());
        }
    }

    private void cleanupTargetReplicaBestEffort(RegionServerInfo target, String tableName, String reason) {
        try {
            Response response = regionAdminExecutor.deleteLocalTable(target, tableName);
            if (response.getCode() == StatusCode.OK || response.getCode() == StatusCode.TABLE_NOT_FOUND) {
                log.info("triggerRebalance cleanup target replica table={} target={} reason={} code={}",
                        tableName, target.getId(), reason, response.getCode());
            } else {
                log.warn("triggerRebalance cleanup target replica failed table={} target={} reason={} code={} msg={}",
                        tableName, target.getId(), reason, response.getCode(), response.getMessage());
            }
        } catch (Exception e) {
            log.warn("triggerRebalance cleanup target replica exception table={} target={} reason={} cause={}",
                    tableName, target.getId(), reason, e.getMessage());
        }
    }

    private void resumeWriteBestEffort(RegionServerInfo primary, String tableName) {
        try {
            Response response = regionAdminExecutor.resumeTableWrite(primary, tableName);
            if (response.getCode() != StatusCode.OK) {
                log.warn("triggerRebalance resume writes failed table={} primary={} code={} msg={}",
                        tableName, primary.getId(), response.getCode(), response.getMessage());
            } else {
                log.info("triggerRebalance resume writes table={} primary={}", tableName, primary.getId());
            }
        } catch (Exception e) {
            log.warn("triggerRebalance resume writes exception table={} primary={} cause={}",
                    tableName, primary.getId(), e.getMessage());
        }
    }

    private TableLocation healTableLocationBestEffort(TableLocation location) {
        if (location == null
                || !location.isSetPrimaryRS()
                || !location.isSetReplicas()
                || location.getReplicas().isEmpty()) {
            return location;
        }
        if (!"ACTIVE".equalsIgnoreCase(location.getTableStatus())) {
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

        if (onlineIds.isEmpty()) {
            return location;
        }

        List<RegionServerInfo> onlineReplicas = new ArrayList<>();
        for (RegionServerInfo replica : location.getReplicas()) {
            if (replica != null && replica.isSetId() && onlineIds.contains(replica.getId())) {
                onlineReplicas.add(replica);
            }
        }

        if (onlineReplicas.isEmpty()) {
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
            for (RegionServerInfo candidate : loadBalancer.selectReplicas(candidates,
                    targetReplicaCount - healedReplicas.size())) {
                healedReplicas.add(candidate);
                changed = true;
            }
        }

        final String primaryId = newPrimary.getId();
        if (!healedReplicas.stream().anyMatch(rs -> rs.getId().equals(primaryId))) {
            healedReplicas.add(0, newPrimary);
            changed = true;
        }

        if (!changed) {
            return location;
        }

        TableLocation promotedLocation = new TableLocation(location.getTableName(), newPrimary, healedReplicas);
        promotedLocation.setVersion(System.currentTimeMillis());
        promotedLocation.setTableStatus(
                (location.isSetTableStatus() && !location.getTableStatus().isBlank())
                        ? location.getTableStatus()
                        : "ACTIVE");

        try {
            metaManager.saveTableLocation(promotedLocation);
            assignmentManager.saveAssignment(promotedLocation.getTableName(), healedReplicas);
            touchStatusUpdatedAtBestEffort(promotedLocation.getTableName());
            log.info("healTableLocation updated table={} primary={} replicas={}",
                    promotedLocation.getTableName(),
                    promotedLocation.getPrimaryRS().getId(),
                    healedReplicas.stream().map(RegionServerInfo::getId).toList());
            return promotedLocation;
        } catch (Exception e) {
            log.error("healTableLocation failed to persist update table={} primary={} cause={}",
                    promotedLocation.getTableName(), promotedLocation.getPrimaryRS().getId(), e.getMessage());
            return location;
        }
    }

    private String buildMigrationAttemptId(String tableName,
                                           RegionServerInfo source,
                                           RegionServerInfo target) {
        return tableName + "-" + source.getId() + "-" + target.getId() + "-" + System.currentTimeMillis();
    }

    @SuppressWarnings("unchecked")
    private void setMigrationAttemptIdBestEffort(String tableName, String migrationAttemptId) {
        if (migrationAttemptId == null || migrationAttemptId.isBlank() || zk() == null) {
            return;
        }
        try {
            String path = tableMetaPath(tableName);
            if (zk().checkExists().forPath(path) == null) {
                return;
            }
            byte[] data = zk().getData().forPath(path);
            if (data == null || data.length == 0) {
                return;
            }
            Map<String, Object> root = MAPPER.readValue(data, Map.class);
            if (!Objects.equals(root.get("migrationAttemptId"), migrationAttemptId)) {
                root.put("migrationAttemptId", migrationAttemptId);
                zk().setData().forPath(path, stringifyMap(root).getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.warn("triggerRebalance set migrationAttemptId failed table={} cause={}",
                    tableName, e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    private void clearMigrationAttemptIdBestEffort(String tableName) {
        if (zk() == null) {
            return;
        }
        try {
            String path = tableMetaPath(tableName);
            if (zk().checkExists().forPath(path) == null) {
                return;
            }
            byte[] data = zk().getData().forPath(path);
            if (data == null || data.length == 0) {
                return;
            }
            Map<String, Object> root = MAPPER.readValue(data, Map.class);
            if (root.remove("migrationAttemptId") != null) {
                zk().setData().forPath(path, stringifyMap(root).getBytes(StandardCharsets.UTF_8));
            }
        } catch (Exception e) {
            log.warn("triggerRebalance clear migrationAttemptId failed table={} cause={}",
                    tableName, e.getMessage());
        }
    }
}
