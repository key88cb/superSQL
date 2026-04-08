package edu.zju.supersql.master.rpc;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.MasterRuntimeContext;
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
import java.util.List;
import java.util.Map;
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
        return "/meta/tables/" + tableName;
    }

    private static String assignmentPath(String tableName) {
        return "/assignments/" + tableName;
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
            String path = tableMetaPath(tableName);
            if (zk.checkExists().forPath(path) == null) {
                RegionServerInfo none = new RegionServerInfo("none", "0.0.0.0", 0);
                TableLocation location = new TableLocation(tableName, none, Collections.singletonList(none));
                location.setTableStatus("TABLE_NOT_FOUND");
                location.setVersion(-1L);
                return location;
            }
            return bytesToLocation(zk.getData().forPath(path), tableName);
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
            String tablePath = tableMetaPath(tableName);
            if (zk.checkExists().forPath(tablePath) != null) {
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

            rsList.sort(Comparator.comparingInt(r -> r.isSetTableCount() ? r.getTableCount() : 0));
            List<RegionServerInfo> replicas = new ArrayList<>();
            int replicaCount = Math.min(3, rsList.size());
            for (int i = 0; i < replicaCount; i++) {
                replicas.add(rsList.get(i));
            }

            RegionServerInfo primary = replicas.get(0);
            TableLocation location = new TableLocation(tableName, primary, replicas);
            location.setTableStatus("ACTIVE");
            location.setVersion(System.currentTimeMillis());

            zk.create().creatingParentsIfNeeded().forPath(tablePath, locationToBytes(location));

            Map<String, Object> assignment = new HashMap<>();
            assignment.put("tableName", tableName);
            assignment.put("replicas", replicas.stream().map(MasterServiceImpl::regionToMap).toList());
            byte[] assignmentBytes = stringifyMap(assignment).getBytes(StandardCharsets.UTF_8);
            String assignPath = assignmentPath(tableName);
            if (zk.checkExists().forPath(assignPath) == null) {
                zk.create().creatingParentsIfNeeded().forPath(assignPath, assignmentBytes);
            } else {
                zk.setData().forPath(assignPath, assignmentBytes);
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
        if (zk == null) {
            Response r = new Response(StatusCode.ERROR);
            r.setMessage("ZooKeeper is unavailable");
            return r;
        }

        try {
            String tablePath = tableMetaPath(tableName);
            if (zk.checkExists().forPath(tablePath) == null) {
                Response r = new Response(StatusCode.TABLE_NOT_FOUND);
                r.setMessage("Table not found: " + tableName);
                return r;
            }
            zk.delete().forPath(tablePath);

            String assignPath = assignmentPath(tableName);
            if (zk.checkExists().forPath(assignPath) != null) {
                zk.delete().forPath(assignPath);
            }

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
            List<String> children = zk.getChildren().forPath("/region_servers");
            List<RegionServerInfo> infos = new ArrayList<>();
            for (String child : children) {
                String path = "/region_servers/" + child;
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
        if (zk == null) {
            return Collections.emptyList();
        }

        try {
            List<String> tables = zk.getChildren().forPath("/meta/tables");
            List<TableLocation> locations = new ArrayList<>();
            for (String tableName : tables) {
                String path = tableMetaPath(tableName);
                byte[] bytes = zk.getData().forPath(path);
                if (bytes == null || bytes.length == 0) {
                    continue;
                }
                locations.add(bytesToLocation(bytes, tableName));
            }
            return locations;
        } catch (Exception e) {
            throw new TException("Failed to list table metadata", e);
        }
    }

    @Override
    public Response triggerRebalance() throws TException {
        return notImplemented("triggerRebalance");
    }
}
