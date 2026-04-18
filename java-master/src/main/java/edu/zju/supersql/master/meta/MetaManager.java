package edu.zju.supersql.master.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.ZkPaths;
import edu.zju.supersql.rpc.RegionServerInfo;
import edu.zju.supersql.rpc.TableLocation;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates table metadata persistence in ZooKeeper.
 */
public class MetaManager {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CuratorFramework zkClient;

    public MetaManager(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    public boolean isReady() {
        return zkClient != null;
    }

    public boolean tableExists(String tableName) throws Exception {
        if (!isReady()) {
            return false;
        }
        return zkClient.checkExists().forPath(ZkPaths.tableMeta(tableName)) != null;
    }

    public TableLocation getTableLocation(String tableName) throws Exception {
        if (!isReady() || !tableExists(tableName)) {
            return null;
        }
        return bytesToLocation(zkClient.getData().forPath(ZkPaths.tableMeta(tableName)), tableName);
    }

    public void saveTableLocation(TableLocation location) throws Exception {
        if (!isReady()) {
            return;
        }
        byte[] bytes = locationToBytes(location);
        String path = ZkPaths.tableMeta(location.getTableName());
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }

    @SuppressWarnings("unchecked")
    public void touchStatusUpdatedAt(String tableName) throws Exception {
        if (!isReady()) {
            return;
        }
        String path = ZkPaths.tableMeta(tableName);
        if (zkClient.checkExists().forPath(path) == null) {
            return;
        }
        byte[] data = zkClient.getData().forPath(path);
        if (data == null || data.length == 0) {
            return;
        }
        Map<String, Object> root = MAPPER.readValue(data, Map.class);
        root.put("statusUpdatedAt", System.currentTimeMillis());
        zkClient.setData().forPath(path, MAPPER.writeValueAsString(root).getBytes(StandardCharsets.UTF_8));
    }

    public void deleteTableLocation(String tableName) throws Exception {
        if (!isReady()) {
            return;
        }
        String path = ZkPaths.tableMeta(tableName);
        if (zkClient.checkExists().forPath(path) != null) {
            zkClient.delete().forPath(path);
        }
    }

    public List<TableLocation> listTables() throws Exception {
        if (!isReady()) {
            return Collections.emptyList();
        }
        List<String> tables = zkClient.getChildren().forPath(ZkPaths.META_TABLES);
        List<TableLocation> locations = new ArrayList<>();
        for (String tableName : tables) {
            byte[] bytes = zkClient.getData().forPath(ZkPaths.tableMeta(tableName));
            if (bytes == null || bytes.length == 0) {
                continue;
            }
            locations.add(bytesToLocation(bytes, tableName));
        }
        return locations;
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

        return MAPPER.writeValueAsString(root).getBytes(StandardCharsets.UTF_8);
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
        if (value instanceof Number number) {
            return number.intValue();
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
        if (value instanceof Number number) {
            return number.longValue();
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
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
