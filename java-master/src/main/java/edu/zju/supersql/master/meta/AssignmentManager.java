package edu.zju.supersql.master.meta;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.master.ZkPaths;
import edu.zju.supersql.rpc.RegionServerInfo;
import org.apache.curator.framework.CuratorFramework;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Encapsulates assignment persistence in ZooKeeper.
 */
public class AssignmentManager {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CuratorFramework zkClient;

    public AssignmentManager(CuratorFramework zkClient) {
        this.zkClient = zkClient;
    }

    public boolean isReady() {
        return zkClient != null;
    }

    public void saveAssignment(String tableName, List<RegionServerInfo> replicas) throws Exception {
        if (!isReady()) {
            return;
        }
        Map<String, Object> assignment = new HashMap<>();
        assignment.put("tableName", tableName);
        assignment.put("replicas", replicas.stream().map(AssignmentManager::regionToMap).toList());
        byte[] bytes = MAPPER.writeValueAsString(assignment).getBytes(StandardCharsets.UTF_8);
        String path = ZkPaths.assignment(tableName);
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, bytes);
        } else {
            zkClient.setData().forPath(path, bytes);
        }
    }

    public void deleteAssignment(String tableName) throws Exception {
        if (!isReady()) {
            return;
        }
        String path = ZkPaths.assignment(tableName);
        if (zkClient.checkExists().forPath(path) != null) {
            zkClient.delete().forPath(path);
        }
    }

    @SuppressWarnings("unchecked")
    public List<RegionServerInfo> getAssignment(String tableName) throws Exception {
        if (!isReady()) {
            return Collections.emptyList();
        }
        String path = ZkPaths.assignment(tableName);
        if (zkClient.checkExists().forPath(path) == null) {
            return Collections.emptyList();
        }
        byte[] bytes = zkClient.getData().forPath(path);
        if (bytes == null || bytes.length == 0) {
            return Collections.emptyList();
        }
        Map<String, Object> raw = MAPPER.readValue(bytes, Map.class);
        List<Map<String, Object>> replicasRaw = (List<Map<String, Object>>) raw.get("replicas");
        if (replicasRaw == null) {
            return Collections.emptyList();
        }
        List<RegionServerInfo> replicas = new ArrayList<>();
        for (Map<String, Object> item : replicasRaw) {
            replicas.add(mapToRegionServerInfo(item));
        }
        return replicas;
    }

    private static Map<String, Object> regionToMap(RegionServerInfo info) {
        Map<String, Object> map = new HashMap<>();
        map.put("id", info.getId());
        map.put("host", info.getHost());
        map.put("port", info.getPort());
        if (info.isSetTableCount()) {
            map.put("tableCount", info.getTableCount());
        }
        if (info.isSetQps1min()) {
            map.put("qps1min", info.getQps1min());
        }
        if (info.isSetCpuUsage()) {
            map.put("cpuUsage", info.getCpuUsage());
        }
        if (info.isSetMemUsage()) {
            map.put("memUsage", info.getMemUsage());
        }
        if (info.isSetLastHeartbeat()) {
            map.put("lastHeartbeat", info.getLastHeartbeat());
        }
        return map;
    }

    private static RegionServerInfo mapToRegionServerInfo(Map<?, ?> node) {
        RegionServerInfo info = new RegionServerInfo(
                valueOr(node.get("id"), "unknown"),
                valueOr(node.get("host"), "127.0.0.1"),
                toInt(node.get("port"), 0)
        );
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

    private static String valueOr(Object value, String fallback) {
        return value == null ? fallback : String.valueOf(value);
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
