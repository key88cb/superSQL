package edu.zju.supersql.regionserver;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.zju.supersql.rpc.RegionServerInfo;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Handles RegionServer registration and heartbeat updates in ZooKeeper.
 */
public class RegionServerRegistrar {

    private static final Logger log = LoggerFactory.getLogger(RegionServerRegistrar.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CuratorFramework zkClient;
    private final String rsId;
    private final String path;

    static final String FIELD_HTTP_PORT = "httpPort";

    public RegionServerRegistrar(CuratorFramework zkClient, String rsId) {
        this.zkClient = zkClient;
        this.rsId = rsId;
        this.path = "/region_servers/" + rsId;
    }

    public void register(String host, int port) throws Exception {
        register(host, port, port + 100);
    }

    public void register(String host, int port, int httpPort) throws Exception {
        RegionServerInfo info = new RegionServerInfo(rsId, host, port);
        info.setTableCount(0);
        info.setQps1min(0.0);
        info.setCpuUsage(0.0);
        info.setMemUsage(0.0);
        info.setLastHeartbeat(System.currentTimeMillis());

        byte[] payload = toPayload(info, httpPort);
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().withMode(org.apache.zookeeper.CreateMode.EPHEMERAL)
                    .forPath(path, payload);
            log.info("RegionServer registered in ZK: {}", path);
        } else {
            zkClient.setData().forPath(path, payload);
            log.info("RegionServer registration refreshed in ZK: {}", path);
        }
    }

    public void heartbeat(String host, int port, int tableCount, double qps1min, double cpuUsage, double memUsage) {
        heartbeat(host,
                port,
                port + 100,
                tableCount,
                qps1min,
                cpuUsage,
                memUsage);
    }

    public void heartbeat(String host,
                          int port,
                          int httpPort,
                          int tableCount,
                          double qps1min,
                          double cpuUsage,
                          double memUsage) {
        heartbeat(host, port, httpPort, tableCount, qps1min, cpuUsage, memUsage, null);
    }

    public void heartbeat(String host,
                          int port,
                          int httpPort,
                          int tableCount,
                          double qps1min,
                          double cpuUsage,
                          double memUsage,
                          Map<String, Object> replicaDecisionSignal) {
        try {
            RegionServerInfo info = new RegionServerInfo(rsId, host, port);
            info.setTableCount(tableCount);
            info.setQps1min(qps1min);
            info.setCpuUsage(cpuUsage);
            info.setMemUsage(memUsage);
            info.setLastHeartbeat(System.currentTimeMillis());
            byte[] payload = toPayload(info, httpPort, replicaDecisionSignal);

            if (zkClient.checkExists().forPath(path) == null) {
                zkClient.create().creatingParentsIfNeeded().withMode(org.apache.zookeeper.CreateMode.EPHEMERAL)
                        .forPath(path, payload);
                return;
            }

            zkClient.setData().forPath(path, payload);
        } catch (Exception e) {
            log.warn("Heartbeat update failed for {}: {}", rsId, e.getMessage());
        }
    }

    private byte[] toPayload(RegionServerInfo info, int httpPort) throws Exception {
        return toPayload(info, httpPort, null);
    }

    private byte[] toPayload(RegionServerInfo info,
                             int httpPort,
                             Map<String, Object> replicaDecisionSignal) throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("id", info.getId());
        map.put("host", info.getHost());
        map.put("port", info.getPort());
        map.put(FIELD_HTTP_PORT, httpPort);
        map.put("tableCount", info.getTableCount());
        map.put("qps1min", info.getQps1min());
        map.put("cpuUsage", info.getCpuUsage());
        map.put("memUsage", info.getMemUsage());
        map.put("lastHeartbeat", info.getLastHeartbeat());
        map.put("manualInterventionRequired", getBooleanSignal(replicaDecisionSignal, "manualInterventionRequired"));
        map.put("terminalQueueCount", getLongSignal(replicaDecisionSignal, "terminalQueueCount"));
        map.put("activeDecisionReadyCount", getLongSignal(replicaDecisionSignal, "activeDecisionReadyCount"));
        map.put("activeDecisionCandidateCount", getLongSignal(replicaDecisionSignal, "activeDecisionCandidateCount"));
        return MAPPER.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
    }

    private static long getLongSignal(Map<String, Object> signal, String key) {
        if (signal == null) {
            return 0L;
        }
        Object value = signal.get(key);
        if (value instanceof Number number) {
            return Math.max(0L, number.longValue());
        }
        return 0L;
    }

    private static boolean getBooleanSignal(Map<String, Object> signal, String key) {
        if (signal == null) {
            return false;
        }
        Object value = signal.get(key);
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value == null) {
            return false;
        }
        return "true".equalsIgnoreCase(String.valueOf(value));
    }
}
