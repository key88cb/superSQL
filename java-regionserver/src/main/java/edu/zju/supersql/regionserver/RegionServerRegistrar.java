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
    static final String FIELD_REPLICA_COMMIT_TERMINAL_QUEUE_COUNT = "replicaCommitTerminalQueueCount";
    static final String FIELD_REPLICA_COMMIT_MANUAL_INTERVENTION_REQUIRED = "replicaCommitManualInterventionRequired";
    static final String FIELD_REPLICA_COMMIT_DECISION_TERMINAL_COUNT = "replicaCommitDecisionTerminalCount";
    static final String FIELD_REPLICA_COMMIT_LAST_DECISION_TERMINAL_AT_MS = "replicaCommitLastDecisionTerminalAtMs";

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

        byte[] payload = toPayload(info, httpPort, 0L, false, 0L, 0L);
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
                memUsage,
                0L,
                false,
                0L,
                0L);
    }

    public void heartbeat(String host,
                          int port,
                          int httpPort,
                          int tableCount,
                          double qps1min,
                          double cpuUsage,
                          double memUsage,
                          long terminalQueueCount,
                          boolean manualInterventionRequired,
                          long decisionTerminalCount,
                          long lastDecisionTerminalAtMs) {
        try {
            RegionServerInfo info = new RegionServerInfo(rsId, host, port);
            info.setTableCount(tableCount);
            info.setQps1min(qps1min);
            info.setCpuUsage(cpuUsage);
            info.setMemUsage(memUsage);
            info.setLastHeartbeat(System.currentTimeMillis());

            if (zkClient.checkExists().forPath(path) == null) {
                register(host, port, httpPort);
                return;
            }

            zkClient.setData().forPath(path,
                    toPayload(info,
                            httpPort,
                            terminalQueueCount,
                            manualInterventionRequired,
                            decisionTerminalCount,
                            lastDecisionTerminalAtMs));
        } catch (Exception e) {
            log.warn("Heartbeat update failed for {}: {}", rsId, e.getMessage());
        }
    }

    private byte[] toPayload(RegionServerInfo info,
                             int httpPort,
                             long terminalQueueCount,
                             boolean manualInterventionRequired,
                             long decisionTerminalCount,
                             long lastDecisionTerminalAtMs) throws Exception {
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
        map.put(FIELD_REPLICA_COMMIT_TERMINAL_QUEUE_COUNT, Math.max(0L, terminalQueueCount));
        map.put(FIELD_REPLICA_COMMIT_MANUAL_INTERVENTION_REQUIRED, manualInterventionRequired);
        map.put(FIELD_REPLICA_COMMIT_DECISION_TERMINAL_COUNT, Math.max(0L, decisionTerminalCount));
        map.put(FIELD_REPLICA_COMMIT_LAST_DECISION_TERMINAL_AT_MS, Math.max(0L, lastDecisionTerminalAtMs));
        return MAPPER.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
    }
}