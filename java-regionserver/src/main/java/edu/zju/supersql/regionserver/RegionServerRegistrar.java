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

    public RegionServerRegistrar(CuratorFramework zkClient, String rsId) {
        this.zkClient = zkClient;
        this.rsId = rsId;
        this.path = "/region_servers/" + rsId;
    }

    public void register(String host, int port) throws Exception {
        RegionServerInfo info = new RegionServerInfo(rsId, host, port);
        info.setTableCount(0);
        info.setQps1min(0.0);
        info.setCpuUsage(0.0);
        info.setMemUsage(0.0);
        info.setLastHeartbeat(System.currentTimeMillis());

        byte[] payload = toPayload(info);
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
        try {
            RegionServerInfo info = new RegionServerInfo(rsId, host, port);
            info.setTableCount(tableCount);
            info.setQps1min(qps1min);
            info.setCpuUsage(cpuUsage);
            info.setMemUsage(memUsage);
            info.setLastHeartbeat(System.currentTimeMillis());

            if (zkClient.checkExists().forPath(path) == null) {
                register(host, port);
                return;
            }

            zkClient.setData().forPath(path, toPayload(info));
        } catch (Exception e) {
            log.warn("Heartbeat update failed for {}: {}", rsId, e.getMessage());
        }
    }

    private byte[] toPayload(RegionServerInfo info) throws Exception {
        Map<String, Object> map = new HashMap<>();
        map.put("id", info.getId());
        map.put("host", info.getHost());
        map.put("port", info.getPort());
        map.put("tableCount", info.getTableCount());
        map.put("qps1min", info.getQps1min());
        map.put("cpuUsage", info.getCpuUsage());
        map.put("memUsage", info.getMemUsage());
        map.put("lastHeartbeat", info.getLastHeartbeat());
        return MAPPER.writeValueAsString(map).getBytes(StandardCharsets.UTF_8);
    }
}