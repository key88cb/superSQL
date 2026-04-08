package edu.zju.supersql.master.election;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Leader election based on Curator LeaderLatch.
 */
public class LeaderElector implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LeaderElector.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final CuratorFramework zkClient;
    private final String masterId;
    private final String masterAddress;

    private LeaderLatch leaderLatch;
    private PathChildrenCache mastersWatcher;

    public LeaderElector(CuratorFramework zkClient, String masterId, String masterAddress) {
        this.zkClient = zkClient;
        this.masterId = masterId;
        this.masterAddress = masterAddress;
    }

    public synchronized void start() throws Exception {
        if (leaderLatch != null) {
            return;
        }

        ensurePath("/masters");
        ensurePath("/active-master");

        leaderLatch = new LeaderLatch(zkClient, "/masters", masterAddress);
        leaderLatch.addListener(new LeaderLatchListener() {
            @Override
            public void isLeader() {
                onElectedLeader();
            }

            @Override
            public void notLeader() {
                log.info("Master {} switched to STANDBY", masterAddress);
            }
        });

        mastersWatcher = new PathChildrenCache(zkClient, "/masters", true);
        mastersWatcher.getListenable().addListener((client, event) -> {
            int onlineMasters = mastersWatcher.getCurrentData().size();
            log.info("/masters watcher event={} onlineMasters={}", event.getType(), onlineMasters);
        });

        mastersWatcher.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        leaderLatch.start();
        log.info("LeaderElector started for {}", masterAddress);
    }

    public boolean isLeader() {
        return leaderLatch != null && leaderLatch.hasLeadership();
    }

    @Override
    public synchronized void close() {
        if (mastersWatcher != null) {
            try {
                mastersWatcher.close();
            } catch (Exception e) {
                log.warn("Close mastersWatcher failed: {}", e.getMessage());
            }
            mastersWatcher = null;
        }

        if (leaderLatch != null) {
            try {
                leaderLatch.close();
            } catch (Exception e) {
                log.warn("Close leaderLatch failed: {}", e.getMessage());
            }
            leaderLatch = null;
        }
    }

    private void onElectedLeader() {
        try {
            long nextEpoch = readCurrentEpoch() + 1;
            byte[] payload = buildActiveMasterPayload(nextEpoch);
            zkClient.setData().forPath("/active-master", payload);
            log.info("Master {} became ACTIVE (epoch={})", masterAddress, nextEpoch);
        } catch (Exception e) {
            log.error("Failed to promote {} as active master", masterAddress, e);
        }
    }

    private long readCurrentEpoch() {
        try {
            byte[] bytes = zkClient.getData().forPath("/active-master");
            if (bytes == null || bytes.length == 0) {
                return 0L;
            }
            Map<?, ?> map = MAPPER.readValue(bytes, Map.class);
            Object epoch = map.get("epoch");
            if (epoch instanceof Number) {
                return ((Number) epoch).longValue();
            }
            if (epoch != null) {
                return Long.parseLong(String.valueOf(epoch));
            }
        } catch (Exception e) {
            log.warn("Read current epoch failed: {}", e.getMessage());
        }
        return 0L;
    }

    private byte[] buildActiveMasterPayload(long epoch) throws Exception {
        Map<String, Object> payload = new HashMap<>();
        payload.put("epoch", epoch);
        payload.put("masterId", masterId);
        payload.put("address", masterAddress);
        payload.put("ts", System.currentTimeMillis());
        return MAPPER.writeValueAsString(payload).getBytes(StandardCharsets.UTF_8);
    }

    private void ensurePath(String path) throws Exception {
        if (zkClient.checkExists().forPath(path) == null) {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        }
    }
}