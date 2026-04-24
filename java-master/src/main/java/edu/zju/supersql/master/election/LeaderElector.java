package edu.zju.supersql.master.election;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.zookeeper.data.Stat;
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
    private CuratorCache mastersWatcher;

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

        mastersWatcher = CuratorCache.build(zkClient, "/masters");
        mastersWatcher.listenable().addListener(CuratorCacheListener.builder()
                .forCreates(node -> log.info("/masters watcher event=CHILD_ADDED onlineMasters={}", countOnlineMasters()))
                .forDeletes(node -> log.info("/masters watcher event=CHILD_REMOVED onlineMasters={}", countOnlineMasters()))
                .forChanges((oldNode, newNode) -> log.info("/masters watcher event=CHILD_UPDATED onlineMasters={}", countOnlineMasters()))
                .build());
        mastersWatcher.start();
        log.info("/masters watcher initialized onlineMasters={}", countOnlineMasters());
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
            long nextEpoch = writeActiveMasterWithCas();
            log.info("Master {} became ACTIVE (epoch={})", masterAddress, nextEpoch);
        } catch (Exception e) {
            log.error("Failed to promote {} as active master", masterAddress, e);
        }
    }

    private long writeActiveMasterWithCas() throws Exception {
        int retries = 6;
        while (retries-- > 0) {
            Stat stat = new Stat();
            byte[] bytes = zkClient.getData().storingStatIn(stat).forPath("/active-master");
            long currentEpoch = parseEpoch(bytes);
            long nextEpoch = currentEpoch + 1;

            try {
                byte[] payload = buildActiveMasterPayload(nextEpoch);
                zkClient.setData().withVersion(stat.getVersion()).forPath("/active-master", payload);
                return nextEpoch;
            } catch (Exception e) {
                if (retries <= 0) {
                    throw e;
                }
                log.warn("CAS conflict when writing active-master, retrying: {}", e.getMessage());
            }
        }
        throw new IllegalStateException("Failed to write active-master by CAS");
    }

    private long parseEpoch(byte[] bytes) {
        try {
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
            log.warn("Parse active-master epoch failed: {}", e.getMessage());
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
        try {
            zkClient.create().creatingParentsIfNeeded().forPath(path, new byte[0]);
        } catch (org.apache.zookeeper.KeeperException.NodeExistsException ignore) {
            // Concurrent master created it first — that is the desired end state.
        }
    }

    private int countOnlineMasters() {
        try {
            return zkClient.getChildren().forPath("/masters").size();
        } catch (Exception e) {
            return 0;
        }
    }
}