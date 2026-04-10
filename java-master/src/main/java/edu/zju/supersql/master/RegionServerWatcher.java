package edu.zju.supersql.master;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches {@code /region_servers} and emits basic lifecycle callbacks.
 */
public class RegionServerWatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RegionServerWatcher.class);

    interface Listener {
        default void onRegionServerUp(String rsId) {
        }

        default void onRegionServerUpdated(String rsId) {
        }

        default void onRegionServerDown(String rsId) {
        }
    }

    private final CuratorFramework zkClient;
    private final Listener listener;

    private CuratorCache cache;

    public RegionServerWatcher(CuratorFramework zkClient) {
        this(zkClient, new Listener() {
        });
    }

    RegionServerWatcher(CuratorFramework zkClient, Listener listener) {
        this.zkClient = zkClient;
        this.listener = listener;
    }

    public synchronized void start() {
        if (zkClient == null || cache != null) {
            return;
        }

        cache = CuratorCache.build(zkClient, ZkPaths.REGION_SERVERS);
        cache.listenable().addListener(CuratorCacheListener.builder()
                .forCreates(node -> {
                    String rsId = rsIdOf(node);
                    log.info("RegionServer watcher event=UP rsId={} online={}", rsId, countOnlineServers());
                    listener.onRegionServerUp(rsId);
                })
                .forChanges((oldNode, newNode) -> {
                    String rsId = rsIdOf(newNode);
                    log.info("RegionServer watcher event=UPDATED rsId={} online={}", rsId, countOnlineServers());
                    listener.onRegionServerUpdated(rsId);
                })
                .forDeletes(node -> {
                    String rsId = rsIdOf(node);
                    log.info("RegionServer watcher event=DOWN rsId={} online={}", rsId, countOnlineServers());
                    listener.onRegionServerDown(rsId);
                })
                .build());
        cache.start();
        log.info("RegionServer watcher initialized online={}", countOnlineServers());
    }

    @Override
    public synchronized void close() {
        if (cache != null) {
            cache.close();
            cache = null;
        }
    }

    private int countOnlineServers() {
        try {
            return zkClient.getChildren().forPath(ZkPaths.REGION_SERVERS).size();
        } catch (Exception e) {
            return 0;
        }
    }

    private static String rsIdOf(ChildData node) {
        if (node == null || node.getPath() == null || node.getPath().isBlank()) {
            return "unknown";
        }
        String path = node.getPath();
        int idx = path.lastIndexOf('/');
        return idx >= 0 ? path.substring(idx + 1) : path;
    }
}
