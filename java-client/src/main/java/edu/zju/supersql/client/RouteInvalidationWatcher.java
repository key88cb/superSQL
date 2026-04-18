package edu.zju.supersql.client;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches table metadata updates and actively invalidates local route cache.
 */
final class RouteInvalidationWatcher implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(RouteInvalidationWatcher.class);

    private final CuratorCache cache;
    private final RouteCache routeCache;

    private RouteInvalidationWatcher(CuratorCache cache, RouteCache routeCache) {
        this.cache = cache;
        this.routeCache = routeCache;
    }

    static RouteInvalidationWatcher start(CuratorFramework zkClient, RouteCache routeCache) {
        CuratorCache curatorCache = CuratorCache.build(zkClient, ZkPaths.META_TABLES);
        RouteInvalidationWatcher watcher = new RouteInvalidationWatcher(curatorCache, routeCache);
        watcher.registerListeners();
        curatorCache.start();
        return watcher;
    }

    static String extractTableNameFromMetaPath(String path) {
        if (path == null) {
            return null;
        }
        String prefix = ZkPaths.META_TABLES + "/";
        if (!path.startsWith(prefix)) {
            return null;
        }
        String tableName = path.substring(prefix.length());
        if (tableName.isBlank() || tableName.contains("/")) {
            return null;
        }
        return tableName;
    }

    private void registerListeners() {
        CuratorCacheListener listener = CuratorCacheListener.builder()
                .forCreates(node -> invalidateByPath(node != null ? node.getPath() : null))
                .forChanges((oldNode, node) -> invalidateByPath(node != null ? node.getPath() : null))
                .forDeletes(node -> invalidateByPath(node != null ? node.getPath() : null))
                .build();
        cache.listenable().addListener(listener);
    }

    private void invalidateByPath(String path) {
        String tableName = extractTableNameFromMetaPath(path);
        if (tableName == null) {
            return;
        }
        routeCache.invalidate(tableName);
        log.info("Route cache actively invalidated from zk event: table={}", tableName);
    }

    @Override
    public void close() {
        cache.close();
    }
}
