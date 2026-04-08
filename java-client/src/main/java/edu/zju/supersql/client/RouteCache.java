package edu.zju.supersql.client;

import edu.zju.supersql.rpc.TableLocation;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Client route cache with TTL and version-based invalidation.
 */
public class RouteCache {

    private static class CachedLocation {
        private final TableLocation location;
        private final long expiresAtMs;

        CachedLocation(TableLocation location, long expiresAtMs) {
            this.location = location;
            this.expiresAtMs = expiresAtMs;
        }
    }

    private final long ttlMs;
    private final ConcurrentHashMap<String, CachedLocation> cache = new ConcurrentHashMap<>();

    public RouteCache(long ttlMs) {
        this.ttlMs = ttlMs;
    }

    public TableLocation get(String tableName) {
        CachedLocation cached = cache.get(tableName);
        if (cached == null) {
            return null;
        }
        if (System.currentTimeMillis() > cached.expiresAtMs) {
            cache.remove(tableName);
            return null;
        }
        return cached.location;
    }

    public void put(String tableName, TableLocation location) {
        cache.put(tableName, new CachedLocation(location, System.currentTimeMillis() + ttlMs));
    }

    public void invalidate(String tableName) {
        cache.remove(tableName);
    }

    public void invalidateIfVersionMismatch(String tableName, long newVersion) {
        CachedLocation cached = cache.get(tableName);
        if (cached == null || cached.location == null || !cached.location.isSetVersion()) {
            return;
        }
        if (cached.location.getVersion() != newVersion) {
            cache.remove(tableName);
        }
    }
}