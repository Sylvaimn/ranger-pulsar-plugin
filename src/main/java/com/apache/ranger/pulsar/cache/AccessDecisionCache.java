package com.apache.ranger.pulsar.cache;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LRU 缓存，缓存权限决策结果以提升性能
 */
public class AccessDecisionCache {

    private final int maxSize;
    private final LinkedHashMap<String, Boolean> cache;
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);

    public AccessDecisionCache(int maxSize) {
        this.maxSize = maxSize;
        this.cache = new LinkedHashMap<String, Boolean>(maxSize, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                return size() > maxSize;
            }
        };
    }

    public Boolean get(String key) {
        synchronized (cache) {
            Boolean result = cache.get(key);
            if (result != null) {
                hitCount.incrementAndGet();
            } else {
                missCount.incrementAndGet();
            }
            return result;
        }
    }

    public void put(String key, boolean allowed) {
        synchronized (cache) {
            cache.put(key, allowed);
        }
    }

    public void invalidate(String key) {
        synchronized (cache) {
            cache.remove(key);
        }
    }

    public void clear() {
        synchronized (cache) {
            cache.clear();
            hitCount.set(0);
            missCount.set(0);
        }
    }

    public CacheStats getStats() {
        long hits = hitCount.get();
        long misses = missCount.get();
        long total = hits + misses;
        double hitRate = total == 0 ? 0.0 : (double) hits / total;
        int size;
        synchronized (cache) {
            size = cache.size();
        }
        return new CacheStats(size, hits, misses, hitRate);
    }

    public static String buildKey(String user, String accessType, String cluster,
                                   String namespace, String topic, String subscription) {
        return user + "|" + accessType + "|" + cluster + "|" + namespace + "|" + topic + "|" + subscription;
    }

    public static class CacheStats {
        public final int size;
        public final long hits;
        public final long misses;
        public final double hitRate;

        public CacheStats(int size, long hits, long misses, double hitRate) {
            this.size = size;
            this.hits = hits;
            this.misses = misses;
            this.hitRate = hitRate;
        }

        @Override
        public String toString() {
            return String.format("CacheStats{size=%d, hits=%d, misses=%d, hitRate=%.2f%%}", size, hits, misses, hitRate * 100);
        }
    }
}
