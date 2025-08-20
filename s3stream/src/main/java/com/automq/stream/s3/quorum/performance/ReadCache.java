/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.stream.s3.quorum.performance;

import com.automq.stream.s3.cache.ReadDataBlock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * LRU-based read cache for S3 Quorum Storage
 * Caches frequently accessed read data to improve read performance
 */
public class ReadCache {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReadCache.class);
    
    private final int maxEntries;
    private final long maxMemoryBytes;
    private final long entryTtlMs;
    
    private final ConcurrentHashMap<CacheKey, CacheEntry> cache = new ConcurrentHashMap<>();
    private final AtomicLong currentMemoryUsage = new AtomicLong(0);
    private final AtomicLong totalHits = new AtomicLong(0);
    private final AtomicLong totalMisses = new AtomicLong(0);
    private final AtomicLong totalEvictions = new AtomicLong(0);
    
    private final ScheduledExecutorService cleanupExecutor;
    
    public ReadCache(int maxEntries, long maxMemoryBytes, long entryTtlMs) {
        this.maxEntries = maxEntries;
        this.maxMemoryBytes = maxMemoryBytes;
        this.entryTtlMs = entryTtlMs;
        
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "read-cache-cleanup");
            t.setDaemon(true);
            return t;
        });
        
        // Schedule periodic cleanup
        cleanupExecutor.scheduleWithFixedDelay(
            this::performCleanup,
            entryTtlMs / 4, // Cleanup more frequently than TTL
            entryTtlMs / 4,
            TimeUnit.MILLISECONDS
        );
        
        LOGGER.info("Read cache initialized with maxEntries={}, maxMemory={}bytes, ttl={}ms",
                   maxEntries, maxMemoryBytes, entryTtlMs);
    }
    
    /**
     * Get cached data for the given key
     */
    public ReadDataBlock get(long streamId, long startOffset, long endOffset) {
        CacheKey key = new CacheKey(streamId, startOffset, endOffset);
        CacheEntry entry = cache.get(key);
        
        if (entry == null) {
            totalMisses.incrementAndGet();
            return null;
        }
        
        // Check if entry is expired
        if (System.currentTimeMillis() - entry.creationTime > entryTtlMs) {
            cache.remove(key);
            currentMemoryUsage.addAndGet(-entry.estimatedSize);
            totalMisses.incrementAndGet();
            return null;
        }
        
        // Update access time for LRU
        entry.lastAccessTime = System.currentTimeMillis();
        totalHits.incrementAndGet();
        
        LOGGER.debug("Cache hit for stream={}, range=[{}, {})", streamId, startOffset, endOffset);
        return entry.data;
    }
    
    /**
     * Put data into the cache
     */
    public void put(long streamId, long startOffset, long endOffset, ReadDataBlock data) {
        if (data == null) {
            return;
        }
        
        CacheKey key = new CacheKey(streamId, startOffset, endOffset);
        long estimatedSize = estimateDataSize(data);
        
        // Check if we can fit this entry
        if (estimatedSize > maxMemoryBytes) {
            LOGGER.debug("Entry too large for cache: {} bytes > {} bytes", estimatedSize, maxMemoryBytes);
            return;
        }
        
        CacheEntry entry = new CacheEntry(data, estimatedSize);
        
        // Add to cache
        CacheEntry existing = cache.put(key, entry);
        if (existing != null) {
            // Replace existing entry
            currentMemoryUsage.addAndGet(estimatedSize - existing.estimatedSize);
        } else {
            currentMemoryUsage.addAndGet(estimatedSize);
        }
        
        LOGGER.debug("Cached data for stream={}, range=[{}, {}), size={} bytes", 
                   streamId, startOffset, endOffset, estimatedSize);
        
        // Evict entries if necessary
        evictIfNecessary();
    }
    
    /**
     * Invalidate cached data for a specific stream
     */
    public void invalidateStream(long streamId) {
        cache.entrySet().removeIf(entry -> {
            if (entry.getKey().streamId == streamId) {
                currentMemoryUsage.addAndGet(-entry.getValue().estimatedSize);
                return true;
            }
            return false;
        });
        
        LOGGER.debug("Invalidated cache entries for stream={}", streamId);
    }
    
    /**
     * Clear all cached data
     */
    public void clear() {
        cache.clear();
        currentMemoryUsage.set(0);
        LOGGER.info("Read cache cleared");
    }
    
    /**
     * Get cache statistics
     */
    public ReadCacheStats getStats() {
        return new ReadCacheStats(
            cache.size(),
            currentMemoryUsage.get(),
            totalHits.get(),
            totalMisses.get(),
            totalEvictions.get(),
            maxEntries,
            maxMemoryBytes,
            entryTtlMs
        );
    }
    
    /**
     * Shutdown the cache and cleanup resources
     */
    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cleanupExecutor.shutdownNow();
        }
        
        clear();
        LOGGER.info("Read cache shutdown completed");
    }
    
    private void evictIfNecessary() {
        // Evict by count
        while (cache.size() > maxEntries) {
            evictLeastRecentlyUsed();
        }
        
        // Evict by memory
        while (currentMemoryUsage.get() > maxMemoryBytes) {
            evictLeastRecentlyUsed();
        }
    }
    
    private void evictLeastRecentlyUsed() {
        if (cache.isEmpty()) {
            return;
        }
        
        // Find the least recently used entry
        CacheKey lruKey = null;
        long oldestAccessTime = Long.MAX_VALUE;
        
        for (var entry : cache.entrySet()) {
            long accessTime = entry.getValue().lastAccessTime;
            if (accessTime < oldestAccessTime) {
                oldestAccessTime = accessTime;
                lruKey = entry.getKey();
            }
        }
        
        if (lruKey != null) {
            CacheEntry removed = cache.remove(lruKey);
            if (removed != null) {
                currentMemoryUsage.addAndGet(-removed.estimatedSize);
                totalEvictions.incrementAndGet();
                
                LOGGER.debug("Evicted LRU cache entry: stream={}, range=[{}, {})", 
                           lruKey.streamId, lruKey.startOffset, lruKey.endOffset);
            }
        }
    }
    
    private void performCleanup() {
        long currentTime = System.currentTimeMillis();
        int removedCount = 0;
        long freedMemory = 0;
        
        // Remove expired entries
        var iterator = cache.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            CacheEntry cacheEntry = entry.getValue();
            
            if (currentTime - cacheEntry.creationTime > entryTtlMs) {
                iterator.remove();
                currentMemoryUsage.addAndGet(-cacheEntry.estimatedSize);
                freedMemory += cacheEntry.estimatedSize;
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            LOGGER.debug("Cleanup removed {} expired entries, freed {} bytes", removedCount, freedMemory);
        }
    }
    
    private long estimateDataSize(ReadDataBlock data) {
        // Estimate size based on the number of records and a typical record size
        // In a real implementation, this would be more accurate
        return data.getRecords().size() * 200L; // Assume ~200 bytes per record
    }
    
    /**
     * Cache key for identifying cached entries
     */
    private static class CacheKey {
        final long streamId;
        final long startOffset;
        final long endOffset;
        
        CacheKey(long streamId, long startOffset, long endOffset) {
            this.streamId = streamId;
            this.startOffset = startOffset;
            this.endOffset = endOffset;
        }
        
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return streamId == cacheKey.streamId &&
                   startOffset == cacheKey.startOffset &&
                   endOffset == cacheKey.endOffset;
        }
        
        @Override
        public int hashCode() {
            return Long.hashCode(streamId) ^ Long.hashCode(startOffset) ^ Long.hashCode(endOffset);
        }
        
        @Override
        public String toString() {
            return String.format("CacheKey{stream=%d, range=[%d, %d)}", streamId, startOffset, endOffset);
        }
    }
    
    /**
     * Cache entry containing the cached data and metadata
     */
    private static class CacheEntry {
        final ReadDataBlock data;
        final long estimatedSize;
        final long creationTime;
        volatile long lastAccessTime;
        
        CacheEntry(ReadDataBlock data, long estimatedSize) {
            this.data = data;
            this.estimatedSize = estimatedSize;
            this.creationTime = System.currentTimeMillis();
            this.lastAccessTime = this.creationTime;
        }
    }
    
    /**
     * Statistics about read cache performance
     */
    public static class ReadCacheStats {
        private final int entryCount;
        private final long memoryUsage;
        private final long totalHits;
        private final long totalMisses;
        private final long totalEvictions;
        private final int maxEntries;
        private final long maxMemoryBytes;
        private final long entryTtlMs;
        
        public ReadCacheStats(int entryCount, long memoryUsage, long totalHits, long totalMisses,
                             long totalEvictions, int maxEntries, long maxMemoryBytes, long entryTtlMs) {
            this.entryCount = entryCount;
            this.memoryUsage = memoryUsage;
            this.totalHits = totalHits;
            this.totalMisses = totalMisses;
            this.totalEvictions = totalEvictions;
            this.maxEntries = maxEntries;
            this.maxMemoryBytes = maxMemoryBytes;
            this.entryTtlMs = entryTtlMs;
        }
        
        public int getEntryCount() { return entryCount; }
        public long getMemoryUsage() { return memoryUsage; }
        public long getTotalHits() { return totalHits; }
        public long getTotalMisses() { return totalMisses; }
        public long getTotalEvictions() { return totalEvictions; }
        public int getMaxEntries() { return maxEntries; }
        public long getMaxMemoryBytes() { return maxMemoryBytes; }
        public long getEntryTtlMs() { return entryTtlMs; }
        
        public double getHitRate() {
            long totalRequests = totalHits + totalMisses;
            return totalRequests > 0 ? (double) totalHits / totalRequests : 0.0;
        }
        
        public double getMemoryUtilization() {
            return maxMemoryBytes > 0 ? (double) memoryUsage / maxMemoryBytes : 0.0;
        }
        
        public double getEntryUtilization() {
            return maxEntries > 0 ? (double) entryCount / maxEntries : 0.0;
        }
        
        @Override
        public String toString() {
            return String.format(
                "ReadCacheStats{entries=%d/%d, memory=%d/%d bytes, hits=%d, misses=%d, " +
                "evictions=%d, hitRate=%.2f%%, memUtil=%.1f%%, entryUtil=%.1f%%}",
                entryCount, maxEntries, memoryUsage, maxMemoryBytes, totalHits, totalMisses,
                totalEvictions, getHitRate() * 100, getMemoryUtilization() * 100, 
                getEntryUtilization() * 100
            );
        }
    }
}