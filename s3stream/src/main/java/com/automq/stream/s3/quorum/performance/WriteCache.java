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

import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.model.StreamRecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Write-through cache for S3 Quorum Storage writes
 * Caches recent writes to improve read performance for recently written data
 */
public class WriteCache {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(WriteCache.class);
    
    private final int maxEntriesPerStream;
    private final long maxMemoryBytes;
    private final long entryTtlMs;
    
    // Stream-based caching: each stream has its own cache
    private final ConcurrentHashMap<Long, StreamWriteCache> streamCaches = new ConcurrentHashMap<>();
    private final AtomicLong totalMemoryUsage = new AtomicLong(0);
    private final AtomicLong totalCacheHits = new AtomicLong(0);
    private final AtomicLong totalCacheMisses = new AtomicLong(0);
    private final AtomicLong totalWritesRecorded = new AtomicLong(0);
    
    private final ScheduledExecutorService cleanupExecutor;
    
    public WriteCache(int maxEntriesPerStream, long maxMemoryBytes, long entryTtlMs) {
        this.maxEntriesPerStream = maxEntriesPerStream;
        this.maxMemoryBytes = maxMemoryBytes;
        this.entryTtlMs = entryTtlMs;
        
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "write-cache-cleanup");
            t.setDaemon(true);
            return t;
        });
        
        // Schedule periodic cleanup
        cleanupExecutor.scheduleWithFixedDelay(
            this::performCleanup,
            entryTtlMs / 4,
            entryTtlMs / 4,
            TimeUnit.MILLISECONDS
        );
        
        LOGGER.info("Write cache initialized with maxEntriesPerStream={}, maxMemory={}bytes, ttl={}ms",
                   maxEntriesPerStream, maxMemoryBytes, entryTtlMs);
    }
    
    /**
     * Record a write operation in the cache
     */
    public void recordWrite(AppendContext context, StreamRecordBatch streamRecord) {
        long streamId = streamRecord.getStreamId();
        
        StreamWriteCache streamCache = streamCaches.computeIfAbsent(streamId, 
            id -> new StreamWriteCache(id, maxEntriesPerStream));
        
        streamCache.recordWrite(context, streamRecord);
        totalWritesRecorded.incrementAndGet();
        
        LOGGER.debug("Recorded write in cache for stream={}, offset={}", 
                   streamId, streamRecord.getBaseOffset());
        
        // Evict if memory usage is too high
        evictIfNecessary();
    }
    
    /**
     * Try to get cached data for a read operation
     */
    public StreamRecordBatch getCachedData(long streamId, long startOffset, long endOffset) {
        StreamWriteCache streamCache = streamCaches.get(streamId);
        if (streamCache == null) {
            totalCacheMisses.incrementAndGet();
            return null;
        }
        
        StreamRecordBatch cachedData = streamCache.getCachedData(startOffset, endOffset);
        if (cachedData != null) {
            totalCacheHits.incrementAndGet();
            LOGGER.debug("Cache hit for stream={}, range=[{}, {})", streamId, startOffset, endOffset);
        } else {
            totalCacheMisses.incrementAndGet();
        }
        
        return cachedData;
    }
    
    /**
     * Invalidate cached data for a specific stream
     */
    public void invalidateStream(long streamId) {
        StreamWriteCache removed = streamCaches.remove(streamId);
        if (removed != null) {
            totalMemoryUsage.addAndGet(-removed.getMemoryUsage());
            LOGGER.debug("Invalidated write cache for stream={}", streamId);
        }
    }
    
    /**
     * Clear all cached data
     */
    public void clear() {
        streamCaches.clear();
        totalMemoryUsage.set(0);
        LOGGER.info("Write cache cleared");
    }
    
    /**
     * Get cache statistics
     */
    public WriteCacheStats getStats() {
        int totalEntries = streamCaches.values().stream()
            .mapToInt(StreamWriteCache::getEntryCount)
            .sum();
        
        return new WriteCacheStats(
            streamCaches.size(),
            totalEntries,
            totalMemoryUsage.get(),
            totalCacheHits.get(),
            totalCacheMisses.get(),
            totalWritesRecorded.get(),
            maxEntriesPerStream,
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
        LOGGER.info("Write cache shutdown completed");
    }
    
    private void evictIfNecessary() {
        while (totalMemoryUsage.get() > maxMemoryBytes && !streamCaches.isEmpty()) {
            // Find the stream cache with the oldest access time
            StreamWriteCache oldestCache = null;
            long oldestAccessTime = Long.MAX_VALUE;
            
            for (StreamWriteCache streamCache : streamCaches.values()) {
                long lastAccessTime = streamCache.getLastAccessTime();
                if (lastAccessTime < oldestAccessTime) {
                    oldestAccessTime = lastAccessTime;
                    oldestCache = streamCache;
                }
            }
            
            if (oldestCache != null) {
                StreamWriteCache removed = streamCaches.remove(oldestCache.getStreamId());
                if (removed != null) {
                    totalMemoryUsage.addAndGet(-removed.getMemoryUsage());
                    LOGGER.debug("Evicted write cache for stream={} due to memory pressure", 
                               oldestCache.getStreamId());
                }
            } else {
                break; // Should not happen, but prevent infinite loop
            }
        }
    }
    
    private void performCleanup() {
        long currentTime = System.currentTimeMillis();
        int removedStreamCaches = 0;
        long freedMemory = 0;
        
        // Remove expired stream caches
        var iterator = streamCaches.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            StreamWriteCache streamCache = entry.getValue();
            
            // Remove expired entries within each stream cache
            long streamFreedMemory = streamCache.removeExpiredEntries(currentTime, entryTtlMs);
            totalMemoryUsage.addAndGet(-streamFreedMemory);
            
            // Remove empty stream caches
            if (streamCache.isEmpty()) {
                iterator.remove();
                totalMemoryUsage.addAndGet(-streamCache.getMemoryUsage());
                freedMemory += streamCache.getMemoryUsage();
                removedStreamCaches++;
            } else {
                freedMemory += streamFreedMemory;
            }
        }
        
        if (removedStreamCaches > 0 || freedMemory > 0) {
            LOGGER.debug("Cleanup removed {} stream caches and freed {} bytes", 
                       removedStreamCaches, freedMemory);
        }
    }
    
    /**
     * Per-stream write cache
     */
    private static class StreamWriteCache {
        private final long streamId;
        private final int maxEntries;
        private final List<CachedWriteEntry> entries = new ArrayList<>();
        private final ReadWriteLock lock = new ReentrantReadWriteLock();
        private volatile long lastAccessTime = System.currentTimeMillis();
        private long memoryUsage = 0;
        
        StreamWriteCache(long streamId, int maxEntries) {
            this.streamId = streamId;
            this.maxEntries = maxEntries;
        }
        
        void recordWrite(AppendContext context, StreamRecordBatch streamRecord) {
            long estimatedSize = streamRecord.encoded().readableBytes();
            CachedWriteEntry entry = new CachedWriteEntry(context, streamRecord, estimatedSize);
            
            lock.writeLock().lock();
            try {
                entries.add(entry);
                memoryUsage += estimatedSize;
                lastAccessTime = System.currentTimeMillis();
                
                // Evict oldest entries if we exceed max entries
                while (entries.size() > maxEntries) {
                    CachedWriteEntry removed = entries.remove(0);
                    memoryUsage -= removed.estimatedSize;
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        
        StreamRecordBatch getCachedData(long startOffset, long endOffset) {
            lock.readLock().lock();
            try {
                lastAccessTime = System.currentTimeMillis();
                
                // Look for an entry that covers the requested range
                for (CachedWriteEntry entry : entries) {
                    StreamRecordBatch record = entry.streamRecord;
                    long recordStart = record.getBaseOffset();
                    long recordEnd = recordStart + record.getLastOffset() - record.getBaseOffset();
                    
                    if (recordStart <= startOffset && recordEnd >= endOffset) {
                        return record;
                    }
                }
                
                return null;
            } finally {
                lock.readLock().unlock();
            }
        }
        
        long removeExpiredEntries(long currentTime, long ttlMs) {
            long freedMemory = 0;
            
            lock.writeLock().lock();
            try {
                var iterator = entries.iterator();
                while (iterator.hasNext()) {
                    CachedWriteEntry entry = iterator.next();
                    if (currentTime - entry.creationTime > ttlMs) {
                        iterator.remove();
                        memoryUsage -= entry.estimatedSize;
                        freedMemory += entry.estimatedSize;
                    }
                }
            } finally {
                lock.writeLock().unlock();
            }
            
            return freedMemory;
        }
        
        boolean isEmpty() {
            lock.readLock().lock();
            try {
                return entries.isEmpty();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        int getEntryCount() {
            lock.readLock().lock();
            try {
                return entries.size();
            } finally {
                lock.readLock().unlock();
            }
        }
        
        long getMemoryUsage() {
            return memoryUsage;
        }
        
        long getLastAccessTime() {
            return lastAccessTime;
        }
        
        long getStreamId() {
            return streamId;
        }
    }
    
    /**
     * Cached write entry
     */
    private static class CachedWriteEntry {
        final AppendContext context;
        final StreamRecordBatch streamRecord;
        final long estimatedSize;
        final long creationTime;
        
        CachedWriteEntry(AppendContext context, StreamRecordBatch streamRecord, long estimatedSize) {
            this.context = context;
            this.streamRecord = streamRecord;
            this.estimatedSize = estimatedSize;
            this.creationTime = System.currentTimeMillis();
        }
    }
    
    /**
     * Statistics about write cache performance
     */
    public static class WriteCacheStats {
        private final int streamCount;
        private final int totalEntries;
        private final long memoryUsage;
        private final long totalHits;
        private final long totalMisses;
        private final long totalWrites;
        private final int maxEntriesPerStream;
        private final long maxMemoryBytes;
        private final long entryTtlMs;
        
        public WriteCacheStats(int streamCount, int totalEntries, long memoryUsage, long totalHits,
                              long totalMisses, long totalWrites, int maxEntriesPerStream,
                              long maxMemoryBytes, long entryTtlMs) {
            this.streamCount = streamCount;
            this.totalEntries = totalEntries;
            this.memoryUsage = memoryUsage;
            this.totalHits = totalHits;
            this.totalMisses = totalMisses;
            this.totalWrites = totalWrites;
            this.maxEntriesPerStream = maxEntriesPerStream;
            this.maxMemoryBytes = maxMemoryBytes;
            this.entryTtlMs = entryTtlMs;
        }
        
        public int getStreamCount() {
            return streamCount;
        }
        
        public int getTotalEntries() {
            return totalEntries;
        }
        
        public long getMemoryUsage() {
            return memoryUsage;
        }
        
        public long getTotalHits() {
            return totalHits;
        }
        
        public long getTotalMisses() {
            return totalMisses;
        }
        
        public long getTotalWrites() {
            return totalWrites;
        }
        
        public int getMaxEntriesPerStream() {
            return maxEntriesPerStream;
        }
        
        public long getMaxMemoryBytes() {
            return maxMemoryBytes;
        }
        
        public long getEntryTtlMs() {
            return entryTtlMs;
        }
        
        public double getHitRate() {
            long totalRequests = totalHits + totalMisses;
            return totalRequests > 0 ? (double) totalHits / totalRequests : 0.0;
        }
        
        public double getMemoryUtilization() {
            return maxMemoryBytes > 0 ? (double) memoryUsage / maxMemoryBytes : 0.0;
        }
        
        public double getAverageEntriesPerStream() {
            return streamCount > 0 ? (double) totalEntries / streamCount : 0.0;
        }
        
        @Override
        public String toString() {
            return String.format(
                "WriteCacheStats{streams=%d, entries=%d, memory=%d/%d bytes, hits=%d, misses=%d, " +
                "writes=%d, hitRate=%.2f%%, memUtil=%.1f%%, avgEntriesPerStream=%.1f}",
                streamCount, totalEntries, memoryUsage, maxMemoryBytes, totalHits, totalMisses,
                totalWrites, getHitRate() * 100, getMemoryUtilization() * 100, 
                getAverageEntriesPerStream()
            );
        }
    }
}