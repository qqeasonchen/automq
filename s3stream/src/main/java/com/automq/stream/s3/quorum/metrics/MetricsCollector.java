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

package com.automq.stream.s3.quorum.metrics;

import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Metrics collector that periodically gathers and reports S3 Quorum Storage metrics
 * Provides hooks for external monitoring systems
 */
public class MetricsCollector {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollector.class);
    
    // Default collection intervals
    private static final long DEFAULT_COLLECTION_INTERVAL_MS = 30000; // 30 seconds
    private static final long DEFAULT_HEALTH_CHECK_INTERVAL_MS = 5000; // 5 seconds
    
    private final QuorumMetrics metrics;
    private final QuorumState quorumState;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    // Configuration
    private final long collectionIntervalMs;
    private final long healthCheckIntervalMs;
    
    // Listeners for metrics events
    private final List<MetricsListener> listeners = new ArrayList<>();
    
    // Last collection state
    private volatile MetricsSnapshot lastSnapshot;
    private volatile long lastCollectionTime = 0;
    
    public MetricsCollector(QuorumMetrics metrics, QuorumState quorumState) {
        this(metrics, quorumState, DEFAULT_COLLECTION_INTERVAL_MS, DEFAULT_HEALTH_CHECK_INTERVAL_MS);
    }
    
    public MetricsCollector(QuorumMetrics metrics, QuorumState quorumState,
                           long collectionIntervalMs, long healthCheckIntervalMs) {
        this.metrics = metrics;
        this.quorumState = quorumState;
        this.collectionIntervalMs = collectionIntervalMs;
        this.healthCheckIntervalMs = healthCheckIntervalMs;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "MetricsCollector");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Start the metrics collector
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Starting metrics collector with collection interval: {}ms, health check interval: {}ms",
                       collectionIntervalMs, healthCheckIntervalMs);
            
            // Schedule periodic metrics collection
            scheduler.scheduleWithFixedDelay(this::collectMetrics,
                collectionIntervalMs, collectionIntervalMs, TimeUnit.MILLISECONDS);
            
            // Schedule periodic health updates
            scheduler.scheduleWithFixedDelay(this::updateHealthMetrics,
                healthCheckIntervalMs, healthCheckIntervalMs, TimeUnit.MILLISECONDS);
            
            // Initial collection
            collectMetrics();
            
            LOGGER.info("Metrics collector started successfully");
        }
    }
    
    /**
     * Stop the metrics collector
     */
    public void stop() {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Stopping metrics collector");
            
            // Final collection before shutdown
            collectMetrics();
            
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            
            LOGGER.info("Metrics collector stopped");
        }
    }
    
    /**
     * Add a metrics listener
     */
    public void addListener(MetricsListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }
    
    /**
     * Remove a metrics listener
     */
    public void removeListener(MetricsListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }
    
    /**
     * Get the current metrics snapshot
     */
    public MetricsSnapshot getCurrentSnapshot() {
        return metrics.getSnapshot();
    }
    
    /**
     * Get the last collected metrics snapshot
     */
    public MetricsSnapshot getLastSnapshot() {
        return lastSnapshot;
    }
    
    /**
     * Get collection statistics
     */
    public CollectionStats getCollectionStats() {
        return new CollectionStats(
            lastCollectionTime,
            System.currentTimeMillis() - lastCollectionTime,
            started.get(),
            listeners.size()
        );
    }
    
    /**
     * Force an immediate metrics collection
     */
    public MetricsSnapshot forceCollection() {
        collectMetrics();
        return lastSnapshot;
    }
    
    /**
     * Periodic metrics collection
     */
    private void collectMetrics() {
        try {
            long startTime = System.currentTimeMillis();
            
            // Get current snapshot
            MetricsSnapshot snapshot = metrics.getSnapshot();
            
            // Update last collection state
            lastSnapshot = snapshot;
            lastCollectionTime = startTime;
            
            // Notify listeners
            notifyListeners(snapshot);
            
            long collectionDuration = System.currentTimeMillis() - startTime;
            LOGGER.debug("Metrics collection completed in {}ms", collectionDuration);
            
            // Log summary if requested
            if (LOGGER.isInfoEnabled() && shouldLogSummary(snapshot)) {
                LOGGER.info("Metrics Summary: {}", snapshot.toString());
            }
            
        } catch (Exception e) {
            LOGGER.error("Error during metrics collection", e);
        }
    }
    
    /**
     * Update health metrics based on current quorum state
     */
    private void updateHealthMetrics() {
        try {
            int healthyCount = quorumState.getHealthyReplicaCount();
            boolean hasQuorum = quorumState.hasQuorum();
            
            metrics.updateQuorumHealth(healthyCount, hasQuorum);
            
        } catch (Exception e) {
            LOGGER.error("Error during health metrics update", e);
        }
    }
    
    /**
     * Notify all listeners of new metrics
     */
    private void notifyListeners(MetricsSnapshot snapshot) {
        synchronized (listeners) {
            for (MetricsListener listener : listeners) {
                try {
                    listener.onMetricsCollected(snapshot);
                } catch (Exception e) {
                    LOGGER.error("Error notifying metrics listener", e);
                }
            }
        }
    }
    
    /**
     * Determine if we should log a summary for this snapshot
     */
    private boolean shouldLogSummary(MetricsSnapshot snapshot) {
        // Log summary every 10 collections or if there are failures
        return (snapshot.getTotalRequests() % 100 == 0) ||
               (snapshot.getTotalFailedRequests() > 0) ||
               (!snapshot.isHealthy());
    }
    
    /**
     * Listener interface for metrics events
     */
    public interface MetricsListener {
        /**
         * Called when new metrics are collected
         */
        void onMetricsCollected(MetricsSnapshot snapshot);
        
        /**
         * Called when a health status change is detected
         */
        default void onHealthStatusChanged(boolean wasHealthy, boolean isHealthy, MetricsSnapshot snapshot) {
            // Default implementation does nothing
        }
        
        /**
         * Called when critical events occur
         */
        default void onCriticalEvent(String event, MetricsSnapshot snapshot) {
            // Default implementation does nothing
        }
    }
    
    /**
     * Enhanced metrics listener that detects health changes and critical events
     */
    public static class HealthAwareMetricsListener implements MetricsListener {
        private volatile Boolean lastHealthStatus = null;
        private final MetricsListener delegate;
        
        public HealthAwareMetricsListener(MetricsListener delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public void onMetricsCollected(MetricsSnapshot snapshot) {
            boolean currentHealth = snapshot.isHealthy();
            
            // Check for health status changes
            if (lastHealthStatus != null && lastHealthStatus != currentHealth) {
                delegate.onHealthStatusChanged(lastHealthStatus, currentHealth, snapshot);
                
                if (!currentHealth) {
                    delegate.onCriticalEvent("HEALTH_DEGRADED", snapshot);
                } else {
                    delegate.onCriticalEvent("HEALTH_RESTORED", snapshot);
                }
            }
            
            // Check for critical events
            if (snapshot.getQuorumLossEvents() > 0) {
                delegate.onCriticalEvent("QUORUM_LOSS", snapshot);
            }
            
            if (snapshot.getEmergencyRecoveriesTotal() > 0) {
                delegate.onCriticalEvent("EMERGENCY_RECOVERY", snapshot);
            }
            
            lastHealthStatus = currentHealth;
            delegate.onMetricsCollected(snapshot);
        }
    }
    
    /**
     * Collection statistics
     */
    public static class CollectionStats {
        private final long lastCollectionTime;
        private final long timeSinceLastCollection;
        private final boolean isRunning;
        private final int listenerCount;
        
        public CollectionStats(long lastCollectionTime, long timeSinceLastCollection,
                              boolean isRunning, int listenerCount) {
            this.lastCollectionTime = lastCollectionTime;
            this.timeSinceLastCollection = timeSinceLastCollection;
            this.isRunning = isRunning;
            this.listenerCount = listenerCount;
        }
        
        public long getLastCollectionTime() {
            return lastCollectionTime;
        }
        
        public long getTimeSinceLastCollection() {
            return timeSinceLastCollection;
        }
        
        public boolean isRunning() {
            return isRunning;
        }
        
        public int getListenerCount() {
            return listenerCount;
        }
        
        @Override
        public String toString() {
            return String.format(
                "CollectionStats{running=%s, lastCollection=%d, timeSince=%dms, listeners=%d}",
                isRunning, lastCollectionTime, timeSinceLastCollection, listenerCount
            );
        }
    }
}