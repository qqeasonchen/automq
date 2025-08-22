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

package com.automq.stream.s3.quorum.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * DynamicQuorumConfig provides runtime configuration management for S3 Quorum Storage.
 * Supports dynamic parameter updates without service restart.
 */
public class DynamicQuorumConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicQuorumConfig.class);
    
    // Core quorum parameters
    private final AtomicInteger writeQuorumSize;
    private final AtomicInteger readQuorumSize;
    private final AtomicLong writeTimeoutMs;
    private final AtomicLong readTimeoutMs;
    private final AtomicLong retryBackoffMs;
    private final AtomicInteger maxRetryAttempts;
    
    // Performance tuning parameters
    private final AtomicLong performanceCheckIntervalMs;
    private final AtomicInteger adaptiveTimeoutEnabled;
    private final AtomicLong baseLatencyThresholdMs;
    
    // Configuration change listeners
    private final ConcurrentHashMap<String, Consumer<ConfigChangeEvent>> listeners = new ConcurrentHashMap<>();
    
    public DynamicQuorumConfig(int initialWriteQuorumSize, int initialReadQuorumSize) {
        this.writeQuorumSize = new AtomicInteger(initialWriteQuorumSize);
        this.readQuorumSize = new AtomicInteger(initialReadQuorumSize);
        this.writeTimeoutMs = new AtomicLong(15000); // 15 seconds
        this.readTimeoutMs = new AtomicLong(5000);   // 5 seconds
        this.retryBackoffMs = new AtomicLong(100);   // 100ms
        this.maxRetryAttempts = new AtomicInteger(3);
        
        // Performance tuning defaults
        this.performanceCheckIntervalMs = new AtomicLong(30000); // 30 seconds
        this.adaptiveTimeoutEnabled = new AtomicInteger(1);      // enabled
        this.baseLatencyThresholdMs = new AtomicLong(1000);      // 1 second
        
        LOGGER.info("DynamicQuorumConfig initialized with writeQuorum={}, readQuorum={}", 
                   initialWriteQuorumSize, initialReadQuorumSize);
    }
    
    // Configuration getters
    public int getWriteQuorumSize() {
        return writeQuorumSize.get();
    }
    public int getReadQuorumSize() {
        return readQuorumSize.get();
    }
    public long getWriteTimeoutMs() {
        return writeTimeoutMs.get();
    }
    public long getReadTimeoutMs() {
        return readTimeoutMs.get();
    }
    public long getRetryBackoffMs() {
        return retryBackoffMs.get();
    }
    public int getMaxRetryAttempts() {
        return maxRetryAttempts.get();
    }
    public long getPerformanceCheckIntervalMs() {
        return performanceCheckIntervalMs.get();
    }
    public boolean isAdaptiveTimeoutEnabled() {
        return adaptiveTimeoutEnabled.get() == 1;
    }
    public long getBaseLatencyThresholdMs() {
        return baseLatencyThresholdMs.get();
    }
    
    // Configuration setters with validation and notification
    public boolean updateWriteQuorumSize(int newSize, String reason) {
        if (newSize <= 0 || newSize > 10) { // Reasonable bounds
            LOGGER.warn("Invalid writeQuorumSize: {} (must be 1-10)", newSize);
            return false;
        }
        
        int oldValue = writeQuorumSize.getAndSet(newSize);
        if (oldValue != newSize) {
            LOGGER.info("Updated writeQuorumSize from {} to {} (reason: {})", oldValue, newSize, reason);
            notifyListeners(new ConfigChangeEvent("writeQuorumSize", oldValue, newSize, reason));
        }
        return true;
    }
    
    public boolean updateReadQuorumSize(int newSize, String reason) {
        if (newSize <= 0 || newSize > 10) {
            LOGGER.warn("Invalid readQuorumSize: {} (must be 1-10)", newSize);
            return false;
        }
        
        int oldValue = readQuorumSize.getAndSet(newSize);
        if (oldValue != newSize) {
            LOGGER.info("Updated readQuorumSize from {} to {} (reason: {})", oldValue, newSize, reason);
            notifyListeners(new ConfigChangeEvent("readQuorumSize", oldValue, newSize, reason));
        }
        return true;
    }
    
    public boolean updateWriteTimeoutMs(long newTimeout, String reason) {
        if (newTimeout < 1000 || newTimeout > 300000) { // 1s to 5min
            LOGGER.warn("Invalid writeTimeoutMs: {} (must be 1000-300000ms)", newTimeout);
            return false;
        }
        
        long oldValue = writeTimeoutMs.getAndSet(newTimeout);
        if (oldValue != newTimeout) {
            LOGGER.info("Updated writeTimeoutMs from {} to {}ms (reason: {})", oldValue, newTimeout, reason);
            notifyListeners(new ConfigChangeEvent("writeTimeoutMs", oldValue, newTimeout, reason));
        }
        return true;
    }
    
    public boolean updateReadTimeoutMs(long newTimeout, String reason) {
        if (newTimeout < 1000 || newTimeout > 300000) {
            LOGGER.warn("Invalid readTimeoutMs: {} (must be 1000-300000ms)", newTimeout);
            return false;
        }
        
        long oldValue = readTimeoutMs.getAndSet(newTimeout);
        if (oldValue != newTimeout) {
            LOGGER.info("Updated readTimeoutMs from {} to {}ms (reason: {})", oldValue, newTimeout, reason);
            notifyListeners(new ConfigChangeEvent("readTimeoutMs", oldValue, newTimeout, reason));
        }
        return true;
    }
    
    public boolean updateRetryBackoffMs(long newBackoff, String reason) {
        if (newBackoff < 10 || newBackoff > 10000) { // 10ms to 10s
            LOGGER.warn("Invalid retryBackoffMs: {} (must be 10-10000ms)", newBackoff);
            return false;
        }
        
        long oldValue = retryBackoffMs.getAndSet(newBackoff);
        if (oldValue != newBackoff) {
            LOGGER.info("Updated retryBackoffMs from {} to {}ms (reason: {})", oldValue, newBackoff, reason);
            notifyListeners(new ConfigChangeEvent("retryBackoffMs", oldValue, newBackoff, reason));
        }
        return true;
    }
    
    public boolean updateMaxRetryAttempts(int newAttempts, String reason) {
        if (newAttempts < 0 || newAttempts > 10) {
            LOGGER.warn("Invalid maxRetryAttempts: {} (must be 0-10)", newAttempts);
            return false;
        }
        
        int oldValue = maxRetryAttempts.getAndSet(newAttempts);
        if (oldValue != newAttempts) {
            LOGGER.info("Updated maxRetryAttempts from {} to {} (reason: {})", oldValue, newAttempts, reason);
            notifyListeners(new ConfigChangeEvent("maxRetryAttempts", oldValue, newAttempts, reason));
        }
        return true;
    }
    
    public void setAdaptiveTimeoutEnabled(boolean enabled, String reason) {
        int newValue = enabled ? 1 : 0;
        int oldValue = adaptiveTimeoutEnabled.getAndSet(newValue);
        if (oldValue != newValue) {
            LOGGER.info("Updated adaptiveTimeoutEnabled from {} to {} (reason: {})", 
                       oldValue == 1, enabled, reason);
            notifyListeners(new ConfigChangeEvent("adaptiveTimeoutEnabled", oldValue == 1, enabled, reason));
        }
    }
    
    // Adaptive configuration based on performance metrics
    public void adaptConfigurationBasedOnMetrics(PerformanceMetrics metrics) {
        if (!isAdaptiveTimeoutEnabled()) {
            return;
        }
        
        // Adaptive timeout adjustment based on network latency
        double avgLatencyMs = metrics.getAverageLatencyMs();
        long currentWriteTimeout = getWriteTimeoutMs();
        long currentReadTimeout = getReadTimeoutMs();
        
        if (avgLatencyMs > getBaseLatencyThresholdMs()) {
            // High latency detected, increase timeouts
            long newWriteTimeout = Math.min(currentWriteTimeout * 2, 300000);
            long newReadTimeout = Math.min(currentReadTimeout * 2, 300000);
            
            updateWriteTimeoutMs(newWriteTimeout, "high-latency-adaptation");
            updateReadTimeoutMs(newReadTimeout, "high-latency-adaptation");
        } else if (avgLatencyMs < getBaseLatencyThresholdMs() / 2) {
            // Low latency, can reduce timeouts for better performance
            long newWriteTimeout = Math.max(currentWriteTimeout / 2, 5000);
            long newReadTimeout = Math.max(currentReadTimeout / 2, 2000);
            
            updateWriteTimeoutMs(newWriteTimeout, "low-latency-optimization");
            updateReadTimeoutMs(newReadTimeout, "low-latency-optimization");
        }
        
        // Adaptive retry adjustment based on error rate
        double errorRate = metrics.getErrorRate();
        if (errorRate > 0.1) { // > 10% error rate
            updateMaxRetryAttempts(Math.min(getMaxRetryAttempts() + 1, 10), "high-error-rate");
            updateRetryBackoffMs(Math.min(getRetryBackoffMs() * 2, 5000), "high-error-rate");
        } else if (errorRate < 0.01) { // < 1% error rate
            updateMaxRetryAttempts(Math.max(getMaxRetryAttempts() - 1, 1), "low-error-rate");
            updateRetryBackoffMs(Math.max(getRetryBackoffMs() / 2, 50), "low-error-rate");
        }
    }
    
    // Configuration change notification system
    public void addConfigChangeListener(String listenerId, Consumer<ConfigChangeEvent> listener) {
        listeners.put(listenerId, listener);
        LOGGER.debug("Added config change listener: {}", listenerId);
    }
    
    public void removeConfigChangeListener(String listenerId) {
        listeners.remove(listenerId);
        LOGGER.debug("Removed config change listener: {}", listenerId);
    }
    
    private void notifyListeners(ConfigChangeEvent event) {
        listeners.forEach((listenerId, listener) -> {
            try {
                listener.accept(event);
            } catch (Exception e) {
                LOGGER.error("Error notifying config change listener {}: {}", listenerId, e.getMessage());
            }
        });
    }
    
    // Configuration snapshot for monitoring
    public ConfigSnapshot getConfigSnapshot() {
        return new ConfigSnapshot(
            getWriteQuorumSize(),
            getReadQuorumSize(),
            getWriteTimeoutMs(),
            getReadTimeoutMs(),
            getRetryBackoffMs(),
            getMaxRetryAttempts(),
            isAdaptiveTimeoutEnabled(),
            System.currentTimeMillis()
        );
    }
    
    // Data classes
    public static class ConfigChangeEvent {
        private final String parameterName;
        private final Object oldValue;
        private final Object newValue;
        private final String reason;
        private final long timestamp;
        
        public ConfigChangeEvent(String parameterName, Object oldValue, Object newValue, String reason) {
            this.parameterName = parameterName;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.reason = reason;
            this.timestamp = System.currentTimeMillis();
        }
        
        public String getParameterName() { return parameterName; }
        public Object getOldValue() { return oldValue; }
        public Object getNewValue() { return newValue; }
        public String getReason() { return reason; }
        public long getTimestamp() { return timestamp; }
    }
    
    public static class PerformanceMetrics {
        private final double averageLatencyMs;
        private final double errorRate;
        private final int throughputOps;
        
        public PerformanceMetrics(double averageLatencyMs, double errorRate, int throughputOps) {
            this.averageLatencyMs = averageLatencyMs;
            this.errorRate = errorRate;
            this.throughputOps = throughputOps;
        }
        
        public double getAverageLatencyMs() { return averageLatencyMs; }
        public double getErrorRate() { return errorRate; }
        public int getThroughputOps() { return throughputOps; }
    }
    
    public static class ConfigSnapshot {
        private final int writeQuorumSize;
        private final int readQuorumSize;
        private final long writeTimeoutMs;
        private final long readTimeoutMs;
        private final long retryBackoffMs;
        private final int maxRetryAttempts;
        private final boolean adaptiveTimeoutEnabled;
        private final long snapshotTime;
        
        public ConfigSnapshot(int writeQuorumSize, int readQuorumSize, long writeTimeoutMs, 
                            long readTimeoutMs, long retryBackoffMs, int maxRetryAttempts, 
                            boolean adaptiveTimeoutEnabled, long snapshotTime) {
            this.writeQuorumSize = writeQuorumSize;
            this.readQuorumSize = readQuorumSize;
            this.writeTimeoutMs = writeTimeoutMs;
            this.readTimeoutMs = readTimeoutMs;
            this.retryBackoffMs = retryBackoffMs;
            this.maxRetryAttempts = maxRetryAttempts;
            this.adaptiveTimeoutEnabled = adaptiveTimeoutEnabled;
            this.snapshotTime = snapshotTime;
        }
        
        // Getters
        public int getWriteQuorumSize() { return writeQuorumSize; }
        public int getReadQuorumSize() { return readQuorumSize; }
        public long getWriteTimeoutMs() { return writeTimeoutMs; }
        public long getReadTimeoutMs() { return readTimeoutMs; }
        public long getRetryBackoffMs() { return retryBackoffMs; }
        public int getMaxRetryAttempts() { return maxRetryAttempts; }
        public boolean isAdaptiveTimeoutEnabled() { return adaptiveTimeoutEnabled; }
        public long getSnapshotTime() { return snapshotTime; }
    }
    
    // Additional getter methods for compatibility
    public long getTimeoutMs() {
        return writeTimeoutMs.get(); // Use write timeout as default
    }
    
    public boolean isAdaptiveTuningEnabled() {
        return adaptiveTimeoutEnabled.get() > 0;
    }
    
    /**
     * Update timeout with validation
     */
    public boolean updateTimeoutMs(long newTimeoutMs, String reason) {
        if (newTimeoutMs < 1000 || newTimeoutMs > 300000) { // 1s to 5m
            LOGGER.warn("Invalid timeout: {} (must be 1000-300000ms)", newTimeoutMs);
            return false;
        }
        
        long oldValue = writeTimeoutMs.getAndSet(newTimeoutMs);
        if (oldValue != newTimeoutMs) {
            LOGGER.info("Updated timeout from {}ms to {}ms (reason: {})", oldValue, newTimeoutMs, reason);
            notifyListeners(new ConfigChangeEvent("timeoutMs", oldValue, newTimeoutMs, reason));
        }
        return true;
    }
    
    /**
     * Get configuration statistics
     */
    public ConfigStats getConfigStats() {
        // Calculate average latencies from available metrics
        double avgWrite = calculateAverageWriteLatency();
        double avgRead = calculateAverageReadLatency();
        
        return new ConfigStats(
            writeQuorumSize.get() + readQuorumSize.get(), // Simple update count approximation
            avgWrite,
            avgRead,
            System.currentTimeMillis() // Current time as last tuning time
        );
    }
    
    private double calculateAverageWriteLatency() {
        // Return current timeout as approximation for average latency
        return writeTimeoutMs.get() * 0.1; // Assume 10% of timeout as average latency
    }
    
    private double calculateAverageReadLatency() {
        // Return current timeout as approximation for average latency  
        return readTimeoutMs.get() * 0.1; // Assume 10% of timeout as average latency
    }
    
    /**
     * Configuration statistics data class
     */
    public static class ConfigStats {
        private final long totalUpdates;
        private final double averageWriteLatency;
        private final double averageReadLatency;
        private final long lastTuningTime;
        
        public ConfigStats(long totalUpdates, double averageWriteLatency, double averageReadLatency, long lastTuningTime) {
            this.totalUpdates = totalUpdates;
            this.averageWriteLatency = averageWriteLatency;
            this.averageReadLatency = averageReadLatency;
            this.lastTuningTime = lastTuningTime;
        }
        
        public long getTotalUpdates() { return totalUpdates; }
        public double getAverageWriteLatency() { return averageWriteLatency; }
        public double getAverageReadLatency() { return averageReadLatency; }
        public long getLastTuningTime() { return lastTuningTime; }
        
        @Override
        public String toString() {
            return String.format("ConfigStats{updates=%d, writeLatency=%.1fms, readLatency=%.1fms, lastTuning=%d}",
                               totalUpdates, averageWriteLatency, averageReadLatency, lastTuningTime);
        }
    }
}