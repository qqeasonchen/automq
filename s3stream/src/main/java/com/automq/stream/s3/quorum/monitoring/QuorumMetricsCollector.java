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

package com.automq.stream.s3.quorum.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.Map;

/**
 * QuorumMetricsCollector provides comprehensive performance monitoring and metrics collection
 * for S3 Quorum Storage operations.
 */
public class QuorumMetricsCollector {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumMetricsCollector.class);
    
    private final Map<String, OperationMetrics> operationMetrics = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> gaugeMetrics = new ConcurrentHashMap<>();
    private final ScheduledExecutorService metricsReporter;
    private final MetricsConfig config;
    private volatile boolean isRunning = false;
    
    public QuorumMetricsCollector(MetricsConfig config) {
        this.config = config;
        this.metricsReporter = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "quorum-metrics-reporter");
            t.setDaemon(true);
            return t;
        });
        
        LOGGER.info("QuorumMetricsCollector initialized with config: {}", config);
    }
    
    /**
     * Start metrics collection and reporting
     */
    public void start() {
        if (isRunning) {
            LOGGER.warn("QuorumMetricsCollector is already running");
            return;
        }
        
        isRunning = true;
        
        if (config.isReportingEnabled()) {
            metricsReporter.scheduleAtFixedRate(
                this::reportMetrics,
                config.getReportingIntervalSeconds(),
                config.getReportingIntervalSeconds(),
                TimeUnit.SECONDS
            );
        }
        
        LOGGER.info("QuorumMetricsCollector started");
    }
    
    /**
     * Stop metrics collection and reporting
     */
    public void stop() {
        if (!isRunning) {
            return;
        }
        
        isRunning = false;
        metricsReporter.shutdown();
        
        try {
            if (!metricsReporter.awaitTermination(5, TimeUnit.SECONDS)) {
                metricsReporter.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            metricsReporter.shutdownNow();
        }
        
        LOGGER.info("QuorumMetricsCollector stopped");
    }
    
    /**
     * Record the start of an operation
     */
    public OperationTimer startOperation(String operationName) {
        return new OperationTimer(operationName, System.nanoTime());
    }
    
    /**
     * Record operation completion
     */
    public void recordOperation(String operationName, long durationNanos, boolean success) {
        OperationMetrics metrics = operationMetrics.computeIfAbsent(operationName, k -> new OperationMetrics());
        metrics.recordOperation(durationNanos, success);
    }
    
    /**
     * Record operation success
     */
    public void recordSuccess(String operationName, long durationNanos) {
        recordOperation(operationName, durationNanos, true);
    }
    
    /**
     * Record operation failure
     */
    public void recordFailure(String operationName, long durationNanos) {
        recordOperation(operationName, durationNanos, false);
    }
    
    /**
     * Set a gauge metric value
     */
    public void setGauge(String metricName, long value) {
        gaugeMetrics.computeIfAbsent(metricName, k -> new AtomicLong()).set(value);
    }
    
    /**
     * Increment a gauge metric
     */
    public void incrementGauge(String metricName) {
        gaugeMetrics.computeIfAbsent(metricName, k -> new AtomicLong()).incrementAndGet();
    }
    
    /**
     * Decrement a gauge metric
     */
    public void decrementGauge(String metricName) {
        gaugeMetrics.computeIfAbsent(metricName, k -> new AtomicLong()).decrementAndGet();
    }
    
    /**
     * Get operation metrics
     */
    public OperationMetrics getOperationMetrics(String operationName) {
        return operationMetrics.get(operationName);
    }
    
    /**
     * Get gauge metric value
     */
    public long getGaugeValue(String metricName) {
        AtomicLong gauge = gaugeMetrics.get(metricName);
        return gauge != null ? gauge.get() : 0;
    }
    
    /**
     * Get all metrics as a snapshot
     */
    public MetricsSnapshot getMetricsSnapshot() {
        Map<String, OperationMetrics> operationSnapshot = new ConcurrentHashMap<>(operationMetrics);
        Map<String, Long> gaugeSnapshot = new ConcurrentHashMap<>();
        
        gaugeMetrics.forEach((key, value) -> gaugeSnapshot.put(key, value.get()));
        
        return new MetricsSnapshot(operationSnapshot, gaugeSnapshot, System.currentTimeMillis());
    }
    
    /**
     * Clear all metrics
     */
    public void clearMetrics() {
        operationMetrics.clear();
        gaugeMetrics.clear();
        LOGGER.info("All metrics cleared");
    }
    
    /**
     * Report metrics to log
     */
    private void reportMetrics() {
        if (!isRunning) {
            return;
        }
        
        try {
            MetricsSnapshot snapshot = getMetricsSnapshot();
            
            LOGGER.info("=== S3 Quorum Storage Metrics Report ===");
            LOGGER.info("Timestamp: {}", snapshot.getTimestamp());
            
            // Report operation metrics
            snapshot.getOperationMetrics().forEach((operation, metrics) -> {
                LOGGER.info("Operation {}: count={}, success={}, failure={}, avg={}ms, min={}ms, max={}ms, successRate={:.2f}%",
                           operation,
                           metrics.getTotalCount(),
                           metrics.getSuccessCount(),
                           metrics.getFailureCount(),
                           metrics.getAverageDurationMs(),
                           metrics.getMinDurationMs(),
                           metrics.getMaxDurationMs(),
                           metrics.getSuccessRate() * 100);
            });
            
            // Report gauge metrics
            snapshot.getGaugeMetrics().forEach((gauge, value) -> {
                LOGGER.info("Gauge {}: {}", gauge, value);
            });
            
            LOGGER.info("=== End Metrics Report ===");
            
        } catch (Exception e) {
            LOGGER.error("Error reporting metrics", e);
        }
    }
    
    /**
     * Operation timer for measuring durations
     */
    public class OperationTimer implements AutoCloseable {
        private final String operationName;
        private final long startTime;
        
        public OperationTimer(String operationName, long startTime) {
            this.operationName = operationName;
            this.startTime = startTime;
        }
        
        /**
         * Complete the operation successfully
         */
        public void success() {
            long duration = System.nanoTime() - startTime;
            recordSuccess(operationName, duration);
        }
        
        /**
         * Complete the operation with failure
         */
        public void failure() {
            long duration = System.nanoTime() - startTime;
            recordFailure(operationName, duration);
        }
        
        @Override
        public void close() {
            // Default to success if not explicitly set
            success();
        }
    }
    
    /**
     * Metrics for a specific operation
     */
    public static class OperationMetrics {
        private final LongAdder totalCount = new LongAdder();
        private final LongAdder successCount = new LongAdder();
        private final LongAdder failureCount = new LongAdder();
        private final LongAdder totalDurationNanos = new LongAdder();
        private volatile long minDurationNanos = Long.MAX_VALUE;
        private volatile long maxDurationNanos = 0;
        
        public void recordOperation(long durationNanos, boolean success) {
            totalCount.increment();
            totalDurationNanos.add(durationNanos);
            
            if (success) {
                successCount.increment();
            } else {
                failureCount.increment();
            }
            
            // Update min/max durations
            updateMinDuration(durationNanos);
            updateMaxDuration(durationNanos);
        }
        
        private synchronized void updateMinDuration(long duration) {
            if (duration < minDurationNanos) {
                minDurationNanos = duration;
            }
        }
        
        private synchronized void updateMaxDuration(long duration) {
            if (duration > maxDurationNanos) {
                maxDurationNanos = duration;
            }
        }
        
        // Getters
        public long getTotalCount() {
            return totalCount.sum();
        }
        
        public long getSuccessCount() {
            return successCount.sum();
        }
        
        public long getFailureCount() {
            return failureCount.sum();
        }
        
        public long getTotalDurationNanos() {
            return totalDurationNanos.sum();
        }
        
        public long getMinDurationNanos() {
            return minDurationNanos == Long.MAX_VALUE ? 0 : minDurationNanos;
        }
        
        public long getMaxDurationNanos() {
            return maxDurationNanos;
        }
        
        public double getAverageDurationMs() {
            long count = getTotalCount();
            return count > 0 ? (getTotalDurationNanos() / (double) count) / 1_000_000.0 : 0.0;
        }
        
        public long getMinDurationMs() {
            return getMinDurationNanos() / 1_000_000;
        }
        
        public long getMaxDurationMs() {
            return getMaxDurationNanos() / 1_000_000;
        }
        
        public double getSuccessRate() {
            long total = getTotalCount();
            return total > 0 ? (double) getSuccessCount() / total : 0.0;
        }
    }
    
    /**
     * Snapshot of all metrics at a point in time
     */
    public static class MetricsSnapshot {
        private final Map<String, OperationMetrics> operationMetrics;
        private final Map<String, Long> gaugeMetrics;
        private final long timestamp;
        
        public MetricsSnapshot(Map<String, OperationMetrics> operationMetrics, 
                             Map<String, Long> gaugeMetrics, 
                             long timestamp) {
            this.operationMetrics = operationMetrics;
            this.gaugeMetrics = gaugeMetrics;
            this.timestamp = timestamp;
        }
        
        public Map<String, OperationMetrics> getOperationMetrics() {
            return operationMetrics;
        }
        
        public Map<String, Long> getGaugeMetrics() {
            return gaugeMetrics;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
    }
    
    /**
     * Configuration for metrics collection
     */
    public static class MetricsConfig {
        private final boolean reportingEnabled;
        private final int reportingIntervalSeconds;
        private final boolean detailedTimingEnabled;
        
        public MetricsConfig(boolean reportingEnabled, int reportingIntervalSeconds, boolean detailedTimingEnabled) {
            this.reportingEnabled = reportingEnabled;
            this.reportingIntervalSeconds = reportingIntervalSeconds;
            this.detailedTimingEnabled = detailedTimingEnabled;
        }
        
        public static MetricsConfig defaultConfig() {
            return new MetricsConfig(true, 60, true);
        }
        
        public boolean isReportingEnabled() {
            return reportingEnabled;
        }
        
        public int getReportingIntervalSeconds() {
            return reportingIntervalSeconds;
        }
        
        public boolean isDetailedTimingEnabled() {
            return detailedTimingEnabled;
        }
        
        @Override
        public String toString() {
            return String.format("MetricsConfig{reporting=%s, interval=%ds, detailedTiming=%s}",
                               reportingEnabled, reportingIntervalSeconds, detailedTimingEnabled);
        }
    }
    
    /**
     * Get comprehensive metrics summary
     */
    public MetricsSummary getMetricsSummary() {
        long totalOps = 0;
        long successOps = 0;
        long totalDurationNanos = 0;
        
        // Aggregate metrics from all operations
        for (OperationMetrics metrics : operationMetrics.values()) {
            totalOps += metrics.getTotalCount();
            successOps += metrics.getSuccessCount();
            totalDurationNanos += metrics.getTotalDurationNanos();
        }
        
        double successRate = totalOps > 0 ? (double) successOps / totalOps : 1.0;
        double avgDuration = totalOps > 0 ? (double) totalDurationNanos / totalOps / 1_000_000.0 : 0.0;
        
        return new MetricsSummary(totalOps, successOps, successRate, avgDuration);
    }
    
    /**
     * Metrics summary data class
     */
    public static class MetricsSummary {
        private final long totalOperations;
        private final long successfulOperations;
        private final double successRate;
        private final double averageOperationDuration;
        
        public MetricsSummary(long totalOperations, long successfulOperations, double successRate, double averageOperationDuration) {
            this.totalOperations = totalOperations;
            this.successfulOperations = successfulOperations;
            this.successRate = successRate;
            this.averageOperationDuration = averageOperationDuration;
        }
        
        public long getTotalOperations() { return totalOperations; }
        public long getSuccessfulOperations() { return successfulOperations; }
        public double getSuccessRate() { return successRate; }
        public double getAverageOperationDuration() { return averageOperationDuration; }
        
        @Override
        public String toString() {
            return String.format("MetricsSummary{total=%d, successful=%d, successRate=%.1f%%, avgDuration=%.1fms}",
                               totalOperations, successfulOperations, successRate * 100, averageOperationDuration);
        }
    }
}