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

package com.automq.stream.s3.quorum.failover;

import com.automq.stream.s3.operator.ObjectStorage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * AdvancedFailureDetector provides sophisticated failure detection capabilities
 * for S3 Quorum Storage replicas with multiple detection strategies.
 */
public class AdvancedFailureDetector {
    private static final Logger LOGGER = LoggerFactory.getLogger(AdvancedFailureDetector.class);
    
    private final List<ObjectStorage> replicas;
    private final Map<Integer, ReplicaHealthState> replicaHealthStates;
    private final FailureDetectionConfig config;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong probeSequence = new AtomicLong(0);
    private final List<FailureListener> failureListeners;
    private volatile boolean isRunning = false;
    
    public AdvancedFailureDetector(List<ObjectStorage> replicas, FailureDetectionConfig config) {
        this.replicas = replicas;
        this.config = config;
        this.replicaHealthStates = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "failure-detector-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });
        this.failureListeners = new ArrayList<>();
        
        // Initialize health states
        for (int i = 0; i < replicas.size(); i++) {
            replicaHealthStates.put(i, new ReplicaHealthState(i));
        }
        
        LOGGER.info("AdvancedFailureDetector initialized for {} replicas with config: {}", 
                   replicas.size(), config);
    }
    
    /**
     * Start failure detection
     */
    public void start() {
        if (isRunning) {
            LOGGER.warn("AdvancedFailureDetector is already running");
            return;
        }
        
        isRunning = true;
        
        // Schedule health probes
        scheduler.scheduleAtFixedRate(this::performHealthProbes, 
                                    config.getInitialDelay(), 
                                    config.getProbeInterval(), 
                                    TimeUnit.MILLISECONDS);
        
        // Schedule failure analysis
        scheduler.scheduleAtFixedRate(this::analyzeReplicaHealth, 
                                    config.getInitialDelay() * 2, 
                                    config.getAnalysisInterval(), 
                                    TimeUnit.MILLISECONDS);
        
        LOGGER.info("AdvancedFailureDetector started");
    }
    
    /**
     * Stop failure detection
     */
    public void stop() {
        if (!isRunning) {
            return;
        }
        
        isRunning = false;
        scheduler.shutdown();
        
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        
        LOGGER.info("AdvancedFailureDetector stopped");
    }
    
    /**
     * Add failure listener
     */
    public void addFailureListener(FailureListener listener) {
        failureListeners.add(listener);
    }
    
    /**
     * Get current health state of a replica
     */
    public ReplicaHealthState getReplicaHealth(int replicaIndex) {
        return replicaHealthStates.get(replicaIndex);
    }
    
    /**
     * Get health states of all replicas
     */
    public Map<Integer, ReplicaHealthState> getAllReplicaHealth() {
        return new ConcurrentHashMap<>(replicaHealthStates);
    }
    
    /**
     * Force a health probe for a specific replica
     */
    public CompletableFuture<HealthProbeResult> probeReplica(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= replicas.size()) {
            return CompletableFuture.completedFuture(
                new HealthProbeResult(replicaIndex, false, "Invalid replica index", 0, 0));
        }
        
        return performSingleHealthProbe(replicaIndex);
    }
    
    private void performHealthProbes() {
        if (!isRunning) {
            return;
        }
        
        LOGGER.debug("Performing health probes for {} replicas", replicas.size());
        
        List<CompletableFuture<HealthProbeResult>> probeFutures = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            probeFutures.add(performSingleHealthProbe(i));
        }
        
        // Wait for all probes to complete
        CompletableFuture.allOf(probeFutures.toArray(new CompletableFuture[0]))
            .whenComplete((result, throwable) -> {
                if (throwable != null) {
                    LOGGER.warn("Health probe batch failed: {}", throwable.getMessage());
                } else {
                    LOGGER.debug("Health probe batch completed for {} replicas", replicas.size());
                }
            });
    }
    
    private CompletableFuture<HealthProbeResult> performSingleHealthProbe(int replicaIndex) {
        long probeId = probeSequence.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        ObjectStorage replica = replicas.get(replicaIndex);
        ReplicaHealthState healthState = replicaHealthStates.get(replicaIndex);
        
        // Create a small probe object
        String probeKey = "health-probe-" + probeId + "-replica-" + replicaIndex;
        ByteBuf probeData = Unpooled.copiedBuffer(("probe-data-" + probeId).getBytes());
        
        return replica.write(new ObjectStorage.WriteOptions(), probeKey, probeData)
            .thenCompose(writeResult -> {
                // Immediately read back to verify
                return replica.read(new ObjectStorage.ReadOptions(), probeKey);
            })
            .thenCompose(readResult -> {
                // Clean up probe object
                List<ObjectStorage.ObjectPath> objectsToDelete = new ArrayList<>();
                objectsToDelete.add(new ObjectStorage.ObjectPath(replica.bucketId(), probeKey));
                return replica.delete(objectsToDelete).thenApply(v -> readResult);
            })
            .thenApply(readResult -> {
                long responseTime = System.currentTimeMillis() - startTime;
                boolean success = readResult != null && readResult.readableBytes() > 0;
                
                HealthProbeResult result = new HealthProbeResult(replicaIndex, success, 
                                                               success ? "OK" : "Read verification failed", 
                                                               responseTime, probeId);
                
                // Update health state
                healthState.recordProbeResult(result);
                
                LOGGER.debug("Health probe {} for replica {} completed: success={}, responseTime={}ms", 
                           probeId, replicaIndex, success, responseTime);
                
                return result;
            })
            .exceptionally(throwable -> {
                long responseTime = System.currentTimeMillis() - startTime;
                HealthProbeResult result = new HealthProbeResult(replicaIndex, false, 
                                                               throwable.getMessage(), responseTime, probeId);
                
                // Update health state
                healthState.recordProbeResult(result);
                
                LOGGER.debug("Health probe {} for replica {} failed: {}", 
                           probeId, replicaIndex, throwable.getMessage());
                
                return result;
            })
            .whenComplete((result, throwable) -> {
                // Always release probe data
                probeData.release();
            });
    }
    
    private void analyzeReplicaHealth() {
        if (!isRunning) {
            return;
        }
        
        LOGGER.debug("Analyzing replica health states");
        
        for (Map.Entry<Integer, ReplicaHealthState> entry : replicaHealthStates.entrySet()) {
            int replicaIndex = entry.getKey();
            ReplicaHealthState healthState = entry.getValue();
            
            HealthStatus previousStatus = healthState.getCurrentStatus();
            healthState.analyzeHealth(config);
            HealthStatus currentStatus = healthState.getCurrentStatus();
            
            // Check for status changes
            if (previousStatus != currentStatus) {
                LOGGER.info("Replica {} health status changed: {} -> {}", 
                           replicaIndex, previousStatus, currentStatus);
                
                // Notify listeners
                FailureEvent event = new FailureEvent(replicaIndex, previousStatus, currentStatus, 
                                                    healthState.getLastError(), System.currentTimeMillis());
                notifyFailureListeners(event);
            }
        }
    }
    
    private void notifyFailureListeners(FailureEvent event) {
        for (FailureListener listener : failureListeners) {
            try {
                listener.onFailureEvent(event);
            } catch (Exception e) {
                LOGGER.error("Error notifying failure listener: {}", e.getMessage(), e);
            }
        }
    }
    
    // Configuration class
    public static class FailureDetectionConfig {
        private final long probeInterval;
        private final long analysisInterval;
        private final long initialDelay;
        private final int failureThreshold;
        private final int recoveryThreshold;
        private final long failureTimeWindow;
        private final long maxResponseTime;
        
        public FailureDetectionConfig(long probeInterval, long analysisInterval, long initialDelay,
                                    int failureThreshold, int recoveryThreshold, 
                                    long failureTimeWindow, long maxResponseTime) {
            this.probeInterval = probeInterval;
            this.analysisInterval = analysisInterval;
            this.initialDelay = initialDelay;
            this.failureThreshold = failureThreshold;
            this.recoveryThreshold = recoveryThreshold;
            this.failureTimeWindow = failureTimeWindow;
            this.maxResponseTime = maxResponseTime;
        }
        
        public static FailureDetectionConfig defaultConfig() {
            return new FailureDetectionConfig(
                5000,   // probeInterval: 5 seconds
                10000,  // analysisInterval: 10 seconds
                1000,   // initialDelay: 1 second
                3,      // failureThreshold: 3 consecutive failures
                2,      // recoveryThreshold: 2 consecutive successes
                30000,  // failureTimeWindow: 30 seconds
                10000   // maxResponseTime: 10 seconds
            );
        }
        
        // Getters
        public long getProbeInterval() { return probeInterval; }
        public long getAnalysisInterval() { return analysisInterval; }
        public long getInitialDelay() { return initialDelay; }
        public int getFailureThreshold() { return failureThreshold; }
        public int getRecoveryThreshold() { return recoveryThreshold; }
        public long getFailureTimeWindow() { return failureTimeWindow; }
        public long getMaxResponseTime() { return maxResponseTime; }
        
        @Override
        public String toString() {
            return String.format("FailureDetectionConfig{probeInterval=%d, analysisInterval=%d, " +
                               "failureThreshold=%d, recoveryThreshold=%d}", 
                               probeInterval, analysisInterval, failureThreshold, recoveryThreshold);
        }
    }
    
    // Health probe result
    public static class HealthProbeResult {
        private final int replicaIndex;
        private final boolean success;
        private final String message;
        private final long responseTime;
        private final long probeId;
        
        public HealthProbeResult(int replicaIndex, boolean success, String message, 
                               long responseTime, long probeId) {
            this.replicaIndex = replicaIndex;
            this.success = success;
            this.message = message;
            this.responseTime = responseTime;
            this.probeId = probeId;
        }
        
        // Getters
        public int getReplicaIndex() { return replicaIndex; }
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public long getResponseTime() { return responseTime; }
        public long getProbeId() { return probeId; }
    }
    
    // Health status enum
    public enum HealthStatus {
        HEALTHY,
        DEGRADED,
        FAILED,
        RECOVERING,
        UNKNOWN
    }
    
    // Replica health state
    public static class ReplicaHealthState {
        private final int replicaIndex;
        private volatile HealthStatus currentStatus = HealthStatus.UNKNOWN;
        private final List<HealthProbeResult> recentProbes = new ArrayList<>();
        private volatile long lastProbeTime = 0;
        private volatile String lastError = "";
        private volatile int consecutiveFailures = 0;
        private volatile int consecutiveSuccesses = 0;
        private volatile long averageResponseTime = 0;
        
        public ReplicaHealthState(int replicaIndex) {
            this.replicaIndex = replicaIndex;
        }
        
        public synchronized void recordProbeResult(HealthProbeResult result) {
            recentProbes.add(result);
            lastProbeTime = System.currentTimeMillis();
            
            if (result.isSuccess()) {
                consecutiveSuccesses++;
                consecutiveFailures = 0;
                lastError = "";
            } else {
                consecutiveFailures++;
                consecutiveSuccesses = 0;
                lastError = result.getMessage();
            }
            
            // Keep only recent probes (last 50)
            if (recentProbes.size() > 50) {
                recentProbes.remove(0);
            }
            
            // Update average response time
            updateAverageResponseTime();
        }
        
        public synchronized void analyzeHealth(FailureDetectionConfig config) {
            long now = System.currentTimeMillis();
            
            if (lastProbeTime == 0 || (now - lastProbeTime) > config.getFailureTimeWindow() * 2) {
                currentStatus = HealthStatus.UNKNOWN;
                return;
            }
            
            if (consecutiveFailures >= config.getFailureThreshold()) {
                currentStatus = HealthStatus.FAILED;
            } else if (consecutiveSuccesses >= config.getRecoveryThreshold() && 
                      currentStatus == HealthStatus.FAILED) {
                currentStatus = HealthStatus.RECOVERING;
            } else if (consecutiveSuccesses >= config.getRecoveryThreshold()) {
                if (averageResponseTime > config.getMaxResponseTime()) {
                    currentStatus = HealthStatus.DEGRADED;
                } else {
                    currentStatus = HealthStatus.HEALTHY;
                }
            }
        }
        
        private void updateAverageResponseTime() {
            if (recentProbes.isEmpty()) {
                return;
            }
            
            long totalResponseTime = recentProbes.stream()
                .filter(HealthProbeResult::isSuccess)
                .mapToLong(HealthProbeResult::getResponseTime)
                .sum();
            
            long successCount = recentProbes.stream()
                .filter(HealthProbeResult::isSuccess)
                .count();
            
            if (successCount > 0) {
                averageResponseTime = totalResponseTime / successCount;
            }
        }
        
        // Getters
        public int getReplicaIndex() { return replicaIndex; }
        public HealthStatus getCurrentStatus() { return currentStatus; }
        public long getLastProbeTime() { return lastProbeTime; }
        public String getLastError() { return lastError; }
        public int getConsecutiveFailures() { return consecutiveFailures; }
        public int getConsecutiveSuccesses() { return consecutiveSuccesses; }
        public long getAverageResponseTime() { return averageResponseTime; }
        public synchronized List<HealthProbeResult> getRecentProbes() { 
            return new ArrayList<>(recentProbes); 
        }
    }
    
    // Failure event
    public static class FailureEvent {
        private final int replicaIndex;
        private final HealthStatus previousStatus;
        private final HealthStatus currentStatus;
        private final String errorMessage;
        private final long timestamp;
        
        public FailureEvent(int replicaIndex, HealthStatus previousStatus, HealthStatus currentStatus,
                           String errorMessage, long timestamp) {
            this.replicaIndex = replicaIndex;
            this.previousStatus = previousStatus;
            this.currentStatus = currentStatus;
            this.errorMessage = errorMessage;
            this.timestamp = timestamp;
        }
        
        // Getters
        public int getReplicaIndex() { return replicaIndex; }
        public HealthStatus getPreviousStatus() { return previousStatus; }
        public HealthStatus getCurrentStatus() { return currentStatus; }
        public String getErrorMessage() { return errorMessage; }
        public long getTimestamp() { return timestamp; }
        
        public boolean isFailure() {
            return currentStatus == HealthStatus.FAILED || currentStatus == HealthStatus.DEGRADED;
        }
        
        public boolean isRecovery() {
            return currentStatus == HealthStatus.HEALTHY || currentStatus == HealthStatus.RECOVERING;
        }
    }
    
    // Failure listener interface
    public interface FailureListener {
        void onFailureEvent(FailureEvent event);
    }
}