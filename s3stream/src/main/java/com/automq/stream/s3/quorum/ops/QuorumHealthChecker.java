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

package com.automq.stream.s3.quorum.ops;

import com.automq.stream.s3.operator.ObjectStorage;
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
 * QuorumHealthChecker provides comprehensive health monitoring and diagnostics
 * for S3 Quorum Storage components.
 */
public class QuorumHealthChecker {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumHealthChecker.class);
    
    private final List<ObjectStorage> replicas;
    private final HealthConfig config;
    private final Map<String, ReplicaHealthStatus> replicaHealth;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong checkCounter;
    private volatile boolean running;
    
    public QuorumHealthChecker(List<ObjectStorage> replicas, HealthConfig config) {
        this.replicas = replicas;
        this.config = config;
        this.replicaHealth = new ConcurrentHashMap<>();
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "quorum-health-checker");
            t.setDaemon(true);
            return t;
        });
        this.checkCounter = new AtomicLong(0);
        this.running = false;
        
        initializeReplicaHealth();
        LOGGER.info("QuorumHealthChecker initialized for {} replicas with interval {}ms", 
                   replicas.size(), config.getCheckIntervalMs());
    }
    
    /**
     * Start health checking
     */
    public synchronized void start() {
        if (running) {
            LOGGER.warn("QuorumHealthChecker is already running");
            return;
        }
        
        running = true;
        
        // Schedule regular health checks
        scheduler.scheduleWithFixedDelay(
            this::performHealthCheck,
            0,
            config.getCheckIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        
        // Schedule deep health checks
        scheduler.scheduleWithFixedDelay(
            this::performDeepHealthCheck,
            config.getDeepCheckIntervalMs(),
            config.getDeepCheckIntervalMs(),
            TimeUnit.MILLISECONDS
        );
        
        LOGGER.info("QuorumHealthChecker started");
    }
    
    /**
     * Stop health checking
     */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        
        running = false;
        scheduler.shutdown();
        
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        
        LOGGER.info("QuorumHealthChecker stopped");
    }
    
    /**
     * Get current health status for all replicas
     */
    public ClusterHealthStatus getClusterHealth() {
        int totalReplicas = replicas.size();
        int healthyReplicas = 0;
        int degradedReplicas = 0;
        int unhealthyReplicas = 0;
        
        List<ReplicaHealthStatus> allStatuses = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            String replicaId = "replica-" + i;
            ReplicaHealthStatus status = replicaHealth.getOrDefault(replicaId, 
                new ReplicaHealthStatus(replicaId, HealthState.UNKNOWN, "Not checked yet", 0, 0, 0));
            
            allStatuses.add(status);
            
            switch (status.getHealthState()) {
                case HEALTHY:
                    healthyReplicas++;
                    break;
                case DEGRADED:
                    degradedReplicas++;
                    break;
                case UNHEALTHY:
                case UNKNOWN:
                default:
                    unhealthyReplicas++;
                    break;
            }
        }
        
        // Determine overall cluster health
        HealthState clusterState;
        if (unhealthyReplicas == 0 && degradedReplicas == 0) {
            clusterState = HealthState.HEALTHY;
        } else if (healthyReplicas >= (totalReplicas / 2 + 1)) {
            clusterState = HealthState.DEGRADED;
        } else {
            clusterState = HealthState.UNHEALTHY;
        }
        
        String clusterMessage = String.format("Cluster: %d healthy, %d degraded, %d unhealthy (total: %d)", 
                                            healthyReplicas, degradedReplicas, unhealthyReplicas, totalReplicas);
        
        return new ClusterHealthStatus(
            clusterState,
            clusterMessage,
            allStatuses,
            System.currentTimeMillis(),
            checkCounter.get()
        );
    }
    
    /**
     * Get health status for a specific replica
     */
    public ReplicaHealthStatus getReplicaHealth(int replicaIndex) {
        String replicaId = "replica-" + replicaIndex;
        return replicaHealth.getOrDefault(replicaId, 
            new ReplicaHealthStatus(replicaId, HealthState.UNKNOWN, "Not checked yet", 0, 0, 0));
    }
    
    /**
     * Perform immediate health check on all replicas
     */
    public CompletableFuture<ClusterHealthStatus> checkHealthNow() {
        return CompletableFuture.supplyAsync(() -> {
            performHealthCheck();
            return getClusterHealth();
        }, scheduler);
    }
    
    /**
     * Perform immediate deep health check on all replicas
     */
    public CompletableFuture<ClusterHealthStatus> deepCheckHealthNow() {
        return CompletableFuture.supplyAsync(() -> {
            performDeepHealthCheck();
            return getClusterHealth();
        }, scheduler);
    }
    
    /**
     * Get health check statistics
     */
    public HealthStats getHealthStats() {
        int totalChecks = (int) checkCounter.get();
        int failedChecks = replicaHealth.values().stream()
            .mapToInt(status -> status.getHealthState() == HealthState.UNHEALTHY ? 1 : 0)
            .sum();
        
        double avgResponseTime = replicaHealth.values().stream()
            .mapToDouble(ReplicaHealthStatus::getLastResponseTimeMs)
            .average()
            .orElse(0.0);
        
        long minLastCheck = replicaHealth.values().stream()
            .mapToLong(ReplicaHealthStatus::getLastCheckTime)
            .min()
            .orElse(System.currentTimeMillis());
        
        return new HealthStats(
            totalChecks,
            failedChecks,
            avgResponseTime,
            System.currentTimeMillis() - minLastCheck
        );
    }
    
    private void initializeReplicaHealth() {
        for (int i = 0; i < replicas.size(); i++) {
            String replicaId = "replica-" + i;
            replicaHealth.put(replicaId, new ReplicaHealthStatus(
                replicaId, HealthState.UNKNOWN, "Initializing", 0, 0, 0));
        }
    }
    
    private void performHealthCheck() {
        if (!running) return;
        
        long checkStart = System.currentTimeMillis();
        checkCounter.incrementAndGet();
        
        LOGGER.debug("Starting health check #{}", checkCounter.get());
        
        List<CompletableFuture<Void>> checkTasks = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;
            final ObjectStorage replica = replicas.get(i);
            final String replicaId = "replica-" + i;
            
            CompletableFuture<Void> checkTask = CompletableFuture.runAsync(() -> {
                checkSingleReplica(replicaId, replica, false);
            }, scheduler).exceptionally(throwable -> {
                LOGGER.error("Health check failed for {}", replicaId, throwable);
                updateReplicaHealth(replicaId, HealthState.UNHEALTHY, 
                                 "Health check error: " + throwable.getMessage(), -1);
                return null;
            });
            
            checkTasks.add(checkTask);
        }
        
        // Wait for all checks to complete with timeout
        CompletableFuture.allOf(checkTasks.toArray(new CompletableFuture[0]))
            .orTimeout(config.getCheckTimeoutMs(), TimeUnit.MILLISECONDS)
            .whenComplete((result, throwable) -> {
                long checkDuration = System.currentTimeMillis() - checkStart;
                if (throwable != null) {
                    LOGGER.warn("Health check #{} completed with errors in {}ms", checkCounter.get(), checkDuration);
                } else {
                    LOGGER.debug("Health check #{} completed successfully in {}ms", checkCounter.get(), checkDuration);
                }
            });
    }
    
    private void performDeepHealthCheck() {
        if (!running) return;
        
        LOGGER.info("Starting deep health check");
        
        for (int i = 0; i < replicas.size(); i++) {
            final ObjectStorage replica = replicas.get(i);
            final String replicaId = "replica-" + i;
            
            scheduler.execute(() -> {
                try {
                    checkSingleReplica(replicaId, replica, true);
                } catch (Exception e) {
                    LOGGER.error("Deep health check failed for {}", replicaId, e);
                    updateReplicaHealth(replicaId, HealthState.UNHEALTHY, 
                                     "Deep check error: " + e.getMessage(), -1);
                }
            });
        }
    }
    
    private void checkSingleReplica(String replicaId, ObjectStorage replica, boolean deepCheck) {
        long checkStart = System.currentTimeMillis();
        
        try {
            // Basic readiness check
            boolean isReady = replica.readinessCheck();
            if (!isReady) {
                updateReplicaHealth(replicaId, HealthState.UNHEALTHY, "Readiness check failed", -1);
                return;
            }
            
            if (deepCheck) {
                // Perform deep health checks
                performDeepReplicaChecks(replicaId, replica, checkStart);
            } else {
                // Basic health check passed
                long responseTime = System.currentTimeMillis() - checkStart;
                
                HealthState state;
                String message;
                
                if (responseTime > config.getDegradedThresholdMs()) {
                    state = HealthState.DEGRADED;
                    message = "Slow response time: " + responseTime + "ms";
                } else {
                    state = HealthState.HEALTHY;
                    message = "Healthy";
                }
                
                updateReplicaHealth(replicaId, state, message, responseTime);
            }
            
        } catch (Exception e) {
            LOGGER.warn("Health check failed for {}: {}", replicaId, e.getMessage());
            updateReplicaHealth(replicaId, HealthState.UNHEALTHY, 
                             "Check failed: " + e.getMessage(), -1);
        }
    }
    
    private void performDeepReplicaChecks(String replicaId, ObjectStorage replica, long checkStart) {
        try {
            // Test basic list operation
            CompletableFuture<List<ObjectStorage.ObjectInfo>> listFuture = replica.list("health-check/");
            listFuture.get(config.getCheckTimeoutMs(), TimeUnit.MILLISECONDS);
            
            long responseTime = System.currentTimeMillis() - checkStart;
            
            HealthState state;
            String message;
            
            if (responseTime > config.getDegradedThresholdMs() * 2) {
                state = HealthState.DEGRADED;
                message = "Deep check slow: " + responseTime + "ms";
            } else {
                state = HealthState.HEALTHY;
                message = "Deep check passed";
            }
            
            updateReplicaHealth(replicaId, state, message, responseTime);
            
        } catch (Exception e) {
            updateReplicaHealth(replicaId, HealthState.UNHEALTHY, 
                             "Deep check failed: " + e.getMessage(), -1);
        }
    }
    
    private void updateReplicaHealth(String replicaId, HealthState state, String message, long responseTime) {
        long currentTime = System.currentTimeMillis();
        ReplicaHealthStatus status = new ReplicaHealthStatus(
            replicaId, state, message, currentTime, responseTime, checkCounter.get()
        );
        
        ReplicaHealthStatus oldStatus = replicaHealth.put(replicaId, status);
        
        // Log health state changes
        if (oldStatus == null || oldStatus.getHealthState() != state) {
            if (state == HealthState.UNHEALTHY) {
                LOGGER.warn("Replica {} health changed to {}: {}", replicaId, state, message);
            } else if (state == HealthState.HEALTHY && oldStatus != null && oldStatus.getHealthState() == HealthState.UNHEALTHY) {
                LOGGER.info("Replica {} recovered to {}: {}", replicaId, state, message);
            } else {
                LOGGER.info("Replica {} health changed to {}: {}", replicaId, state, message);
            }
        }
    }
    
    // Configuration class
    public static class HealthConfig {
        private final long checkIntervalMs;
        private final long deepCheckIntervalMs;
        private final long checkTimeoutMs;
        private final long degradedThresholdMs;
        
        public HealthConfig(long checkIntervalMs, long deepCheckIntervalMs, 
                           long checkTimeoutMs, long degradedThresholdMs) {
            this.checkIntervalMs = checkIntervalMs;
            this.deepCheckIntervalMs = deepCheckIntervalMs;
            this.checkTimeoutMs = checkTimeoutMs;
            this.degradedThresholdMs = degradedThresholdMs;
        }
        
        public static HealthConfig defaultConfig() {
            return new HealthConfig(30000, 300000, 10000, 5000); // 30s, 5m, 10s, 5s
        }
        
        public long getCheckIntervalMs() { return checkIntervalMs; }
        public long getDeepCheckIntervalMs() { return deepCheckIntervalMs; }
        public long getCheckTimeoutMs() { return checkTimeoutMs; }
        public long getDegradedThresholdMs() { return degradedThresholdMs; }
    }
    
    // Data classes
    public enum HealthState {
        HEALTHY,
        DEGRADED,
        UNHEALTHY,
        UNKNOWN
    }
    
    public static class ReplicaHealthStatus {
        private final String replicaId;
        private final HealthState healthState;
        private final String message;
        private final long lastCheckTime;
        private final long lastResponseTimeMs;
        private final long checkNumber;
        
        public ReplicaHealthStatus(String replicaId, HealthState healthState, String message,
                                 long lastCheckTime, long lastResponseTimeMs, long checkNumber) {
            this.replicaId = replicaId;
            this.healthState = healthState;
            this.message = message;
            this.lastCheckTime = lastCheckTime;
            this.lastResponseTimeMs = lastResponseTimeMs;
            this.checkNumber = checkNumber;
        }
        
        public String getReplicaId() { return replicaId; }
        public HealthState getHealthState() { return healthState; }
        public String getMessage() { return message; }
        public long getLastCheckTime() { return lastCheckTime; }
        public long getLastResponseTimeMs() { return lastResponseTimeMs; }
        public long getCheckNumber() { return checkNumber; }
        
        public boolean isHealthy() {
            return healthState == HealthState.HEALTHY || healthState == HealthState.DEGRADED;
        }
        
        @Override
        public String toString() {
            return String.format("%s: %s (%s) - checked %d, response %dms", 
                               replicaId, healthState, message, checkNumber, lastResponseTimeMs);
        }
    }
    
    public static class ClusterHealthStatus {
        private final HealthState clusterState;
        private final String clusterMessage;
        private final List<ReplicaHealthStatus> replicaStatuses;
        private final long checkTime;
        private final long checkNumber;
        
        public ClusterHealthStatus(HealthState clusterState, String clusterMessage,
                                 List<ReplicaHealthStatus> replicaStatuses, long checkTime, long checkNumber) {
            this.clusterState = clusterState;
            this.clusterMessage = clusterMessage;
            this.replicaStatuses = new ArrayList<>(replicaStatuses);
            this.checkTime = checkTime;
            this.checkNumber = checkNumber;
        }
        
        public HealthState getClusterState() { return clusterState; }
        public String getClusterMessage() { return clusterMessage; }
        public List<ReplicaHealthStatus> getReplicaStatuses() { return replicaStatuses; }
        public long getCheckTime() { return checkTime; }
        public long getCheckNumber() { return checkNumber; }
        
        public boolean isHealthy() {
            return clusterState == HealthState.HEALTHY || clusterState == HealthState.DEGRADED;
        }
        
        public int getHealthyReplicaCount() {
            return (int) replicaStatuses.stream().filter(ReplicaHealthStatus::isHealthy).count();
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("Cluster Health: ").append(clusterState).append(" - ").append(clusterMessage).append("\n");
            sb.append("Check #").append(checkNumber).append(" at ").append(new java.util.Date(checkTime)).append("\n");
            sb.append("Replica Details:\n");
            for (ReplicaHealthStatus status : replicaStatuses) {
                sb.append("  ").append(status.toString()).append("\n");
            }
            return sb.toString();
        }
    }
    
    public static class HealthStats {
        private final int totalChecks;
        private final int failedChecks;
        private final double avgResponseTimeMs;
        private final long timeSinceLastCheckMs;
        
        public HealthStats(int totalChecks, int failedChecks, double avgResponseTimeMs, long timeSinceLastCheckMs) {
            this.totalChecks = totalChecks;
            this.failedChecks = failedChecks;
            this.avgResponseTimeMs = avgResponseTimeMs;
            this.timeSinceLastCheckMs = timeSinceLastCheckMs;
        }
        
        public int getTotalChecks() { return totalChecks; }
        public int getFailedChecks() { return failedChecks; }
        public double getSuccessRate() { return totalChecks > 0 ? 1.0 - (double) failedChecks / totalChecks : 1.0; }
        public double getAvgResponseTimeMs() { return avgResponseTimeMs; }
        public long getTimeSinceLastCheckMs() { return timeSinceLastCheckMs; }
    }
}