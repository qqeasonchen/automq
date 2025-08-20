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

import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Failure detector for S3 Quorum Storage replicas
 * Implements adaptive failure detection with configurable thresholds
 */
public class FailureDetector {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetector.class);
    
    // Default configuration values
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 5000;
    private static final long DEFAULT_FAILURE_TIMEOUT_MS = 15000;
    private static final int DEFAULT_FAILURE_THRESHOLD = 3;
    private static final double DEFAULT_PHI_THRESHOLD = 8.0;
    
    private final QuorumConfig quorumConfig;
    private final QuorumState quorumState;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    // Failure detection configuration
    private final long heartbeatIntervalMs;
    private final long failureTimeoutMs;
    private final int failureThreshold;
    private final double phiThreshold;
    
    // Per-replica failure detection state
    private final Map<Integer, ReplicaFailureState> replicaStates = new ConcurrentHashMap<>();
    private final List<FailureDetectorListener> listeners = new ArrayList<>();
    
    public FailureDetector(QuorumConfig quorumConfig, QuorumState quorumState) {
        this(quorumConfig, quorumState, 
             DEFAULT_HEARTBEAT_INTERVAL_MS, 
             DEFAULT_FAILURE_TIMEOUT_MS,
             DEFAULT_FAILURE_THRESHOLD, 
             DEFAULT_PHI_THRESHOLD);
    }
    
    public FailureDetector(QuorumConfig quorumConfig, 
                          QuorumState quorumState,
                          long heartbeatIntervalMs,
                          long failureTimeoutMs,
                          int failureThreshold,
                          double phiThreshold) {
        this.quorumConfig = quorumConfig;
        this.quorumState = quorumState;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.failureTimeoutMs = failureTimeoutMs;
        this.failureThreshold = failureThreshold;
        this.phiThreshold = phiThreshold;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "FailureDetector");
            t.setDaemon(true);
            return t;
        });
        
        // Initialize replica states
        for (ReplicaConfig replica : quorumConfig.getReplicaConfigs()) {
            replicaStates.put(replica.getReplicaId(), new ReplicaFailureState(replica));
        }
    }

    /**
     * Start the failure detector
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Starting failure detector with heartbeat interval: {}ms, failure timeout: {}ms", 
                       heartbeatIntervalMs, failureTimeoutMs);
            
            // Start periodic failure detection
            scheduler.scheduleWithFixedDelay(this::detectFailures, 
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
            
            // Start periodic health check
            scheduler.scheduleWithFixedDelay(this::performHealthChecks, 
                heartbeatIntervalMs / 2, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
                
            LOGGER.info("Failure detector started successfully");
        }
    }

    /**
     * Stop the failure detector
     */
    public void stop() {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Stopping failure detector");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            LOGGER.info("Failure detector stopped");
        }
    }

    /**
     * Record a successful operation for a replica
     */
    public void recordSuccess(int replicaId) {
        ReplicaFailureState state = replicaStates.get(replicaId);
        if (state != null) {
            state.recordSuccess();
            
            // Check if replica has recovered
            if (!state.isHealthy() && state.shouldRecover()) {
                handleReplicaRecovery(replicaId, state);
            }
        }
    }

    /**
     * Record a failed operation for a replica
     */
    public void recordFailure(int replicaId, Exception exception) {
        ReplicaFailureState state = replicaStates.get(replicaId);
        if (state != null) {
            state.recordFailure(exception);
            
            // Check if replica should be marked as failed
            if (state.isHealthy() && state.shouldMarkAsFailed()) {
                handleReplicaFailure(replicaId, state);
            }
        }
    }

    /**
     * Add a failure detector listener
     */
    public void addListener(FailureDetectorListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    /**
     * Remove a failure detector listener
     */
    public void removeListener(FailureDetectorListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    /**
     * Get the current failure state of a replica
     */
    public ReplicaFailureState getReplicaState(int replicaId) {
        return replicaStates.get(replicaId);
    }

    /**
     * Get all replica states
     */
    public Map<Integer, ReplicaFailureState> getAllReplicaStates() {
        return new ConcurrentHashMap<>(replicaStates);
    }

    /**
     * Periodic failure detection
     */
    private void detectFailures() {
        try {
            long currentTime = System.currentTimeMillis();
            
            for (Map.Entry<Integer, ReplicaFailureState> entry : replicaStates.entrySet()) {
                int replicaId = entry.getKey();
                ReplicaFailureState state = entry.getValue();
                
                // Check for timeout-based failures
                if (state.isHealthy() && hasTimedOut(state, currentTime)) {
                    LOGGER.warn("Replica {} timed out (last success: {}ms ago)", 
                               replicaId, currentTime - state.getLastSuccessTime());
                    handleReplicaFailure(replicaId, state);
                }
                
                // Check PHI failure detector
                if (state.isHealthy() && exceedsPhiThreshold(state, currentTime)) {
                    LOGGER.warn("Replica {} exceeds PHI threshold (phi: {:.2f} > {:.2f})", 
                               replicaId, state.calculatePhi(currentTime), phiThreshold);
                    handleReplicaFailure(replicaId, state);
                }
            }
            
        } catch (Exception e) {
            LOGGER.error("Error during failure detection", e);
        }
    }

    /**
     * Perform health checks on all replicas
     */
    private void performHealthChecks() {
        try {
            for (Map.Entry<Integer, ReplicaFailureState> entry : replicaStates.entrySet()) {
                int replicaId = entry.getKey();
                ReplicaFailureState state = entry.getValue();
                
                // Update PHI accrual failure detector statistics
                state.updatePhiStatistics();
                
                // Log periodic health status
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Replica {} health: healthy={}, failures={}, phi={:.2f}", 
                               replicaId, state.isHealthy(), state.getFailureCount(), 
                               state.calculatePhi(System.currentTimeMillis()));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during health checks", e);
        }
    }

    /**
     * Check if a replica has timed out
     */
    private boolean hasTimedOut(ReplicaFailureState state, long currentTime) {
        return (currentTime - state.getLastSuccessTime()) > failureTimeoutMs;
    }

    /**
     * Check if PHI threshold is exceeded
     */
    private boolean exceedsPhiThreshold(ReplicaFailureState state, long currentTime) {
        return state.calculatePhi(currentTime) > phiThreshold;
    }

    /**
     * Handle replica failure detection
     */
    private void handleReplicaFailure(int replicaId, ReplicaFailureState state) {
        LOGGER.warn("Marking replica {} as failed (failures: {}, last success: {}ms ago)", 
                   replicaId, state.getFailureCount(), 
                   System.currentTimeMillis() - state.getLastSuccessTime());
        
        state.markAsUnhealthy();
        quorumState.markReplicaFailed(replicaId);
        
        // Notify listeners
        notifyReplicaFailed(replicaId, state);
        
        // Check quorum health
        checkQuorumHealth();
    }

    /**
     * Handle replica recovery
     */
    private void handleReplicaRecovery(int replicaId, ReplicaFailureState state) {
        LOGGER.info("Replica {} has recovered (consecutive successes: {})", 
                   replicaId, state.getConsecutiveSuccesses());
        
        state.markAsHealthy();
        quorumState.markReplicaSuccess(replicaId);
        
        // Notify listeners
        notifyReplicaRecovered(replicaId, state);
        
        // Check quorum health
        checkQuorumHealth();
    }

    /**
     * Check overall quorum health and notify if critical
     */
    private void checkQuorumHealth() {
        int healthyReplicas = quorumState.getHealthyReplicaCount();
        int totalReplicas = quorumConfig.getQuorumSize();
        
        if (healthyReplicas < quorumConfig.getWriteQuorumSize()) {
            LOGGER.error("CRITICAL: Write quorum lost! Healthy replicas: {}/{}, required: {}", 
                        healthyReplicas, totalReplicas, quorumConfig.getWriteQuorumSize());
            notifyQuorumLost(FailureType.WRITE_QUORUM_LOST);
        } else if (healthyReplicas < quorumConfig.getReadQuorumSize()) {
            LOGGER.error("CRITICAL: Read quorum lost! Healthy replicas: {}/{}, required: {}", 
                        healthyReplicas, totalReplicas, quorumConfig.getReadQuorumSize());
            notifyQuorumLost(FailureType.READ_QUORUM_LOST);
        } else if (healthyReplicas <= totalReplicas / 2) {
            LOGGER.warn("WARNING: Quorum at risk! Healthy replicas: {}/{}", 
                       healthyReplicas, totalReplicas);
            notifyQuorumAtRisk(healthyReplicas, totalReplicas);
        } else if (healthyReplicas == totalReplicas) {
            LOGGER.info("All replicas are healthy: {}/{}", healthyReplicas, totalReplicas);
            notifyQuorumHealthy();
        }
    }

    /**
     * Notify listeners of replica failure
     */
    private void notifyReplicaFailed(int replicaId, ReplicaFailureState state) {
        synchronized (listeners) {
            for (FailureDetectorListener listener : listeners) {
                try {
                    listener.onReplicaFailed(replicaId, state);
                } catch (Exception e) {
                    LOGGER.error("Error notifying listener of replica failure", e);
                }
            }
        }
    }

    /**
     * Notify listeners of replica recovery
     */
    private void notifyReplicaRecovered(int replicaId, ReplicaFailureState state) {
        synchronized (listeners) {
            for (FailureDetectorListener listener : listeners) {
                try {
                    listener.onReplicaRecovered(replicaId, state);
                } catch (Exception e) {
                    LOGGER.error("Error notifying listener of replica recovery", e);
                }
            }
        }
    }

    /**
     * Notify listeners of quorum loss
     */
    private void notifyQuorumLost(FailureType type) {
        synchronized (listeners) {
            for (FailureDetectorListener listener : listeners) {
                try {
                    listener.onQuorumLost(type);
                } catch (Exception e) {
                    LOGGER.error("Error notifying listener of quorum loss", e);
                }
            }
        }
    }

    /**
     * Notify listeners of quorum at risk
     */
    private void notifyQuorumAtRisk(int healthyReplicas, int totalReplicas) {
        synchronized (listeners) {
            for (FailureDetectorListener listener : listeners) {
                try {
                    listener.onQuorumAtRisk(healthyReplicas, totalReplicas);
                } catch (Exception e) {
                    LOGGER.error("Error notifying listener of quorum at risk", e);
                }
            }
        }
    }

    /**
     * Notify listeners of healthy quorum
     */
    private void notifyQuorumHealthy() {
        synchronized (listeners) {
            for (FailureDetectorListener listener : listeners) {
                try {
                    listener.onQuorumHealthy();
                } catch (Exception e) {
                    LOGGER.error("Error notifying listener of healthy quorum", e);
                }
            }
        }
    }

    /**
     * Failure types
     */
    public enum FailureType {
        REPLICA_TIMEOUT,
        REPLICA_PHI_THRESHOLD,
        WRITE_QUORUM_LOST,
        READ_QUORUM_LOST,
        QUORUM_AT_RISK
    }

    /**
     * Failure detector listener interface
     */
    public interface FailureDetectorListener {
        default void onReplicaFailed(int replicaId, ReplicaFailureState state) {}
        default void onReplicaRecovered(int replicaId, ReplicaFailureState state) {}
        default void onQuorumLost(FailureType type) {}
        default void onQuorumAtRisk(int healthyReplicas, int totalReplicas) {}
        default void onQuorumHealthy() {}
    }
}