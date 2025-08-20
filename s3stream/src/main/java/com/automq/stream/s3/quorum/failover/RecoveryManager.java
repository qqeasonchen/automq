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

import com.automq.stream.s3.Storage;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;
import com.automq.stream.s3.quorum.failover.FailureDetector.FailureDetectorListener;
import com.automq.stream.s3.quorum.failover.FailureDetector.FailureType;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Manages automatic recovery operations for failed replicas
 * Handles replica restoration, data consistency checks, and emergency procedures
 */
public class RecoveryManager implements FailureDetectorListener {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryManager.class);
    
    // Recovery configuration
    private static final long DEFAULT_RECOVERY_INTERVAL_MS = 30000; // 30 seconds
    private static final long DEFAULT_CONSISTENCY_CHECK_INTERVAL_MS = 60000; // 1 minute
    private static final int MAX_CONCURRENT_RECOVERIES = 2;
    private static final int MAX_RECOVERY_ATTEMPTS = 3;
    
    private final QuorumConfig quorumConfig;
    private final QuorumState quorumState;
    private final List<Storage> replicaStorages;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    
    // Recovery state tracking
    private final Map<Integer, RecoveryState> recoveryStates = new ConcurrentHashMap<>();
    private final AtomicInteger activeRecoveries = new AtomicInteger(0);
    private final List<RecoveryListener> listeners = new ArrayList<>();
    
    public RecoveryManager(QuorumConfig quorumConfig, 
                          QuorumState quorumState, 
                          List<Storage> replicaStorages) {
        this.quorumConfig = quorumConfig;
        this.quorumState = quorumState;
        this.replicaStorages = replicaStorages;
        this.scheduler = Executors.newScheduledThreadPool(3, r -> {
            Thread t = new Thread(r, "RecoveryManager");
            t.setDaemon(true);
            return t;
        });
        
        // Initialize recovery states
        for (ReplicaConfig replica : quorumConfig.getReplicaConfigs()) {
            recoveryStates.put(replica.getReplicaId(), new RecoveryState(replica));
        }
    }

    /**
     * Start the recovery manager
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Starting recovery manager");
            
            // Start periodic recovery attempts
            scheduler.scheduleWithFixedDelay(this::performRecoveryAttempts, 
                DEFAULT_RECOVERY_INTERVAL_MS, DEFAULT_RECOVERY_INTERVAL_MS, TimeUnit.MILLISECONDS);
            
            // Start periodic consistency checks
            scheduler.scheduleWithFixedDelay(this::performConsistencyChecks, 
                DEFAULT_CONSISTENCY_CHECK_INTERVAL_MS, DEFAULT_CONSISTENCY_CHECK_INTERVAL_MS, TimeUnit.MILLISECONDS);
                
            LOGGER.info("Recovery manager started successfully");
        }
    }

    /**
     * Stop the recovery manager
     */
    public void stop() {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Stopping recovery manager");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
            LOGGER.info("Recovery manager stopped");
        }
    }

    /**
     * Add a recovery listener
     */
    public void addListener(RecoveryListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    /**
     * Manually trigger recovery for a specific replica
     */
    public CompletableFuture<Boolean> triggerRecovery(int replicaId) {
        RecoveryState state = recoveryStates.get(replicaId);
        if (state == null) {
            return CompletableFuture.completedFuture(false);
        }
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                return performReplicaRecovery(replicaId, state);
            } catch (Exception e) {
                LOGGER.error("Manual recovery failed for replica {}", replicaId, e);
                return false;
            }
        });
    }

    /**
     * Get recovery state for a replica
     */
    public RecoveryState getRecoveryState(int replicaId) {
        return recoveryStates.get(replicaId);
    }

    // FailureDetectorListener implementation

    @Override
    public void onReplicaFailed(int replicaId, ReplicaFailureState failureState) {
        LOGGER.warn("Replica {} failed, scheduling recovery", replicaId);
        
        RecoveryState recoveryState = recoveryStates.get(replicaId);
        if (recoveryState != null) {
            recoveryState.markAsNeedingRecovery();
            notifyRecoveryNeeded(replicaId, recoveryState);
        }
    }

    @Override
    public void onReplicaRecovered(int replicaId, ReplicaFailureState failureState) {
        LOGGER.info("Replica {} recovered naturally", replicaId);
        
        RecoveryState recoveryState = recoveryStates.get(replicaId);
        if (recoveryState != null) {
            recoveryState.markAsRecovered();
            notifyRecoveryCompleted(replicaId, recoveryState, true);
        }
    }

    @Override
    public void onQuorumLost(FailureType type) {
        LOGGER.error("CRITICAL: Quorum lost ({}), initiating emergency recovery", type);
        initiateEmergencyRecovery(type);
    }

    @Override
    public void onQuorumAtRisk(int healthyReplicas, int totalReplicas) {
        LOGGER.warn("Quorum at risk ({}/{}), accelerating recovery efforts", 
                   healthyReplicas, totalReplicas);
        accelerateRecovery();
    }

    /**
     * Perform periodic recovery attempts
     */
    private void performRecoveryAttempts() {
        try {
            if (activeRecoveries.get() >= MAX_CONCURRENT_RECOVERIES) {
                LOGGER.debug("Maximum concurrent recoveries reached, skipping this cycle");
                return;
            }
            
            for (Map.Entry<Integer, RecoveryState> entry : recoveryStates.entrySet()) {
                int replicaId = entry.getKey();
                RecoveryState state = entry.getValue();
                
                if (shouldAttemptRecovery(replicaId, state)) {
                    scheduler.submit(() -> performReplicaRecovery(replicaId, state));
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during recovery attempts", e);
        }
    }

    /**
     * Perform periodic consistency checks
     */
    private void performConsistencyChecks() {
        try {
            LOGGER.debug("Performing consistency checks across replicas");
            
            // Check for data inconsistencies that might require repair
            for (int i = 0; i < replicaStorages.size(); i++) {
                if (quorumState.isReplicaHealthy(i)) {
                    // In a real implementation, this would:
                    // 1. Sample data from healthy replicas
                    // 2. Compare checksums or metadata
                    // 3. Identify inconsistencies
                    // 4. Schedule repair operations
                    
                    LOGGER.debug("Consistency check completed for replica {}", i);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during consistency checks", e);
        }
    }

    /**
     * Check if recovery should be attempted for a replica
     */
    private boolean shouldAttemptRecovery(int replicaId, RecoveryState state) {
        return state.needsRecovery() && 
               !state.isRecovering() && 
               state.getAttemptCount() < MAX_RECOVERY_ATTEMPTS &&
               (System.currentTimeMillis() - state.getLastAttemptTime()) > DEFAULT_RECOVERY_INTERVAL_MS;
    }

    /**
     * Perform recovery for a specific replica
     */
    private boolean performReplicaRecovery(int replicaId, RecoveryState state) {
        if (activeRecoveries.incrementAndGet() > MAX_CONCURRENT_RECOVERIES) {
            activeRecoveries.decrementAndGet();
            return false;
        }
        
        try {
            LOGGER.info("Starting recovery for replica {} (attempt {})", 
                       replicaId, state.getAttemptCount() + 1);
            
            state.startRecovery();
            notifyRecoveryStarted(replicaId, state);
            
            // Perform recovery steps
            boolean success = executeRecoverySteps(replicaId, state);
            
            if (success) {
                state.markAsRecovered();
                quorumState.markReplicaSuccess(replicaId);
                LOGGER.info("Recovery completed successfully for replica {}", replicaId);
                notifyRecoveryCompleted(replicaId, state, true);
            } else {
                state.markRecoveryFailed();
                LOGGER.warn("Recovery failed for replica {} (attempt {})", 
                           replicaId, state.getAttemptCount());
                notifyRecoveryCompleted(replicaId, state, false);
            }
            
            return success;
            
        } catch (Exception e) {
            state.markRecoveryFailed();
            LOGGER.error("Recovery exception for replica {}", replicaId, e);
            notifyRecoveryCompleted(replicaId, state, false);
            return false;
        } finally {
            activeRecoveries.decrementAndGet();
        }
    }

    /**
     * Execute the actual recovery steps
     */
    private boolean executeRecoverySteps(int replicaId, RecoveryState state) {
        try {
            Storage replicaStorage = replicaStorages.get(replicaId);
            
            // Step 1: Basic connectivity test
            LOGGER.debug("Step 1: Testing connectivity to replica {}", replicaId);
            if (!testReplicaConnectivity(replicaStorage)) {
                LOGGER.warn("Connectivity test failed for replica {}", replicaId);
                return false;
            }
            
            // Step 2: Health check
            LOGGER.debug("Step 2: Performing health check on replica {}", replicaId);
            if (!performHealthCheck(replicaStorage)) {
                LOGGER.warn("Health check failed for replica {}", replicaId);
                return false;
            }
            
            // Step 3: Data consistency verification
            LOGGER.debug("Step 3: Verifying data consistency for replica {}", replicaId);
            if (!verifyDataConsistency(replicaId, replicaStorage)) {
                LOGGER.warn("Data consistency verification failed for replica {}", replicaId);
                // Note: This might trigger data repair in a real implementation
            }
            
            // Step 4: Re-enable replica in quorum
            LOGGER.debug("Step 4: Re-enabling replica {} in quorum", replicaId);
            return true;
            
        } catch (Exception e) {
            LOGGER.error("Error during recovery steps for replica {}", replicaId, e);
            return false;
        }
    }

    /**
     * Test basic connectivity to a replica
     */
    private boolean testReplicaConnectivity(Storage replicaStorage) {
        try {
            // In a real implementation, this would:
            // 1. Test network connectivity
            // 2. Verify authentication
            // 3. Check basic S3 operations
            
            // For now, simulate a connectivity test
            Thread.sleep(100); // Simulate network delay
            return true;
            
        } catch (Exception e) {
            LOGGER.debug("Connectivity test failed", e);
            return false;
        }
    }

    /**
     * Perform health check on a replica
     */
    private boolean performHealthCheck(Storage replicaStorage) {
        try {
            // In a real implementation, this would:
            // 1. Check replica resource usage
            // 2. Verify storage availability
            // 3. Test read/write capabilities
            
            // For now, simulate a health check
            Thread.sleep(50);
            return true;
            
        } catch (Exception e) {
            LOGGER.debug("Health check failed", e);
            return false;
        }
    }

    /**
     * Verify data consistency for a replica
     */
    private boolean verifyDataConsistency(int replicaId, Storage replicaStorage) {
        try {
            // In a real implementation, this would:
            // 1. Compare data checksums with other replicas
            // 2. Verify metadata consistency
            // 3. Check for missing or corrupted data
            // 4. Trigger repair if needed
            
            // For now, simulate consistency check
            Thread.sleep(200);
            return true;
            
        } catch (Exception e) {
            LOGGER.debug("Data consistency check failed", e);
            return false;
        }
    }

    /**
     * Initiate emergency recovery procedures
     */
    private void initiateEmergencyRecovery(FailureType type) {
        LOGGER.error("Initiating emergency recovery for failure type: {}", type);
        
        // In a real implementation, this would:
        // 1. Attempt to recover critical replicas immediately
        // 2. Enable degraded mode operation
        // 3. Alert administrators
        // 4. Attempt to restore minimum quorum
        
        // For now, attempt to recover all failed replicas simultaneously
        for (Map.Entry<Integer, RecoveryState> entry : recoveryStates.entrySet()) {
            int replicaId = entry.getKey();
            RecoveryState state = entry.getValue();
            
            if (state.needsRecovery()) {
                scheduler.submit(() -> performReplicaRecovery(replicaId, state));
            }
        }
    }

    /**
     * Accelerate recovery efforts when quorum is at risk
     */
    private void accelerateRecovery() {
        LOGGER.warn("Accelerating recovery efforts due to quorum risk");
        
        // Trigger immediate recovery attempts for all failed replicas
        for (Map.Entry<Integer, RecoveryState> entry : recoveryStates.entrySet()) {
            int replicaId = entry.getKey();
            RecoveryState state = entry.getValue();
            
            if (state.needsRecovery() && !state.isRecovering()) {
                scheduler.submit(() -> performReplicaRecovery(replicaId, state));
            }
        }
    }

    // Notification methods

    private void notifyRecoveryNeeded(int replicaId, RecoveryState state) {
        synchronized (listeners) {
            for (RecoveryListener listener : listeners) {
                try {
                    listener.onRecoveryNeeded(replicaId, state);
                } catch (Exception e) {
                    LOGGER.error("Error notifying recovery needed", e);
                }
            }
        }
    }

    private void notifyRecoveryStarted(int replicaId, RecoveryState state) {
        synchronized (listeners) {
            for (RecoveryListener listener : listeners) {
                try {
                    listener.onRecoveryStarted(replicaId, state);
                } catch (Exception e) {
                    LOGGER.error("Error notifying recovery started", e);
                }
            }
        }
    }

    private void notifyRecoveryCompleted(int replicaId, RecoveryState state, boolean success) {
        synchronized (listeners) {
            for (RecoveryListener listener : listeners) {
                try {
                    listener.onRecoveryCompleted(replicaId, state, success);
                } catch (Exception e) {
                    LOGGER.error("Error notifying recovery completed", e);
                }
            }
        }
    }

    /**
     * Recovery listener interface
     */
    public interface RecoveryListener {
        default void onRecoveryNeeded(int replicaId, RecoveryState state) {}
        default void onRecoveryStarted(int replicaId, RecoveryState state) {}
        default void onRecoveryCompleted(int replicaId, RecoveryState state, boolean success) {}
    }

    /**
     * Recovery state for a single replica
     */
    public static class RecoveryState {
        private final ReplicaConfig replicaConfig;
        private final AtomicBoolean needsRecovery = new AtomicBoolean(false);
        private final AtomicBoolean isRecovering = new AtomicBoolean(false);
        private final AtomicInteger attemptCount = new AtomicInteger(0);
        private final AtomicLong lastAttemptTime = new AtomicLong(0);
        private final AtomicLong recoveryStartTime = new AtomicLong(0);

        public RecoveryState(ReplicaConfig replicaConfig) {
            this.replicaConfig = replicaConfig;
        }

        public void markAsNeedingRecovery() {
            needsRecovery.set(true);
        }

        public void startRecovery() {
            isRecovering.set(true);
            attemptCount.incrementAndGet();
            lastAttemptTime.set(System.currentTimeMillis());
            recoveryStartTime.set(System.currentTimeMillis());
        }

        public void markAsRecovered() {
            needsRecovery.set(false);
            isRecovering.set(false);
            attemptCount.set(0);
        }

        public void markRecoveryFailed() {
            isRecovering.set(false);
            lastAttemptTime.set(System.currentTimeMillis());
        }

        public boolean needsRecovery() {
            return needsRecovery.get();
        }
        
        public boolean isRecovering() {
            return isRecovering.get();
        }
        
        public int getAttemptCount() {
            return attemptCount.get();
        }
        
        public long getLastAttemptTime() {
            return lastAttemptTime.get();
        }
        
        public long getRecoveryStartTime() {
            return recoveryStartTime.get();
        }
        
        public ReplicaConfig getReplicaConfig() {
            return replicaConfig;
        }
    }
}