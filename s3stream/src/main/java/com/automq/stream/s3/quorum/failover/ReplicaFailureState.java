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

import com.automq.stream.s3.quorum.config.ReplicaConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tracks failure state and statistics for a single replica
 * Implements PHI Accrual Failure Detector algorithm
 */
public class ReplicaFailureState {
    
    // Configuration
    private static final int DEFAULT_WINDOW_SIZE = 100;
    private static final int MIN_WINDOW_SIZE = 10;
    private static final int RECOVERY_THRESHOLD = 3;
    private static final int FAILURE_THRESHOLD = 3;
    
    private final ReplicaConfig replicaConfig;
    private final AtomicBoolean healthy = new AtomicBoolean(true);
    private final AtomicLong lastSuccessTime = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicInteger consecutiveSuccesses = new AtomicInteger(0);
    
    // PHI failure detector state
    private final List<Long> intervalHistory = new ArrayList<>();
    private final AtomicLong lastHeartbeatTime = new AtomicLong(System.currentTimeMillis());
    private volatile double meanInterval = 5000.0; // Default 5 seconds
    private volatile double variance = 1000.0;     // Default variance
    
    // Failure details
    private volatile Exception lastException;
    private volatile String lastFailureReason;
    
    public ReplicaFailureState(ReplicaConfig replicaConfig) {
        this.replicaConfig = replicaConfig;
    }

    /**
     * Record a successful operation
     */
    public synchronized void recordSuccess() {
        long currentTime = System.currentTimeMillis();
        lastSuccessTime.set(currentTime);
        consecutiveSuccesses.incrementAndGet();
        consecutiveFailures.set(0);
        
        // Update heartbeat for PHI detector
        updateHeartbeat(currentTime);
    }

    /**
     * Record a failed operation
     */
    public synchronized void recordFailure(Exception exception) {
        long currentTime = System.currentTimeMillis();
        lastFailureTime.set(currentTime);
        failureCount.incrementAndGet();
        consecutiveFailures.incrementAndGet();
        consecutiveSuccesses.set(0);
        
        this.lastException = exception;
        this.lastFailureReason = exception != null ? exception.getMessage() : "Unknown failure";
    }

    /**
     * Update heartbeat timestamp for PHI failure detector
     */
    private void updateHeartbeat(long currentTime) {
        long lastTime = lastHeartbeatTime.getAndSet(currentTime);
        
        if (lastTime > 0) {
            long interval = currentTime - lastTime;
            
            synchronized (intervalHistory) {
                intervalHistory.add(interval);
                
                // Keep only recent intervals
                if (intervalHistory.size() > DEFAULT_WINDOW_SIZE) {
                    intervalHistory.remove(0);
                }
                
                // Update statistics if we have enough data
                if (intervalHistory.size() >= MIN_WINDOW_SIZE) {
                    updateStatistics();
                }
            }
        }
    }

    /**
     * Update PHI failure detector statistics
     */
    public void updatePhiStatistics() {
        synchronized (intervalHistory) {
            if (intervalHistory.size() >= MIN_WINDOW_SIZE) {
                updateStatistics();
            }
        }
    }

    /**
     * Calculate current PHI value
     */
    public double calculatePhi(long currentTime) {
        long timeSinceLastHeartbeat = currentTime - lastHeartbeatTime.get();
        
        if (timeSinceLastHeartbeat <= 0 || variance <= 0) {
            return 0.0;
        }
        
        // PHI = -log10(1 - F(t))
        // Where F(t) is the CDF of the normal distribution
        double y = (timeSinceLastHeartbeat - meanInterval) / Math.sqrt(variance);
        double e = Math.exp(-y * (1.5976 + 0.070566 * y * y));
        
        if (timeSinceLastHeartbeat > meanInterval) {
            return -Math.log10(e / (1.0 + e));
        } else {
            return -Math.log10(1.0 - 1.0 / (1.0 + e));
        }
    }

    /**
     * Check if replica should be marked as failed
     */
    public boolean shouldMarkAsFailed() {
        return consecutiveFailures.get() >= FAILURE_THRESHOLD;
    }

    /**
     * Check if replica should be marked as recovered
     */
    public boolean shouldRecover() {
        return consecutiveSuccesses.get() >= RECOVERY_THRESHOLD;
    }

    /**
     * Mark replica as unhealthy
     */
    public void markAsUnhealthy() {
        healthy.set(false);
    }

    /**
     * Mark replica as healthy
     */
    public void markAsHealthy() {
        healthy.set(true);
        // Reset failure counters on recovery
        consecutiveFailures.set(0);
        failureCount.set(0);
    }

    /**
     * Update interval statistics for PHI detector
     */
    private void updateStatistics() {
        if (intervalHistory.isEmpty()) {
            return;
        }
        
        // Calculate mean
        double sum = 0.0;
        for (Long interval : intervalHistory) {
            sum += interval;
        }
        meanInterval = sum / intervalHistory.size();
        
        // Calculate variance
        double sumSquaredDiffs = 0.0;
        for (Long interval : intervalHistory) {
            double diff = interval - meanInterval;
            sumSquaredDiffs += diff * diff;
        }
        variance = sumSquaredDiffs / intervalHistory.size();
        
        // Ensure minimum variance to avoid division by zero
        if (variance < 1.0) {
            variance = 1.0;
        }
    }

    // Getters

    public ReplicaConfig getReplicaConfig() {
        return replicaConfig;
    }

    public boolean isHealthy() {
        return healthy.get();
    }

    public long getLastSuccessTime() {
        return lastSuccessTime.get();
    }

    public long getLastFailureTime() {
        return lastFailureTime.get();
    }

    public int getFailureCount() {
        return failureCount.get();
    }

    public int getConsecutiveFailures() {
        return consecutiveFailures.get();
    }

    public int getConsecutiveSuccesses() {
        return consecutiveSuccesses.get();
    }

    public long getLastHeartbeatTime() {
        return lastHeartbeatTime.get();
    }

    public double getMeanInterval() {
        return meanInterval;
    }

    public double getVariance() {
        return variance;
    }

    public Exception getLastException() {
        return lastException;
    }

    public String getLastFailureReason() {
        return lastFailureReason;
    }

    public int getHistorySize() {
        synchronized (intervalHistory) {
            return intervalHistory.size();
        }
    }

    /**
     * Get a snapshot of current state for monitoring/debugging
     */
    public FailureStateSnapshot getSnapshot() {
        return new FailureStateSnapshot(
            replicaConfig.getReplicaId(),
            replicaConfig.getRegion(),
            isHealthy(),
            getLastSuccessTime(),
            getLastFailureTime(),
            getFailureCount(),
            getConsecutiveFailures(),
            getConsecutiveSuccesses(),
            calculatePhi(System.currentTimeMillis()),
            getMeanInterval(),
            getVariance(),
            getLastFailureReason()
        );
    }

    /**
     * Immutable snapshot of failure state
     */
    public static class FailureStateSnapshot {
        private final int replicaId;
        private final String region;
        private final boolean healthy;
        private final long lastSuccessTime;
        private final long lastFailureTime;
        private final int failureCount;
        private final int consecutiveFailures;
        private final int consecutiveSuccesses;
        private final double phi;
        private final double meanInterval;
        private final double variance;
        private final String lastFailureReason;

        public FailureStateSnapshot(int replicaId, String region, boolean healthy,
                                   long lastSuccessTime, long lastFailureTime,
                                   int failureCount, int consecutiveFailures,
                                   int consecutiveSuccesses, double phi,
                                   double meanInterval, double variance,
                                   String lastFailureReason) {
            this.replicaId = replicaId;
            this.region = region;
            this.healthy = healthy;
            this.lastSuccessTime = lastSuccessTime;
            this.lastFailureTime = lastFailureTime;
            this.failureCount = failureCount;
            this.consecutiveFailures = consecutiveFailures;
            this.consecutiveSuccesses = consecutiveSuccesses;
            this.phi = phi;
            this.meanInterval = meanInterval;
            this.variance = variance;
            this.lastFailureReason = lastFailureReason;
        }

        // Getters
        public int getReplicaId() {
            return replicaId;
        }
        
        public String getRegion() {
            return region;
        }
        
        public boolean isHealthy() {
            return healthy;
        }
        
        public long getLastSuccessTime() {
            return lastSuccessTime;
        }
        
        public long getLastFailureTime() {
            return lastFailureTime;
        }
        
        public int getFailureCount() {
            return failureCount;
        }
        
        public int getConsecutiveFailures() {
            return consecutiveFailures;
        }
        
        public int getConsecutiveSuccesses() {
            return consecutiveSuccesses;
        }
        
        public double getPhi() {
            return phi;
        }
        
        public double getMeanInterval() {
            return meanInterval;
        }
        
        public double getVariance() {
            return variance;
        }
        
        public String getLastFailureReason() {
            return lastFailureReason;
        }

        @Override
        public String toString() {
            return String.format(
                "FailureState{replica=%d, region=%s, healthy=%s, failures=%d, phi=%.2f}",
                replicaId, region, healthy, failureCount, phi);
        }
    }
}