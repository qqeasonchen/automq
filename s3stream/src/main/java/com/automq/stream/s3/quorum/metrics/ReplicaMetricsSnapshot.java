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

/**
 * Immutable snapshot of ReplicaMetrics at a specific point in time
 */
public class ReplicaMetricsSnapshot {
    
    private final int replicaId;
    
    // Write metrics
    private final long writeRequestsTotal;
    private final long writeRequestsSuccessful;
    private final long writeRequestsFailed;
    private final double writeSuccessRate;
    private final double writeAverageLatency;
    
    // Read metrics
    private final long readRequestsTotal;
    private final long readRequestsSuccessful;
    private final long readRequestsFailed;
    private final double readSuccessRate;
    private final double readAverageLatency;
    
    // Health metrics
    private final boolean healthy;
    private final long failureCount;
    private final long recoveryCount;
    private final long consecutiveFailures;
    private final long consecutiveSuccesses;
    private final double uptimeRatio;
    private final long timeSinceLastFailure;
    private final long timeSinceLastRecovery;
    
    // Overall metrics
    private final double overallSuccessRate;
    private final double overallAverageLatency;
    
    // Metadata
    private final long timestamp;
    
    private ReplicaMetricsSnapshot(Builder builder) {
        this.replicaId = builder.replicaId;
        
        this.writeRequestsTotal = builder.writeRequestsTotal;
        this.writeRequestsSuccessful = builder.writeRequestsSuccessful;
        this.writeRequestsFailed = builder.writeRequestsFailed;
        this.writeSuccessRate = builder.writeSuccessRate;
        this.writeAverageLatency = builder.writeAverageLatency;
        
        this.readRequestsTotal = builder.readRequestsTotal;
        this.readRequestsSuccessful = builder.readRequestsSuccessful;
        this.readRequestsFailed = builder.readRequestsFailed;
        this.readSuccessRate = builder.readSuccessRate;
        this.readAverageLatency = builder.readAverageLatency;
        
        this.healthy = builder.healthy;
        this.failureCount = builder.failureCount;
        this.recoveryCount = builder.recoveryCount;
        this.consecutiveFailures = builder.consecutiveFailures;
        this.consecutiveSuccesses = builder.consecutiveSuccesses;
        this.uptimeRatio = builder.uptimeRatio;
        this.timeSinceLastFailure = builder.timeSinceLastFailure;
        this.timeSinceLastRecovery = builder.timeSinceLastRecovery;
        
        this.overallSuccessRate = builder.overallSuccessRate;
        this.overallAverageLatency = builder.overallAverageLatency;
        
        this.timestamp = builder.timestamp;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Basic getters
    public int getReplicaId() {
        return replicaId;
    }
    
    public long getWriteRequestsTotal() {
        return writeRequestsTotal;
    }
    
    public long getWriteRequestsSuccessful() {
        return writeRequestsSuccessful;
    }
    
    public long getWriteRequestsFailed() {
        return writeRequestsFailed;
    }
    
    public double getWriteSuccessRate() {
        return writeSuccessRate;
    }
    
    public double getWriteAverageLatency() {
        return writeAverageLatency;
    }
    
    public long getReadRequestsTotal() {
        return readRequestsTotal;
    }
    
    public long getReadRequestsSuccessful() {
        return readRequestsSuccessful;
    }
    
    public long getReadRequestsFailed() {
        return readRequestsFailed;
    }
    
    public double getReadSuccessRate() {
        return readSuccessRate;
    }
    
    public double getReadAverageLatency() {
        return readAverageLatency;
    }
    
    public boolean isHealthy() {
        return healthy;
    }
    
    public long getFailureCount() {
        return failureCount;
    }
    
    public long getRecoveryCount() {
        return recoveryCount;
    }
    
    public long getConsecutiveFailures() {
        return consecutiveFailures;
    }
    
    public long getConsecutiveSuccesses() {
        return consecutiveSuccesses;
    }
    
    public double getUptimeRatio() {
        return uptimeRatio;
    }
    
    public long getTimeSinceLastFailure() {
        return timeSinceLastFailure;
    }
    
    public long getTimeSinceLastRecovery() {
        return timeSinceLastRecovery;
    }
    
    public double getOverallSuccessRate() {
        return overallSuccessRate;
    }
    
    public double getOverallAverageLatency() {
        return overallAverageLatency;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    // Computed metrics
    public long getTotalRequests() {
        return writeRequestsTotal + readRequestsTotal;
    }
    
    public long getTotalSuccessfulRequests() {
        return writeRequestsSuccessful + readRequestsSuccessful;
    }
    
    public long getTotalFailedRequests() {
        return writeRequestsFailed + readRequestsFailed;
    }
    
    /**
     * Check if replica is performing well
     */
    public boolean isPerformingWell() {
        return healthy &&
               overallSuccessRate >= 0.95 && // At least 95% success rate
               consecutiveFailures < 3 && // Less than 3 consecutive failures
               uptimeRatio >= 0.95; // At least 95% uptime
    }
    
    /**
     * Get performance status
     */
    public String getPerformanceStatus() {
        if (!healthy) {
            return "UNHEALTHY";
        } else if (consecutiveFailures >= 5) {
            return "DEGRADED - High failure rate";
        } else if (overallSuccessRate < 0.90) {
            return "DEGRADED - Low success rate";
        } else if (uptimeRatio < 0.90) {
            return "DEGRADED - Low uptime";
        } else if (overallAverageLatency > 1000) { // More than 1 second average
            return "DEGRADED - High latency";
        } else {
            return "HEALTHY";
        }
    }
    
    /**
     * Get reliability score (0.0 to 1.0)
     */
    public double getReliabilityScore() {
        if (!healthy) {
            return 0.0;
        }
        
        // Weighted average of different reliability factors
        double successRateWeight = 0.4;
        double uptimeWeight = 0.3;
        double consistencyWeight = 0.2; // Based on consecutive failures
        double latencyWeight = 0.1;
        
        double successRateScore = overallSuccessRate;
        double uptimeScore = uptimeRatio;
        
        // Consistency score based on consecutive failures (max 10 failures = 0 score)
        double consistencyScore = Math.max(0.0, 1.0 - (consecutiveFailures / 10.0));
        
        // Latency score (assume 100ms is good, 1000ms is poor)
        double latencyScore = Math.max(0.0, Math.min(1.0, (1000.0 - overallAverageLatency) / 900.0));
        
        return (successRateScore * successRateWeight) +
               (uptimeScore * uptimeWeight) +
               (consistencyScore * consistencyWeight) +
               (latencyScore * latencyWeight);
    }
    
    @Override
    public String toString() {
        return String.format(
            "ReplicaSnapshot{id=%d, status=%s, requests=%d/%d(%.1f%%), " +
            "uptime=%.1f%%, failures=%d, score=%.2f}",
            replicaId,
            getPerformanceStatus(),
            getTotalSuccessfulRequests(),
            getTotalRequests(),
            overallSuccessRate * 100,
            uptimeRatio * 100,
            failureCount,
            getReliabilityScore()
        );
    }
    
    /**
     * Builder pattern for ReplicaMetricsSnapshot
     */
    public static class Builder {
        private int replicaId;
        private long writeRequestsTotal;
        private long writeRequestsSuccessful;
        private long writeRequestsFailed;
        private double writeSuccessRate;
        private double writeAverageLatency;
        private long readRequestsTotal;
        private long readRequestsSuccessful;
        private long readRequestsFailed;
        private double readSuccessRate;
        private double readAverageLatency;
        private boolean healthy;
        private long failureCount;
        private long recoveryCount;
        private long consecutiveFailures;
        private long consecutiveSuccesses;
        private double uptimeRatio;
        private long timeSinceLastFailure;
        private long timeSinceLastRecovery;
        private double overallSuccessRate;
        private double overallAverageLatency;
        private long timestamp;
        
        public Builder replicaId(int replicaId) {
            this.replicaId = replicaId;
            return this;
        }
        
        public Builder writeMetrics(long total, long successful, long failed, 
                                  double successRate, double avgLatency) {
            this.writeRequestsTotal = total;
            this.writeRequestsSuccessful = successful;
            this.writeRequestsFailed = failed;
            this.writeSuccessRate = successRate;
            this.writeAverageLatency = avgLatency;
            return this;
        }
        
        public Builder readMetrics(long total, long successful, long failed,
                                 double successRate, double avgLatency) {
            this.readRequestsTotal = total;
            this.readRequestsSuccessful = successful;
            this.readRequestsFailed = failed;
            this.readSuccessRate = successRate;
            this.readAverageLatency = avgLatency;
            return this;
        }
        
        public Builder healthMetrics(boolean healthy, long failures, long recoveries,
                                   long consecutiveFailures, long consecutiveSuccesses,
                                   double uptimeRatio, long timeSinceLastFailure,
                                   long timeSinceLastRecovery) {
            this.healthy = healthy;
            this.failureCount = failures;
            this.recoveryCount = recoveries;
            this.consecutiveFailures = consecutiveFailures;
            this.consecutiveSuccesses = consecutiveSuccesses;
            this.uptimeRatio = uptimeRatio;
            this.timeSinceLastFailure = timeSinceLastFailure;
            this.timeSinceLastRecovery = timeSinceLastRecovery;
            return this;
        }
        
        public Builder overallMetrics(double successRate, double avgLatency) {
            this.overallSuccessRate = successRate;
            this.overallAverageLatency = avgLatency;
            return this;
        }
        
        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public ReplicaMetricsSnapshot build() {
            return new ReplicaMetricsSnapshot(this);
        }
    }
}