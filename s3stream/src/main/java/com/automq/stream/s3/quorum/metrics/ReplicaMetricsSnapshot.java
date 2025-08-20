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
    
    public ReplicaMetricsSnapshot(int replicaId,
                                 long writeRequestsTotal, long writeRequestsSuccessful, long writeRequestsFailed,
                                 double writeSuccessRate, double writeAverageLatency,
                                 long readRequestsTotal, long readRequestsSuccessful, long readRequestsFailed,
                                 double readSuccessRate, double readAverageLatency,
                                 boolean healthy, long failureCount, long recoveryCount,
                                 long consecutiveFailures, long consecutiveSuccesses, double uptimeRatio,
                                 long timeSinceLastFailure, long timeSinceLastRecovery,
                                 double overallSuccessRate, double overallAverageLatency,
                                 long timestamp) {
        this.replicaId = replicaId;
        
        this.writeRequestsTotal = writeRequestsTotal;
        this.writeRequestsSuccessful = writeRequestsSuccessful;
        this.writeRequestsFailed = writeRequestsFailed;
        this.writeSuccessRate = writeSuccessRate;
        this.writeAverageLatency = writeAverageLatency;
        
        this.readRequestsTotal = readRequestsTotal;
        this.readRequestsSuccessful = readRequestsSuccessful;
        this.readRequestsFailed = readRequestsFailed;
        this.readSuccessRate = readSuccessRate;
        this.readAverageLatency = readAverageLatency;
        
        this.healthy = healthy;
        this.failureCount = failureCount;
        this.recoveryCount = recoveryCount;
        this.consecutiveFailures = consecutiveFailures;
        this.consecutiveSuccesses = consecutiveSuccesses;
        this.uptimeRatio = uptimeRatio;
        this.timeSinceLastFailure = timeSinceLastFailure;
        this.timeSinceLastRecovery = timeSinceLastRecovery;
        
        this.overallSuccessRate = overallSuccessRate;
        this.overallAverageLatency = overallAverageLatency;
        
        this.timestamp = timestamp;
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
     * Format replica metrics for detailed display
     */
    public String toDetailedString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("=== Replica %d Metrics ===\n", replicaId));
        sb.append(String.format("Status: %s (Healthy: %s)\n", getPerformanceStatus(), healthy));
        sb.append(String.format("Reliability Score: %.2f\n\n", getReliabilityScore()));
        
        sb.append("Write Operations:\n");
        sb.append(String.format("  Total: %d, Successful: %d, Failed: %d\n", 
                                writeRequestsTotal, writeRequestsSuccessful, writeRequestsFailed));
        sb.append(String.format("  Success Rate: %.2f%%, Average Latency: %.2f ms\n\n", 
                                writeSuccessRate * 100, writeAverageLatency));
        
        sb.append("Read Operations:\n");
        sb.append(String.format("  Total: %d, Successful: %d, Failed: %d\n", 
                                readRequestsTotal, readRequestsSuccessful, readRequestsFailed));
        sb.append(String.format("  Success Rate: %.2f%%, Average Latency: %.2f ms\n\n", 
                                readSuccessRate * 100, readAverageLatency));
        
        sb.append("Health Statistics:\n");
        sb.append(String.format("  Total Failures: %d, Recoveries: %d\n", failureCount, recoveryCount));
        sb.append(String.format("  Consecutive Failures: %d, Successes: %d\n", 
                                consecutiveFailures, consecutiveSuccesses));
        sb.append(String.format("  Uptime Ratio: %.2f%%\n", uptimeRatio * 100));
        
        if (timeSinceLastFailure >= 0) {
            sb.append(String.format("  Time Since Last Failure: %d ms\n", timeSinceLastFailure));
        }
        if (timeSinceLastRecovery >= 0) {
            sb.append(String.format("  Time Since Last Recovery: %d ms\n", timeSinceLastRecovery));
        }
        
        sb.append(String.format("\nOverall: %.2f%% success rate, %.2f ms avg latency\n", 
                                overallSuccessRate * 100, overallAverageLatency));
        sb.append(String.format("Timestamp: %d", timestamp));
        
        return sb.toString();
    }
}