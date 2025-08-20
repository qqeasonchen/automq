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
 * Immutable snapshot of QuorumMetrics at a specific point in time
 * Provides a consistent view of all metrics for reporting and monitoring
 */
public class MetricsSnapshot {
    
    // Write operation metrics
    private final long writeRequestsTotal;
    private final long writeRequestsSuccessful;
    private final long writeRequestsFailed;
    private final double writeSuccessRate;
    private final double writeAverageLatency;
    private final long writeBytesTotal;
    
    // Read operation metrics
    private final long readRequestsTotal;
    private final long readRequestsSuccessful;
    private final long readRequestsFailed;
    private final double readSuccessRate;
    private final double readAverageLatency;
    private final long readBytesTotal;
    
    // Health metrics
    private final long healthyReplicas;
    private final long totalReplicas;
    private final double quorumHealthRatio;
    private final double quorumAvailability;
    
    // Failure metrics
    private final long replicaFailuresTotal;
    private final long replicaRecoveriesTotal;
    private final long emergencyRecoveriesTotal;
    private final long quorumLossEvents;
    
    // Metadata
    private final long timestamp;
    
    public MetricsSnapshot(long writeRequestsTotal, long writeRequestsSuccessful, long writeRequestsFailed,
                          double writeSuccessRate, double writeAverageLatency, long writeBytesTotal,
                          long readRequestsTotal, long readRequestsSuccessful, long readRequestsFailed,
                          double readSuccessRate, double readAverageLatency, long readBytesTotal,
                          long healthyReplicas, long totalReplicas, double quorumHealthRatio,
                          double quorumAvailability, long replicaFailuresTotal, long replicaRecoveriesTotal,
                          long emergencyRecoveriesTotal, long quorumLossEvents, long timestamp) {
        this.writeRequestsTotal = writeRequestsTotal;
        this.writeRequestsSuccessful = writeRequestsSuccessful;
        this.writeRequestsFailed = writeRequestsFailed;
        this.writeSuccessRate = writeSuccessRate;
        this.writeAverageLatency = writeAverageLatency;
        this.writeBytesTotal = writeBytesTotal;
        
        this.readRequestsTotal = readRequestsTotal;
        this.readRequestsSuccessful = readRequestsSuccessful;
        this.readRequestsFailed = readRequestsFailed;
        this.readSuccessRate = readSuccessRate;
        this.readAverageLatency = readAverageLatency;
        this.readBytesTotal = readBytesTotal;
        
        this.healthyReplicas = healthyReplicas;
        this.totalReplicas = totalReplicas;
        this.quorumHealthRatio = quorumHealthRatio;
        this.quorumAvailability = quorumAvailability;
        
        this.replicaFailuresTotal = replicaFailuresTotal;
        this.replicaRecoveriesTotal = replicaRecoveriesTotal;
        this.emergencyRecoveriesTotal = emergencyRecoveriesTotal;
        this.quorumLossEvents = quorumLossEvents;
        
        this.timestamp = timestamp;
    }
    
    // Write metrics getters
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
    
    public long getWriteBytesTotal() {
        return writeBytesTotal;
    }
    
    // Read metrics getters
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
    
    public long getReadBytesTotal() {
        return readBytesTotal;
    }
    
    // Health metrics getters
    public long getHealthyReplicas() {
        return healthyReplicas;
    }
    
    public long getTotalReplicas() {
        return totalReplicas;
    }
    
    public double getQuorumHealthRatio() {
        return quorumHealthRatio;
    }
    
    public double getQuorumAvailability() {
        return quorumAvailability;
    }
    
    // Failure metrics getters
    public long getReplicaFailuresTotal() {
        return replicaFailuresTotal;
    }
    
    public long getReplicaRecoveriesTotal() {
        return replicaRecoveriesTotal;
    }
    
    public long getEmergencyRecoveriesTotal() {
        return emergencyRecoveriesTotal;
    }
    
    public long getQuorumLossEvents() {
        return quorumLossEvents;
    }
    
    // Metadata getters
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
    
    public double getOverallSuccessRate() {
        long total = getTotalRequests();
        return total > 0 ? (double) getTotalSuccessfulRequests() / total : 0.0;
    }
    
    public double getOverallAverageLatency() {
        // Weighted average based on request counts
        long writeCount = writeRequestsTotal;
        long readCount = readRequestsTotal;
        long totalCount = writeCount + readCount;
        
        if (totalCount == 0) {
            return 0.0;
        }
        
        double weightedLatency = (writeAverageLatency * writeCount) + (readAverageLatency * readCount);
        return weightedLatency / totalCount;
    }
    
    public long getTotalBytesProcessed() {
        return writeBytesTotal + readBytesTotal;
    }
    
    /**
     * Get throughput in requests per second
     * Note: This requires time window information for accurate calculation
     */
    public double getRequestThroughput(long timeWindowMs) {
        if (timeWindowMs <= 0) {
            return 0.0;
        }
        return (double) getTotalRequests() / (timeWindowMs / 1000.0);
    }
    
    /**
     * Get throughput in bytes per second
     */
    public double getByteThroughput(long timeWindowMs) {
        if (timeWindowMs <= 0) {
            return 0.0;
        }
        return (double) getTotalBytesProcessed() / (timeWindowMs / 1000.0);
    }
    
    /**
     * Check if system is operating normally
     */
    public boolean isHealthy() {
        return quorumHealthRatio >= 0.67 && // At least 2/3 replicas healthy
               getOverallSuccessRate() >= 0.95 && // At least 95% success rate
               quorumAvailability >= 0.99; // At least 99% availability
    }
    
    /**
     * Get health status summary
     */
    public String getHealthStatus() {
        if (quorumLossEvents > 0) {
            return "CRITICAL - Quorum loss detected";
        } else if (quorumHealthRatio < 0.5) {
            return "CRITICAL - Majority replicas unhealthy";
        } else if (quorumHealthRatio < 0.67) {
            return "WARNING - Reduced replica availability";
        } else if (getOverallSuccessRate() < 0.95) {
            return "WARNING - High error rate";
        } else if (quorumAvailability < 0.99) {
            return "WARNING - Reduced availability";
        } else {
            return "HEALTHY";
        }
    }
    
    @Override
    public String toString() {
        return String.format(
            "MetricsSnapshot{" +
            "writes=%d/%d(%.1f%%), reads=%d/%d(%.1f%%), " +
            "replicas=%d/%d(%.1f%%), availability=%.1f%%, " +
            "failures=%d, recoveries=%d, status=%s, ts=%d}",
            writeRequestsSuccessful, writeRequestsTotal, writeSuccessRate * 100,
            readRequestsSuccessful, readRequestsTotal, readSuccessRate * 100,
            healthyReplicas, totalReplicas, quorumHealthRatio * 100,
            quorumAvailability * 100,
            replicaFailuresTotal, replicaRecoveriesTotal,
            getHealthStatus(),
            timestamp
        );
    }
    
    /**
     * Format metrics for human-readable display
     */
    public String toDetailedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== S3 Quorum Storage Metrics ===\n");
        sb.append(String.format("Timestamp: %d\n", timestamp));
        sb.append(String.format("Status: %s\n\n", getHealthStatus()));
        
        sb.append("Write Operations:\n");
        sb.append(String.format("  Total: %d, Successful: %d, Failed: %d\n", 
                                writeRequestsTotal, writeRequestsSuccessful, writeRequestsFailed));
        sb.append(String.format("  Success Rate: %.2f%%, Average Latency: %.2f ms\n", 
                                writeSuccessRate * 100, writeAverageLatency));
        sb.append(String.format("  Total Bytes: %d\n\n", writeBytesTotal));
        
        sb.append("Read Operations:\n");
        sb.append(String.format("  Total: %d, Successful: %d, Failed: %d\n", 
                                readRequestsTotal, readRequestsSuccessful, readRequestsFailed));
        sb.append(String.format("  Success Rate: %.2f%%, Average Latency: %.2f ms\n", 
                                readSuccessRate * 100, readAverageLatency));
        sb.append(String.format("  Total Bytes: %d\n\n", readBytesTotal));
        
        sb.append("Quorum Health:\n");
        sb.append(String.format("  Healthy Replicas: %d/%d (%.1f%%)\n", 
                                healthyReplicas, totalReplicas, quorumHealthRatio * 100));
        sb.append(String.format("  Availability: %.2f%%\n\n", quorumAvailability * 100));
        
        sb.append("Failure Recovery:\n");
        sb.append(String.format("  Replica Failures: %d, Recoveries: %d\n", 
                                replicaFailuresTotal, replicaRecoveriesTotal));
        sb.append(String.format("  Emergency Recoveries: %d, Quorum Loss Events: %d\n", 
                                emergencyRecoveriesTotal, quorumLossEvents));
        
        return sb.toString();
    }
}