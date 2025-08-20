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
 */
public class MetricsSnapshot {
    
    // Write metrics
    private final long writeRequestsTotal;
    private final long writeRequestsSuccessful;
    private final long writeRequestsFailed;
    private final double writeSuccessRate;
    private final double writeAverageLatency;
    private final long writeBytesTotal;
    
    // Read metrics
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
    
    private MetricsSnapshot(Builder builder) {
        this.writeRequestsTotal = builder.writeRequestsTotal;
        this.writeRequestsSuccessful = builder.writeRequestsSuccessful;
        this.writeRequestsFailed = builder.writeRequestsFailed;
        this.writeSuccessRate = builder.writeSuccessRate;
        this.writeAverageLatency = builder.writeAverageLatency;
        this.writeBytesTotal = builder.writeBytesTotal;
        
        this.readRequestsTotal = builder.readRequestsTotal;
        this.readRequestsSuccessful = builder.readRequestsSuccessful;
        this.readRequestsFailed = builder.readRequestsFailed;
        this.readSuccessRate = builder.readSuccessRate;
        this.readAverageLatency = builder.readAverageLatency;
        this.readBytesTotal = builder.readBytesTotal;
        
        this.healthyReplicas = builder.healthyReplicas;
        this.totalReplicas = builder.totalReplicas;
        this.quorumHealthRatio = builder.quorumHealthRatio;
        this.quorumAvailability = builder.quorumAvailability;
        
        this.replicaFailuresTotal = builder.replicaFailuresTotal;
        this.replicaRecoveriesTotal = builder.replicaRecoveriesTotal;
        this.emergencyRecoveriesTotal = builder.emergencyRecoveriesTotal;
        this.quorumLossEvents = builder.quorumLossEvents;
        
        this.timestamp = builder.timestamp;
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    // Getters
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
    
    public long getTimestamp() {
        return timestamp;
    }
    
    // Computed metrics
    public long getTotalRequests() {
        return writeRequestsTotal + readRequestsTotal;
    }
    
    public long getTotalBytes() {
        return writeBytesTotal + readBytesTotal;
    }
    
    public long getTotalBytesProcessed() {
        return getTotalBytes();
    }
    
    public long getTotalSuccessfulRequests() {
        return writeRequestsSuccessful + readRequestsSuccessful;
    }
    
    public boolean isHealthy() {
        return getHealthScore() > 0.8;
    }
    
    public String getHealthStatus() {
        double score = getHealthScore();
        if (score >= 0.9) {
            return "HEALTHY";
        } else if (score >= 0.7) {
            return "DEGRADED";
        } else {
            return "UNHEALTHY";
        }
    }
    
    public String toDetailedString() {
        StringBuilder sb = new StringBuilder();
        sb.append("=== Quorum Metrics Summary ===\n");
        sb.append(String.format("Health Score: %.2f (%s)\n", getHealthScore(), getHealthStatus()));
        sb.append(String.format("Availability: %.1f%%\n\n", quorumAvailability * 100));
        
        sb.append("Operations:\n");
        sb.append(String.format("  Total Requests: %d (Successful: %d, Failed: %d)\n",
                                getTotalRequests(), getTotalSuccessfulRequests(),
                                writeRequestsFailed + readRequestsFailed));
        sb.append(String.format("  Overall Success Rate: %.2f%%\n", getOverallSuccessRate() * 100));
        sb.append(String.format("  Average Latency: %.2f ms\n\n", getOverallAverageLatency()));
        
        sb.append("Write Operations:\n");
        sb.append(String.format("  Total: %d, Success: %d, Failed: %d\n",
                                writeRequestsTotal, writeRequestsSuccessful, writeRequestsFailed));
        sb.append(String.format("  Success Rate: %.2f%%, Avg Latency: %.2f ms\n",
                                writeSuccessRate * 100, writeAverageLatency));
        sb.append(String.format("  Bytes Written: %d\n\n", writeBytesTotal));
        
        sb.append("Read Operations:\n");
        sb.append(String.format("  Total: %d, Success: %d, Failed: %d\n",
                                readRequestsTotal, readRequestsSuccessful, readRequestsFailed));
        sb.append(String.format("  Success Rate: %.2f%%, Avg Latency: %.2f ms\n",
                                readSuccessRate * 100, readAverageLatency));
        sb.append(String.format("  Bytes Read: %d\n\n", readBytesTotal));
        
        sb.append("Quorum Health:\n");
        sb.append(String.format("  Healthy Replicas: %d/%d (%.1f%%)\n",
                                healthyReplicas, totalReplicas, quorumHealthRatio * 100));
        sb.append(String.format("  Failures: %d, Recoveries: %d\n",
                                replicaFailuresTotal, replicaRecoveriesTotal));
        sb.append(String.format("  Emergency Recoveries: %d, Quorum Loss Events: %d\n",
                                emergencyRecoveriesTotal, quorumLossEvents));
        
        sb.append(String.format("\nTimestamp: %d", timestamp));
        return sb.toString();
    }
    
    public double getOverallSuccessRate() {
        long totalRequests = getTotalRequests();
        if (totalRequests == 0) {
            return 0.0;
        }
        long totalSuccessful = writeRequestsSuccessful + readRequestsSuccessful;
        return (double) totalSuccessful / totalRequests;
    }
    
    public double getOverallAverageLatency() {
        long totalRequests = getTotalRequests();
        if (totalRequests == 0) {
            return 0.0;
        }
        double totalLatency = (writeAverageLatency * writeRequestsTotal) + 
                             (readAverageLatency * readRequestsTotal);
        return totalLatency / totalRequests;
    }
    
    public double getFailureRecoveryRatio() {
        if (replicaFailuresTotal == 0) {
            return 1.0;
        }
        return (double) replicaRecoveriesTotal / replicaFailuresTotal;
    }
    
    /**
     * Get overall health score (0.0 to 1.0)
     */
    public double getHealthScore() {
        double quorumHealthWeight = 0.4;
        double successRateWeight = 0.3;
        double availabilityWeight = 0.2;
        double recoveryWeight = 0.1;
        
        double successRateScore = getOverallSuccessRate();
        double recoveryScore = getFailureRecoveryRatio();
        
        return (quorumHealthRatio * quorumHealthWeight) +
               (successRateScore * successRateWeight) +
               (quorumAvailability * availabilityWeight) +
               (recoveryScore * recoveryWeight);
    }
    
    @Override
    public String toString() {
        return String.format(
            "MetricsSnapshot{requests=%d/%d(%.1f%%), quorum=%d/%d(%.1f%%), " +
            "health=%.2f, availability=%.1f%%, failures=%d, recoveries=%d}",
            writeRequestsSuccessful + readRequestsSuccessful,
            getTotalRequests(),
            getOverallSuccessRate() * 100,
            healthyReplicas,
            totalReplicas,
            quorumHealthRatio * 100,
            getHealthScore(),
            quorumAvailability * 100,
            replicaFailuresTotal,
            replicaRecoveriesTotal
        );
    }
    
    /**
     * Builder pattern for MetricsSnapshot
     */
    public static class Builder {
        private long writeRequestsTotal;
        private long writeRequestsSuccessful;
        private long writeRequestsFailed;
        private double writeSuccessRate;
        private double writeAverageLatency;
        private long writeBytesTotal;
        private long readRequestsTotal;
        private long readRequestsSuccessful;
        private long readRequestsFailed;
        private double readSuccessRate;
        private double readAverageLatency;
        private long readBytesTotal;
        private long healthyReplicas;
        private long totalReplicas;
        private double quorumHealthRatio;
        private double quorumAvailability;
        private long replicaFailuresTotal;
        private long replicaRecoveriesTotal;
        private long emergencyRecoveriesTotal;
        private long quorumLossEvents;
        private long timestamp;
        
        public Builder writeMetrics(long total, long successful, long failed,
                                  double successRate, double avgLatency, long bytes) {
            this.writeRequestsTotal = total;
            this.writeRequestsSuccessful = successful;
            this.writeRequestsFailed = failed;
            this.writeSuccessRate = successRate;
            this.writeAverageLatency = avgLatency;
            this.writeBytesTotal = bytes;
            return this;
        }
        
        public Builder readMetrics(long total, long successful, long failed,
                                 double successRate, double avgLatency, long bytes) {
            this.readRequestsTotal = total;
            this.readRequestsSuccessful = successful;
            this.readRequestsFailed = failed;
            this.readSuccessRate = successRate;
            this.readAverageLatency = avgLatency;
            this.readBytesTotal = bytes;
            return this;
        }
        
        public Builder healthMetrics(long healthy, long total, double healthRatio, 
                                   double availability) {
            this.healthyReplicas = healthy;
            this.totalReplicas = total;
            this.quorumHealthRatio = healthRatio;
            this.quorumAvailability = availability;
            return this;
        }
        
        public Builder failureMetrics(long failures, long recoveries, 
                                    long emergencyRecoveries, long quorumLoss) {
            this.replicaFailuresTotal = failures;
            this.replicaRecoveriesTotal = recoveries;
            this.emergencyRecoveriesTotal = emergencyRecoveries;
            this.quorumLossEvents = quorumLoss;
            return this;
        }
        
        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public MetricsSnapshot build() {
            return new MetricsSnapshot(this);
        }
    }
}