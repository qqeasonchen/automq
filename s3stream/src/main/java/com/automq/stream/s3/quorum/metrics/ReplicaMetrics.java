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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Metrics for a single replica in the quorum
 * Tracks operational statistics and health indicators
 */
public class ReplicaMetrics {
    
    private final int replicaId;
    
    // Write operation metrics
    private final AtomicLong writeRequestsTotal = new AtomicLong(0);
    private final AtomicLong writeRequestsSuccessful = new AtomicLong(0);
    private final AtomicLong writeRequestsFailed = new AtomicLong(0);
    private final DoubleAdder writeLatencySum = new DoubleAdder();
    private final AtomicLong writeLatencyCount = new AtomicLong(0);
    
    // Read operation metrics
    private final AtomicLong readRequestsTotal = new AtomicLong(0);
    private final AtomicLong readRequestsSuccessful = new AtomicLong(0);
    private final AtomicLong readRequestsFailed = new AtomicLong(0);
    private final DoubleAdder readLatencySum = new DoubleAdder();
    private final AtomicLong readLatencyCount = new AtomicLong(0);
    
    // Health and failure metrics
    private final AtomicLong failureCount = new AtomicLong(0);
    private final AtomicLong recoveryCount = new AtomicLong(0);
    private final AtomicLong lastFailureTime = new AtomicLong(0);
    private final AtomicLong lastRecoveryTime = new AtomicLong(0);
    private final AtomicLong consecutiveFailures = new AtomicLong(0);
    private final AtomicLong consecutiveSuccesses = new AtomicLong(0);
    
    // Uptime tracking
    private final AtomicLong totalUptimeMs = new AtomicLong(0);
    private final AtomicLong totalDowntimeMs = new AtomicLong(0);
    private volatile long lastStateChangeTime = System.currentTimeMillis();
    private volatile boolean isHealthy = true;
    
    public ReplicaMetrics(int replicaId) {
        this.replicaId = replicaId;
    }
    
    // Write operation recording
    
    public void recordWrite(boolean success, long latencyMs) {
        writeRequestsTotal.incrementAndGet();
        writeLatencySum.add(latencyMs);
        writeLatencyCount.incrementAndGet();
        
        if (success) {
            writeRequestsSuccessful.incrementAndGet();
            consecutiveSuccesses.incrementAndGet();
            consecutiveFailures.set(0);
        } else {
            writeRequestsFailed.incrementAndGet();
            consecutiveFailures.incrementAndGet();
            consecutiveSuccesses.set(0);
        }
    }
    
    // Read operation recording
    
    public void recordRead(boolean success, long latencyMs) {
        readRequestsTotal.incrementAndGet();
        readLatencySum.add(latencyMs);
        readLatencyCount.incrementAndGet();
        
        if (success) {
            readRequestsSuccessful.incrementAndGet();
            consecutiveSuccesses.incrementAndGet();
            consecutiveFailures.set(0);
        } else {
            readRequestsFailed.incrementAndGet();
            consecutiveFailures.incrementAndGet();
            consecutiveSuccesses.set(0);
        }
    }
    
    // Health state recording
    
    public void recordFailure() {
        failureCount.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());
        
        if (isHealthy) {
            updateHealthState(false);
        }
    }
    
    public void recordRecovery() {
        recoveryCount.incrementAndGet();
        lastRecoveryTime.set(System.currentTimeMillis());
        
        if (!isHealthy) {
            updateHealthState(true);
        }
    }
    
    private void updateHealthState(boolean healthy) {
        long currentTime = System.currentTimeMillis();
        long stateDuration = currentTime - lastStateChangeTime;
        
        if (isHealthy) {
            totalUptimeMs.addAndGet(stateDuration);
        } else {
            totalDowntimeMs.addAndGet(stateDuration);
        }
        
        isHealthy = healthy;
        lastStateChangeTime = currentTime;
    }
    
    // Getters
    
    public int getReplicaId() {
        return replicaId;
    }
    
    public long getWriteRequestsTotal() {
        return writeRequestsTotal.get();
    }
    
    public long getWriteRequestsSuccessful() {
        return writeRequestsSuccessful.get();
    }
    
    public long getWriteRequestsFailed() {
        return writeRequestsFailed.get();
    }
    
    public double getWriteSuccessRate() {
        long total = writeRequestsTotal.get();
        return total > 0 ? (double) writeRequestsSuccessful.get() / total : 0.0;
    }
    
    public double getWriteAverageLatency() {
        long count = writeLatencyCount.get();
        return count > 0 ? writeLatencySum.sum() / count : 0.0;
    }
    
    public long getReadRequestsTotal() {
        return readRequestsTotal.get();
    }
    
    public long getReadRequestsSuccessful() {
        return readRequestsSuccessful.get();
    }
    
    public long getReadRequestsFailed() {
        return readRequestsFailed.get();
    }
    
    public double getReadSuccessRate() {
        long total = readRequestsTotal.get();
        return total > 0 ? (double) readRequestsSuccessful.get() / total : 0.0;
    }
    
    public double getReadAverageLatency() {
        long count = readLatencyCount.get();
        return count > 0 ? readLatencySum.sum() / count : 0.0;
    }
    
    public long getFailureCount() {
        return failureCount.get();
    }
    
    public long getRecoveryCount() {
        return recoveryCount.get();
    }
    
    public long getLastFailureTime() {
        return lastFailureTime.get();
    }
    
    public long getLastRecoveryTime() {
        return lastRecoveryTime.get();
    }
    
    public long getConsecutiveFailures() {
        return consecutiveFailures.get();
    }
    
    public long getConsecutiveSuccesses() {
        return consecutiveSuccesses.get();
    }
    
    public boolean isHealthy() {
        return isHealthy;
    }
    
    public long getTotalUptimeMs() {
        long uptime = totalUptimeMs.get();
        if (isHealthy) {
            uptime += System.currentTimeMillis() - lastStateChangeTime;
        }
        return uptime;
    }
    
    public long getTotalDowntimeMs() {
        long downtime = totalDowntimeMs.get();
        if (!isHealthy) {
            downtime += System.currentTimeMillis() - lastStateChangeTime;
        }
        return downtime;
    }
    
    public double getUptimeRatio() {
        long uptime = getTotalUptimeMs();
        long downtime = getTotalDowntimeMs();
        long total = uptime + downtime;
        return total > 0 ? (double) uptime / total : 1.0;
    }
    
    public long getTimeSinceLastFailure() {
        long lastFailure = lastFailureTime.get();
        return lastFailure > 0 ? System.currentTimeMillis() - lastFailure : -1;
    }
    
    public long getTimeSinceLastRecovery() {
        long lastRecovery = lastRecoveryTime.get();
        return lastRecovery > 0 ? System.currentTimeMillis() - lastRecovery : -1;
    }
    
    /**
     * Get overall operation success rate (combines read and write)
     */
    public double getOverallSuccessRate() {
        long totalSuccessful = writeRequestsSuccessful.get() + readRequestsSuccessful.get();
        long totalRequests = writeRequestsTotal.get() + readRequestsTotal.get();
        return totalRequests > 0 ? (double) totalSuccessful / totalRequests : 0.0;
    }
    
    /**
     * Get overall average latency (combines read and write)
     */
    public double getOverallAverageLatency() {
        long totalCount = writeLatencyCount.get() + readLatencyCount.get();
        double totalLatency = writeLatencySum.sum() + readLatencySum.sum();
        return totalCount > 0 ? totalLatency / totalCount : 0.0;
    }
    
    /**
     * Record successful repair operation
     */
    public void recordRepairSuccess() {
        recoveryCount.incrementAndGet();
        lastRecoveryTime.set(System.currentTimeMillis());
        consecutiveFailures.set(0);
        consecutiveSuccesses.incrementAndGet();
        markAsHealthy();
    }
    
    /**
     * Record failed repair operation
     */
    public void recordRepairFailure() {
        failureCount.incrementAndGet();
        lastFailureTime.set(System.currentTimeMillis());
        consecutiveSuccesses.set(0);
        consecutiveFailures.incrementAndGet();
        updateHealthState(false);
    }
    
    /**
     * Mark replica as healthy
     */
    public void markAsHealthy() {
        if (!isHealthy) {
            updateHealthState(true);
        }
    }
    
    /**
     * Mark replica as unhealthy  
     */
    public void markAsUnhealthy() {
        if (isHealthy) {
            updateHealthState(false);
        }
    }
    
    /**
     * Get replica metrics snapshot
     */
    public ReplicaMetricsSnapshot getSnapshot() {
        return ReplicaMetricsSnapshot.builder()
            .replicaId(replicaId)
            .writeMetrics(
                getWriteRequestsTotal(),
                getWriteRequestsSuccessful(),
                getWriteRequestsFailed(),
                getWriteSuccessRate(),
                getWriteAverageLatency()
            )
            .readMetrics(
                getReadRequestsTotal(),
                getReadRequestsSuccessful(),
                getReadRequestsFailed(),
                getReadSuccessRate(),
                getReadAverageLatency()
            )
            .healthMetrics(
                isHealthy(),
                getFailureCount(),
                getRecoveryCount(),
                getConsecutiveFailures(),
                getConsecutiveSuccesses(),
                getUptimeRatio(),
                getTimeSinceLastFailure(),
                getTimeSinceLastRecovery()
            )
            .overallMetrics(
                getOverallSuccessRate(),
                getOverallAverageLatency()
            )
            .timestamp(System.currentTimeMillis())
            .build();
    }
    
    /**
     * Reset all metrics (useful for testing)
     */
    public void reset() {
        writeRequestsTotal.set(0);
        writeRequestsSuccessful.set(0);
        writeRequestsFailed.set(0);
        writeLatencySum.reset();
        writeLatencyCount.set(0);
        
        readRequestsTotal.set(0);
        readRequestsSuccessful.set(0);
        readRequestsFailed.set(0);
        readLatencySum.reset();
        readLatencyCount.set(0);
        
        failureCount.set(0);
        recoveryCount.set(0);
        lastFailureTime.set(0);
        lastRecoveryTime.set(0);
        consecutiveFailures.set(0);
        consecutiveSuccesses.set(0);
        
        totalUptimeMs.set(0);
        totalDowntimeMs.set(0);
        lastStateChangeTime = System.currentTimeMillis();
        isHealthy = true;
    }
    
    @Override
    public String toString() {
        return String.format(
            "ReplicaMetrics{id=%d, healthy=%s, writes=%d/%d(%.1f%%), reads=%d/%d(%.1f%%), failures=%d, uptime=%.1f%%}",
            replicaId,
            isHealthy,
            writeRequestsSuccessful.get(),
            writeRequestsTotal.get(),
            getWriteSuccessRate() * 100,
            readRequestsSuccessful.get(),
            readRequestsTotal.get(),
            getReadSuccessRate() * 100,
            failureCount.get(),
            getUptimeRatio() * 100
        );
    }
}