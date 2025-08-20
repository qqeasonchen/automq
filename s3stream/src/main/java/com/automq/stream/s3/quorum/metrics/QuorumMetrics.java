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

import com.automq.stream.s3.quorum.config.QuorumConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Central metrics collector for S3 Quorum Storage
 * Provides comprehensive observability into quorum operations
 */
public class QuorumMetrics {
    
    // Write operation metrics
    private final AtomicLong writeRequestsTotal = new AtomicLong(0);
    private final AtomicLong writeRequestsSuccessful = new AtomicLong(0);
    private final AtomicLong writeRequestsFailed = new AtomicLong(0);
    private final DoubleAdder writeLatencySum = new DoubleAdder();
    private final AtomicLong writeLatencyCount = new AtomicLong(0);
    private final AtomicLong writeBytesTotal = new AtomicLong(0);
    
    // Read operation metrics
    private final AtomicLong readRequestsTotal = new AtomicLong(0);
    private final AtomicLong readRequestsSuccessful = new AtomicLong(0);
    private final AtomicLong readRequestsFailed = new AtomicLong(0);
    private final DoubleAdder readLatencySum = new DoubleAdder();
    private final AtomicLong readLatencyCount = new AtomicLong(0);
    private final AtomicLong readBytesTotal = new AtomicLong(0);
    
    // Quorum health metrics
    private final AtomicLong healthyReplicas = new AtomicLong(0);
    private final AtomicLong totalReplicas = new AtomicLong(0);
    private final AtomicLong quorumAvailableTime = new AtomicLong(0);
    private final AtomicLong quorumUnavailableTime = new AtomicLong(0);
    
    // Failure and recovery metrics
    private final AtomicLong replicaFailuresTotal = new AtomicLong(0);
    private final AtomicLong replicaRecoveriesTotal = new AtomicLong(0);
    private final AtomicLong emergencyRecoveriesTotal = new AtomicLong(0);
    private final AtomicLong quorumLossEvents = new AtomicLong(0);
    
    // Per-replica metrics
    private final Map<Integer, ReplicaMetrics> replicaMetrics = new ConcurrentHashMap<>();
    
    // Configuration
    private final QuorumConfig config;
    
    // Timing state
    private volatile long lastHealthyTime = System.currentTimeMillis();
    private volatile boolean wasHealthy = true;
    
    public QuorumMetrics(QuorumConfig config) {
        this.config = config;
        this.totalReplicas.set(config.getQuorumSize());
        
        // Initialize per-replica metrics
        for (int i = 0; i < config.getQuorumSize(); i++) {
            replicaMetrics.put(i, new ReplicaMetrics(i));
        }
    }
    
    // Write operation metrics
    
    public void recordWriteRequest() {
        writeRequestsTotal.incrementAndGet();
    }
    
    public void recordWriteSuccess(long latencyMs, long bytes) {
        writeRequestsSuccessful.incrementAndGet();
        writeLatencySum.add(latencyMs);
        writeLatencyCount.incrementAndGet();
        writeBytesTotal.addAndGet(bytes);
    }
    
    public void recordWriteFailure(long latencyMs) {
        writeRequestsFailed.incrementAndGet();
        writeLatencySum.add(latencyMs);
        writeLatencyCount.incrementAndGet();
    }
    
    // Read operation metrics
    
    public void recordReadRequest() {
        readRequestsTotal.incrementAndGet();
    }
    
    public void recordReadSuccess(long latencyMs, long bytes) {
        readRequestsSuccessful.incrementAndGet();
        readLatencySum.add(latencyMs);
        readLatencyCount.incrementAndGet();
        readBytesTotal.addAndGet(bytes);
    }
    
    public void recordReadFailure(long latencyMs) {
        readRequestsFailed.incrementAndGet();
        readLatencySum.add(latencyMs);
        readLatencyCount.incrementAndGet();
    }
    
    // Quorum health metrics
    
    public void updateQuorumHealth(int healthyCount, boolean hasQuorum) {
        healthyReplicas.set(healthyCount);
        
        long currentTime = System.currentTimeMillis();
        if (wasHealthy != hasQuorum) {
            long timeDiff = currentTime - lastHealthyTime;
            if (wasHealthy) {
                quorumAvailableTime.addAndGet(timeDiff);
            } else {
                quorumUnavailableTime.addAndGet(timeDiff);
            }
            wasHealthy = hasQuorum;
            lastHealthyTime = currentTime;
        }
    }
    
    // Failure and recovery metrics
    
    public void recordReplicaFailure(int replicaId) {
        replicaFailuresTotal.incrementAndGet();
        ReplicaMetrics replica = replicaMetrics.get(replicaId);
        if (replica != null) {
            replica.recordFailure();
        }
    }
    
    public void recordReplicaRecovery(int replicaId) {
        replicaRecoveriesTotal.incrementAndGet();
        ReplicaMetrics replica = replicaMetrics.get(replicaId);
        if (replica != null) {
            replica.recordRecovery();
        }
    }
    
    public void recordEmergencyRecovery() {
        emergencyRecoveriesTotal.incrementAndGet();
    }
    
    public void recordQuorumLoss() {
        quorumLossEvents.incrementAndGet();
    }
    
    // Per-replica operation metrics
    
    public void recordReplicaWrite(int replicaId, boolean success, long latencyMs) {
        ReplicaMetrics replica = replicaMetrics.get(replicaId);
        if (replica != null) {
            replica.recordWrite(success, latencyMs);
        }
    }
    
    public void recordReplicaRead(int replicaId, boolean success, long latencyMs) {
        ReplicaMetrics replica = replicaMetrics.get(replicaId);
        if (replica != null) {
            replica.recordRead(success, latencyMs);
        }
    }
    
    // Getters for metric values
    
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
    
    public long getWriteBytesTotal() {
        return writeBytesTotal.get();
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
    
    public long getReadBytesTotal() {
        return readBytesTotal.get();
    }
    
    public long getHealthyReplicas() {
        return healthyReplicas.get();
    }
    
    public long getTotalReplicas() {
        return totalReplicas.get();
    }
    
    public double getQuorumHealthRatio() {
        return (double) healthyReplicas.get() / totalReplicas.get();
    }
    
    public long getQuorumAvailableTime() {
        // Include current period if healthy
        long current = quorumAvailableTime.get();
        if (wasHealthy) {
            current += System.currentTimeMillis() - lastHealthyTime;
        }
        return current;
    }
    
    public long getQuorumUnavailableTime() {
        // Include current period if unhealthy
        long current = quorumUnavailableTime.get();
        if (!wasHealthy) {
            current += System.currentTimeMillis() - lastHealthyTime;
        }
        return current;
    }
    
    public double getQuorumAvailability() {
        long available = getQuorumAvailableTime();
        long unavailable = getQuorumUnavailableTime();
        long total = available + unavailable;
        return total > 0 ? (double) available / total : 1.0;
    }
    
    public long getReplicaFailuresTotal() {
        return replicaFailuresTotal.get();
    }
    
    public long getReplicaRecoveriesTotal() {
        return replicaRecoveriesTotal.get();
    }
    
    public long getEmergencyRecoveriesTotal() {
        return emergencyRecoveriesTotal.get();
    }
    
    public long getQuorumLossEvents() {
        return quorumLossEvents.get();
    }
    
    public ReplicaMetrics getReplicaMetrics(int replicaId) {
        return replicaMetrics.get(replicaId);
    }
    
    public Map<Integer, ReplicaMetrics> getAllReplicaMetrics() {
        return new ConcurrentHashMap<>(replicaMetrics);
    }
    
    /**
     * Get a comprehensive metrics snapshot
     */
    public MetricsSnapshot getSnapshot() {
        return new MetricsSnapshot(
            // Write metrics
            getWriteRequestsTotal(),
            getWriteRequestsSuccessful(),
            getWriteRequestsFailed(),
            getWriteSuccessRate(),
            getWriteAverageLatency(),
            getWriteBytesTotal(),
            
            // Read metrics
            getReadRequestsTotal(),
            getReadRequestsSuccessful(),
            getReadRequestsFailed(),
            getReadSuccessRate(),
            getReadAverageLatency(),
            getReadBytesTotal(),
            
            // Health metrics
            getHealthyReplicas(),
            getTotalReplicas(),
            getQuorumHealthRatio(),
            getQuorumAvailability(),
            
            // Failure metrics
            getReplicaFailuresTotal(),
            getReplicaRecoveriesTotal(),
            getEmergencyRecoveriesTotal(),
            getQuorumLossEvents(),
            
            // Timestamp
            System.currentTimeMillis()
        );
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
        writeBytesTotal.set(0);
        
        readRequestsTotal.set(0);
        readRequestsSuccessful.set(0);
        readRequestsFailed.set(0);
        readLatencySum.reset();
        readLatencyCount.set(0);
        readBytesTotal.set(0);
        
        healthyReplicas.set(config.getQuorumSize());
        quorumAvailableTime.set(0);
        quorumUnavailableTime.set(0);
        
        replicaFailuresTotal.set(0);
        replicaRecoveriesTotal.set(0);
        emergencyRecoveriesTotal.set(0);
        quorumLossEvents.set(0);
        
        lastHealthyTime = System.currentTimeMillis();
        wasHealthy = true;
        
        replicaMetrics.values().forEach(ReplicaMetrics::reset);
    }
}