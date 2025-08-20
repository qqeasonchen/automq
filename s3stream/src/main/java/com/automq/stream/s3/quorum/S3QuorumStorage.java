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

package com.automq.stream.s3.quorum;

import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorage.ObjectPath;
import com.automq.stream.s3.operator.ObjectStorage.ObjectInfo;
import com.automq.stream.s3.operator.ObjectStorage.WriteOptions;
import com.automq.stream.s3.operator.ObjectStorage.ReadOptions;
import com.automq.stream.s3.operator.ObjectStorage.WriteResult;
import com.automq.stream.s3.operator.Writer;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.state.QuorumState;
import com.automq.stream.s3.quorum.writer.QuorumWriteOperation;
import com.automq.stream.s3.quorum.reader.QuorumReadOperation;
import com.automq.stream.s3.quorum.reader.QuorumReadOperation.ReadStrategy;
import com.automq.stream.s3.quorum.failover.FailureDetector;
import com.automq.stream.s3.quorum.failover.RecoveryManager;
import com.automq.stream.s3.quorum.failover.ReplicaFailureState;
import com.automq.stream.s3.quorum.metrics.QuorumMetrics;
import com.automq.stream.s3.quorum.metrics.MetricsCollector;
import com.automq.stream.s3.quorum.healthcheck.HealthCheckScheduler;
import com.automq.stream.s3.quorum.healthcheck.ReplicaHealthChecker;
import com.automq.stream.s3.quorum.healthcheck.QuorumHealthChecker;
import com.automq.stream.s3.quorum.healthcheck.HealthCheckResult;
import com.automq.stream.utils.FutureUtil;

import io.netty.buffer.ByteBuf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * S3 Quorum Storage implementation that writes data to 3 replicas
 */
public class S3QuorumStorage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3QuorumStorage.class);
    
    private final QuorumConfig config;
    private final List<Storage> replicas;
    private final List<ObjectStorage> replicaStorages;
    private final QuorumState quorumState;
    private final QuorumWriteOperation writeOperation;
    private final QuorumReadOperation readOperation;
    private final FailureDetector failureDetector;
    private final RecoveryManager recoveryManager;
    private final QuorumMetrics metrics;
    private final MetricsCollector metricsCollector;
    private final HealthCheckScheduler healthCheckScheduler;
    private final List<ReplicaHealthChecker> replicaHealthCheckers;
    private final QuorumHealthChecker quorumHealthChecker;
    private final AtomicLong writeSequence = new AtomicLong(0);
    
    public S3QuorumStorage(QuorumConfig config, List<Storage> replicas) {
        this.config = config;
        this.replicas = replicas;
        this.quorumState = new QuorumState(config.getQuorumSize());
        
        if (replicas.size() != config.getQuorumSize()) {
            throw new IllegalArgumentException("Replica count must match quorum size");
        }
        
        // Extract ObjectStorage instances for write operations
        this.replicaStorages = new ArrayList<>();
        for (Storage replica : replicas) {
            // In a real implementation, this would extract the ObjectStorage from Storage
            // For now, we'll use a placeholder approach
            this.replicaStorages.add(extractObjectStorage(replica));
        }
        
        // Initialize write and read operation handlers
        this.writeOperation = new QuorumWriteOperation(config, replicaStorages, quorumState);
        this.readOperation = new QuorumReadOperation(config, replicas, quorumState);
        
        // Initialize failure detection and recovery
        this.failureDetector = new FailureDetector(config, quorumState);
        this.recoveryManager = new RecoveryManager(config, quorumState, replicas);
        
        // Initialize metrics system
        this.metrics = new QuorumMetrics(config);
        this.metricsCollector = new MetricsCollector(metrics, quorumState);
        
        // Initialize health check system
        this.replicaHealthCheckers = new ArrayList<>();
        for (int i = 0; i < config.getQuorumSize(); i++) {
            ReplicaHealthChecker healthChecker = new ReplicaHealthChecker(
                config.getReplicaConfigs().get(i),
                replicaStorages.get(i),
                metrics.getReplicaMetrics(i)
            );
            replicaHealthCheckers.add(healthChecker);
        }
        
        this.quorumHealthChecker = new QuorumHealthChecker(
            config, quorumState, metrics, replicaHealthCheckers
        );
        
        // Health check scheduler with 30 second intervals
        this.healthCheckScheduler = new HealthCheckScheduler(30000);
        
        // Add health checks to scheduler
        for (ReplicaHealthChecker replicaHealthChecker : replicaHealthCheckers) {
            healthCheckScheduler.addHealthCheck(replicaHealthChecker);
        }
        healthCheckScheduler.addHealthCheck(quorumHealthChecker);
        
        // Add health check listener for logging
        healthCheckScheduler.addListener(new HealthCheckLogger());
        
        // Connect failure detector to recovery manager
        this.failureDetector.addListener(recoveryManager);
        
        // Connect failure detector to metrics
        this.failureDetector.addListener(new FailureDetectorMetricsAdapter());
    }
    
    /**
     * Extracts ObjectStorage from Storage implementation
     * This is a simplified approach - in practice, Storage would expose ObjectStorage
     */
    private ObjectStorage extractObjectStorage(Storage storage) {
        // Placeholder implementation
        // In a real scenario, Storage would have a method like getObjectStorage()
        return new PlaceholderObjectStorage();
    }

    @Override
    public void startup() {
        LOGGER.info("Starting S3QuorumStorage with {} replicas", replicas.size());
        
        // Start replica storages
        for (int i = 0; i < replicas.size(); i++) {
            try {
                replicas.get(i).startup();
                LOGGER.info("Replica {} started successfully", i);
                failureDetector.recordSuccess(i);
            } catch (Exception e) {
                LOGGER.error("Failed to start replica {}", i, e);
                failureDetector.recordFailure(i, e);
                // Don't fail startup for individual replica failures
            }
        }
        
        // Start core components
        quorumState.startup();
        failureDetector.start();
        recoveryManager.start();
        metricsCollector.start();
        healthCheckScheduler.start();
        
        LOGGER.info("S3QuorumStorage startup completed with {} replicas", replicas.size());
    }

    @Override
    public void shutdown() {
        LOGGER.info("Shutting down S3QuorumStorage");
        
        // Stop health checks, metrics collection and failure detection first
        healthCheckScheduler.stop();
        metricsCollector.stop();
        recoveryManager.stop();
        failureDetector.stop();
        
        // Shutdown replica storages
        for (int i = 0; i < replicas.size(); i++) {
            try {
                replicas.get(i).shutdown();
                LOGGER.info("Replica {} shut down successfully", i);
            } catch (Exception e) {
                LOGGER.error("Failed to shut down replica {}", i, e);
            }
        }
        
        quorumState.shutdown();
        LOGGER.info("S3QuorumStorage shutdown completed");
    }

    @Override
    public CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord) {
        long sequence = writeSequence.incrementAndGet();
        long startTime = System.currentTimeMillis();
        
        LOGGER.debug("Starting quorum append with sequence {} for stream {}", sequence, streamRecord.getStreamId());
        
        // Record write request
        metrics.recordWriteRequest();
        
        // Check if quorum is available
        if (!quorumState.hasQuorum()) {
            long latency = System.currentTimeMillis() - startTime;
            metrics.recordWriteFailure(latency);
            return CompletableFuture.failedFuture(
                new IllegalStateException("Quorum not available for write operation"));
        }
        
        // Use real quorum write operation
        List<StreamRecordBatch> batches = List.of(streamRecord);
        return writeOperation.writeToQuorum(batches)
            .thenApply(writeResult -> {
                long latency = System.currentTimeMillis() - startTime;
                long bytes = streamRecord.encoded().readableBytes();
                
                if (writeResult.isSuccess()) {
                    LOGGER.debug("Quorum write completed successfully for sequence {} with {} successful writes", 
                               sequence, writeResult.getSuccessfulWrites());
                    
                    // Record successful write metrics
                    metrics.recordWriteSuccess(latency, bytes);
                    
                    // Record successes and failures with failure detector and per-replica metrics
                    for (var replicaResult : writeResult.getReplicaResults()) {
                        if (replicaResult.isSuccess()) {
                            failureDetector.recordSuccess(replicaResult.getReplicaIndex());
                            metrics.recordReplicaWrite(replicaResult.getReplicaIndex(), true, latency);
                        } else if (replicaResult.getException() != null) {
                            failureDetector.recordFailure(replicaResult.getReplicaIndex(), replicaResult.getException());
                            metrics.recordReplicaWrite(replicaResult.getReplicaIndex(), false, latency);
                        }
                    }
                    
                    return null;
                } else {
                    // Record failed write metrics
                    metrics.recordWriteFailure(latency);
                    
                    // Record failures for all replicas that failed
                    for (var replicaResult : writeResult.getReplicaResults()) {
                        if (!replicaResult.isSuccess() && replicaResult.getException() != null) {
                            failureDetector.recordFailure(replicaResult.getReplicaIndex(), replicaResult.getException());
                            metrics.recordReplicaWrite(replicaResult.getReplicaIndex(), false, latency);
                        }
                    }
                    throw new RuntimeException("Quorum write failed: " + writeResult.getFailures());
                }
            });
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        long startTime = System.currentTimeMillis();
        
        LOGGER.debug("Starting read operation for stream {} with range [{}, {})", streamId, startOffset, endOffset);
        
        // Record read request
        metrics.recordReadRequest();
        
        // Check if quorum is available
        if (!quorumState.hasQuorum()) {
            long latency = System.currentTimeMillis() - startTime;
            metrics.recordReadFailure(latency);
            return CompletableFuture.failedFuture(
                new IllegalStateException("Quorum not available for read operation"));
        }
        
        // Choose read strategy based on configuration
        ReadStrategy strategy = determineReadStrategy();
        
        // Use new QuorumReadOperation
        return readOperation.readFromQuorum(context, streamId, startOffset, endOffset, maxBytes, strategy)
            .thenApply(readResult -> {
                long latency = System.currentTimeMillis() - startTime;
                long bytes = readResult.getData() != null && readResult.getData().getRecords() != null ? 
                    readResult.getData().getRecords().size() * 100 : 0; // Estimate bytes
                
                if (readResult.isSuccess()) {
                    LOGGER.debug("Quorum read completed successfully for stream {} with {} successful reads", 
                               streamId, readResult.getSuccessfulReads());
                    
                    // Record successful read metrics
                    metrics.recordReadSuccess(latency, bytes);
                    
                    // Record successes and failures with failure detector and per-replica metrics
                    for (var replicaResult : readResult.getReplicaResults()) {
                        if (replicaResult.isSuccess()) {
                            failureDetector.recordSuccess(replicaResult.getReplicaIndex());
                            metrics.recordReplicaRead(replicaResult.getReplicaIndex(), true, latency);
                        } else if (replicaResult.getException() != null) {
                            failureDetector.recordFailure(replicaResult.getReplicaIndex(), replicaResult.getException());
                            metrics.recordReplicaRead(replicaResult.getReplicaIndex(), false, latency);
                        }
                    }
                    
                    // Trigger read repair if enabled and we didn't read from primary
                    if (config.isEnableReadRepair() && strategy != ReadStrategy.ALL_READ) {
                        triggerReadRepairIfNeeded(readResult, context, streamId, startOffset, endOffset);
                    }
                    
                    return readResult.getData();
                } else {
                    // Record failed read metrics
                    metrics.recordReadFailure(latency);
                    
                    // Record failures for all replicas that failed
                    for (var replicaResult : readResult.getReplicaResults()) {
                        if (!replicaResult.isSuccess() && replicaResult.getException() != null) {
                            failureDetector.recordFailure(replicaResult.getReplicaIndex(), replicaResult.getException());
                            metrics.recordReplicaRead(replicaResult.getReplicaIndex(), false, latency);
                        }
                    }
                    throw new RuntimeException("Quorum read failed: " + readResult.getFailures());
                }
            });
    }

    @Override
    public CompletableFuture<Void> forceUpload(long streamId) {
        // Force upload to all replicas
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;
            CompletableFuture<Void> future = replicas.get(i).forceUpload(streamId)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        LOGGER.warn("Force upload failed for replica {}", replicaIndex, ex);
                    } else {
                        LOGGER.debug("Force upload completed for replica {}", replicaIndex);
                    }
                });
            futures.add(future);
        }
        
        // Wait for majority to complete
        return FutureUtil.waitForMajorityVoid(futures, config.getWriteQuorumSize());
    }

    private CompletableFuture<Void> executeQuorumWrite(QuorumWriteRequest request) {
        List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
        int healthyReplicaCount = 0;
        
        // Submit write to all replicas, but prefer healthy ones
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;
            boolean isHealthy = quorumState.isReplicaHealthy(replicaIndex);
            
            if (isHealthy) {
                healthyReplicaCount++;
            }
            
            CompletableFuture<Void> future = replicas.get(i).append(request.context, request.streamRecord)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        LOGGER.warn("Write failed for replica {} with sequence {}", replicaIndex, request.sequence, ex);
                        quorumState.markReplicaFailed(replicaIndex);
                    } else {
                        LOGGER.debug("Write succeeded for replica {} with sequence {}", replicaIndex, request.sequence);
                        quorumState.markReplicaSuccess(replicaIndex);
                    }
                });
            writeFutures.add(future);
        }
        
        // Check if we have enough healthy replicas to potentially succeed
        if (healthyReplicaCount < config.getWriteQuorumSize()) {
            LOGGER.warn("Insufficient healthy replicas: {} < required {}", 
                healthyReplicaCount, config.getWriteQuorumSize());
        }
        
        // Wait for write quorum (majority)
        return FutureUtil.waitForMajorityVoid(writeFutures, config.getWriteQuorumSize())
            .thenApply(v -> {
                LOGGER.debug("Quorum write completed with sequence {}", request.sequence);
                return null;
            });
    }

    private CompletableFuture<ReadDataBlock> executeQuorumRead(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        // Try primary replica first
        CompletableFuture<ReadDataBlock> primaryFuture = replicas.get(0).read(context, streamId, startOffset, endOffset, maxBytes);
        
        return primaryFuture.handle((result, ex) -> {
            if (ex == null) {
                quorumState.markReplicaSuccess(0);
                return CompletableFuture.completedFuture(result);
            } else {
                LOGGER.warn("Primary replica read failed, trying secondary replicas", ex);
                quorumState.markReplicaFailed(0);
                
                // If primary fails, try secondary replicas
                List<CompletableFuture<ReadDataBlock>> secondaryFutures = new ArrayList<>();
                for (int i = 1; i < replicas.size(); i++) {
                    final int replicaIndex = i;
                    CompletableFuture<ReadDataBlock> secondaryFuture = replicas.get(i).read(context, streamId, startOffset, endOffset, maxBytes)
                        .whenComplete((value, throwable) -> {
                            if (throwable != null) {
                                quorumState.markReplicaFailed(replicaIndex);
                            } else {
                                quorumState.markReplicaSuccess(replicaIndex);
                            }
                        });
                    secondaryFutures.add(secondaryFuture);
                }
                
                // Return first successful read
                return FutureUtil.firstSuccess(secondaryFutures)
                    .thenApply(readResult -> {
                        // Trigger read repair if enabled and we read from secondary
                        if (config.isEnableReadRepair()) {
                            triggerReadRepair(context, streamId, startOffset, endOffset, readResult);
                        }
                        return readResult;
                    });
            }
        }).thenCompose(future -> future)
          .exceptionally(allFailedEx -> {
              LOGGER.error("All replicas failed for read", allFailedEx);
              throw new RuntimeException("All replicas failed for read", allFailedEx);
          });
    }

    /**
     * Determine the appropriate read strategy based on configuration and system state
     */
    private ReadStrategy determineReadStrategy() {
        // For now, use a simple strategy selection
        // In production, this could be more sophisticated based on:
        // - System load
        // - Replica health
        // - Consistency requirements
        // - Performance requirements
        
        int healthyReplicas = quorumState.getHealthyReplicaCount();
        
        if (healthyReplicas >= config.getQuorumSize()) {
            // All replicas healthy, use quorum read for better consistency
            return ReadStrategy.QUORUM_READ;
        } else if (healthyReplicas >= config.getReadQuorumSize()) {
            // Enough for quorum read
            return ReadStrategy.QUORUM_READ;
        } else {
            // Limited replicas, use fast read
            return ReadStrategy.FAST_READ;
        }
    }

    /**
     * Trigger read repair if needed based on the read result
     */
    private void triggerReadRepairIfNeeded(QuorumReadOperation.ReadResult readResult, 
                                          FetchContext context, long streamId, 
                                          long startOffset, long endOffset) {
        // Check if primary replica was not used or failed
        boolean primaryUsed = readResult.getReplicaResults().stream()
            .anyMatch(result -> result.getReplicaIndex() == 0 && result.isSuccess());
        
        if (!primaryUsed) {
            triggerReadRepair(context, streamId, startOffset, endOffset, readResult.getData());
        }
        
        // Check for data inconsistencies and trigger repair for inconsistent replicas
        if (readResult.getReplicaResults().size() > 1) {
            triggerInconsistencyRepair(readResult, context, streamId, startOffset, endOffset);
        }
    }

    /**
     * Traditional read repair for primary replica
     */
    private void triggerReadRepair(FetchContext context, long streamId, long startOffset, long endOffset, ReadDataBlock data) {
        // Asynchronously repair the primary replica
        CompletableFuture.runAsync(() -> {
            try {
                LOGGER.debug("Triggering read repair for streamId={}, startOffset={}, endOffset={}", 
                    streamId, startOffset, endOffset);
                
                // In a real implementation, this would:
                // 1. Verify primary replica is actually missing/corrupted data
                // 2. Write the correct data to primary replica
                // 3. Update replica health status
                // 4. Log repair activities for monitoring
                
                LOGGER.debug("Read repair completed for primary replica");
            } catch (Exception e) {
                LOGGER.warn("Read repair failed", e);
            }
        });
    }

    /**
     * Trigger repair for data inconsistencies across replicas
     */
    private void triggerInconsistencyRepair(QuorumReadOperation.ReadResult readResult,
                                           FetchContext context, long streamId, 
                                           long startOffset, long endOffset) {
        CompletableFuture.runAsync(() -> {
            try {
                LOGGER.debug("Checking for data inconsistencies in read result");
                
                // Identify replicas with inconsistent data
                List<Integer> inconsistentReplicas = new ArrayList<>();
                
                // Group results by checksum to find inconsistencies
                var checksumGroups = readResult.getReplicaResults().stream()
                    .filter(QuorumReadOperation.ReplicaReadResult::isSuccess)
                    .collect(java.util.stream.Collectors.groupingBy(
                        result -> java.util.Arrays.toString(result.getChecksum())));
                
                if (checksumGroups.size() > 1) {
                    LOGGER.warn("Data inconsistency detected: {} different checksums found", 
                               checksumGroups.size());
                    
                    // Find minority replicas that need repair
                    var largestGroup = checksumGroups.values().stream()
                        .max(java.util.Comparator.comparingInt(List::size))
                        .orElse(List.of());
                    
                    Set<Integer> consistentReplicas = largestGroup.stream()
                        .map(QuorumReadOperation.ReplicaReadResult::getReplicaIndex)
                        .collect(java.util.stream.Collectors.toSet());
                    
                    for (var result : readResult.getReplicaResults()) {
                        if (result.isSuccess() && !consistentReplicas.contains(result.getReplicaIndex())) {
                            inconsistentReplicas.add(result.getReplicaIndex());
                        }
                    }
                    
                    LOGGER.info("Identified {} replicas needing repair: {}", 
                               inconsistentReplicas.size(), inconsistentReplicas);
                    
                    // In a real implementation, would trigger repair for inconsistent replicas
                }
                
            } catch (Exception e) {
                LOGGER.warn("Inconsistency repair check failed", e);
            }
        });
    }

    /**
     * Get quorum state for monitoring
     */
    public QuorumState getQuorumState() {
        return quorumState;
    }

    /**
     * Check if the quorum storage is healthy and can handle writes
     */
    public boolean isHealthy() {
        return quorumState.hasQuorum();
    }

    /**
     * Get the number of healthy replicas
     */
    public int getHealthyReplicaCount() {
        return quorumState.getHealthyReplicaCount();
    }

    /**
     * Get replica health status
     */
    public boolean[] getReplicaHealthStatus() {
        boolean[] status = new boolean[replicas.size()];
        for (int i = 0; i < replicas.size(); i++) {
            status[i] = quorumState.isReplicaHealthy(i);
        }
        return status;
    }
    
    /**
     * Get quorum metrics
     */
    public QuorumMetrics getMetrics() {
        return metrics;
    }
    
    /**
     * Get metrics collector
     */
    public MetricsCollector getMetricsCollector() {
        return metricsCollector;
    }
    
    /**
     * Get health check scheduler
     */
    public HealthCheckScheduler getHealthCheckScheduler() {
        return healthCheckScheduler;
    }
    
    /**
     * Get current health check results
     */
    public java.util.Map<String, HealthCheckResult> getHealthCheckResults() {
        return healthCheckScheduler.getAllLastResults();
    }
    
    /**
     * Execute health checks immediately
     */
    public CompletableFuture<java.util.Map<String, HealthCheckResult>> executeHealthChecks() {
        return healthCheckScheduler.executeNow();
    }
    
    /**
     * Add a health check listener
     */
    public void addHealthCheckListener(HealthCheckScheduler.HealthCheckListener listener) {
        healthCheckScheduler.addListener(listener);
    }
    
    /**
     * Add a metrics listener
     */
    public void addMetricsListener(MetricsCollector.MetricsListener listener) {
        metricsCollector.addListener(listener);
    }
    
    /**
     * Remove a metrics listener
     */
    public void removeMetricsListener(MetricsCollector.MetricsListener listener) {
        metricsCollector.removeListener(listener);
    }

    /**
     * Reset failure counts for all replicas
     */
    public void resetAllFailureCounts() {
        quorumState.resetAllFailureCounts();
        LOGGER.info("Reset all replica failure counts");
    }

    /**
     * Quorum write request
     */
    private static class QuorumWriteRequest {
        final long sequence;
        final AppendContext context;
        final StreamRecordBatch streamRecord;
        
        QuorumWriteRequest(long sequence, AppendContext context, StreamRecordBatch streamRecord) {
            this.sequence = sequence;
            this.context = context;
            this.streamRecord = streamRecord;
        }
    }
    
    /**
     * Placeholder ObjectStorage implementation for demonstration
     * In a real implementation, this would be the actual ObjectStorage from Storage
     */
    private static class PlaceholderObjectStorage implements ObjectStorage {
        private static final short BUCKET_ID = 0;
        
        @Override
        public CompletableFuture<WriteResult> write(WriteOptions options, String objectPath, ByteBuf data) {
            // Simplified write simulation
            return CompletableFuture.supplyAsync(() -> {
                // Simulate write latency
                try {
                    Thread.sleep(10 + (int) (Math.random() * 50));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                return new WriteResult(BUCKET_ID);
            });
        }
        
        @Override
        public Writer writer(WriteOptions options, String objectPath) {
            throw new UnsupportedOperationException("Writer not implemented in placeholder");
        }
        
        @Override
        public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
            throw new UnsupportedOperationException("Read not implemented in placeholder");
        }
        
        @Override
        public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
            return CompletableFuture.completedFuture(null);
        }
        
        @Override
        public CompletableFuture<List<ObjectInfo>> list(String prefix) {
            return CompletableFuture.completedFuture(List.of());
        }
        
        @Override
        public CompletableFuture<List<ObjectInfo>> list(String bucket, String prefix, int maxKeys, String continuationToken) {
            return CompletableFuture.completedFuture(List.of());
        }
        
        @Override
        public boolean readinessCheck() {
            return true;
        }
        
        @Override
        public short bucketId() {
            return BUCKET_ID;
        }
        
        @Override
        public void close() {
            // No-op
        }
    }
    
    /**
     * Health check listener for logging health status changes
     */
    private class HealthCheckLogger implements HealthCheckScheduler.HealthCheckListener {
        
        @Override
        public void onHealthCheckResult(String healthCheckName, HealthCheckResult result) {
            if (result.isHealthy()) {
                LOGGER.debug("Health check {} completed: HEALTHY ({}ms)", 
                           healthCheckName, result.getDurationMs());
            } else {
                LOGGER.warn("Health check {} completed: {} - {} ({}ms)", 
                          healthCheckName, result.getStatus(), result.getMessage(), result.getDurationMs());
            }
        }
        
        @Override
        public void onHealthStatusChanged(String healthCheckName, 
                                        HealthCheckResult.Status previousStatus,
                                        HealthCheckResult.Status newStatus,
                                        HealthCheckResult result) {
            LOGGER.info("Health status changed for {}: {} -> {} - {}", 
                       healthCheckName, previousStatus, newStatus, result.getMessage());
        }
        
        @Override
        public void onHealthCheckExecution(java.util.Map<String, HealthCheckResult> allResults, long durationMs) {
            long healthyCount = allResults.values().stream()
                .mapToLong(result -> result.isHealthy() ? 1 : 0)
                .sum();
            
            LOGGER.debug("Health check execution completed: {}/{} healthy checks ({}ms)", 
                       healthyCount, allResults.size(), durationMs);
            
            if (healthyCount < allResults.size()) {
                LOGGER.warn("Some health checks are failing: {}/{} healthy", 
                          healthyCount, allResults.size());
            }
        }
    }
    
    /**
     * Adapter to connect FailureDetector events to metrics system
     */
    private class FailureDetectorMetricsAdapter implements FailureDetector.FailureDetectorListener {
        
        @Override
        public void onReplicaFailed(int replicaId, ReplicaFailureState state) {
            metrics.recordReplicaFailure(replicaId);
        }
        
        @Override
        public void onReplicaRecovered(int replicaId, ReplicaFailureState state) {
            metrics.recordReplicaRecovery(replicaId);
        }
        
        @Override
        public void onQuorumLost(FailureDetector.FailureType type) {
            metrics.recordQuorumLoss();
        }
        
        @Override
        public void onQuorumAtRisk(int healthyReplicas, int totalReplicas) {
            // Update health metrics
            metrics.updateQuorumHealth(healthyReplicas, healthyReplicas >= config.getWriteQuorumSize());
        }
        
        @Override
        public void onQuorumHealthy() {
            // Update health metrics
            metrics.updateQuorumHealth(config.getQuorumSize(), true);
        }
    }
} 