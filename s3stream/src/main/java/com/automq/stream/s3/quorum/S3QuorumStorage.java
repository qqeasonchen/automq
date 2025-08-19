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
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.state.QuorumState;
import com.automq.stream.utils.FutureUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * S3 Quorum Storage implementation that writes data to 3 replicas
 */
public class S3QuorumStorage implements Storage {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3QuorumStorage.class);
    
    private final QuorumConfig config;
    private final List<Storage> replicas;
    private final QuorumState quorumState;
    private final AtomicLong writeSequence = new AtomicLong(0);
    
    public S3QuorumStorage(QuorumConfig config, List<Storage> replicas) {
        this.config = config;
        this.replicas = replicas;
        this.quorumState = new QuorumState(config.getQuorumSize());
        
        if (replicas.size() != config.getQuorumSize()) {
            throw new IllegalArgumentException("Replica count must match quorum size");
        }
    }

    @Override
    public void startup() {
        LOGGER.info("Starting S3QuorumStorage with {} replicas", replicas.size());
        for (int i = 0; i < replicas.size(); i++) {
            try {
                replicas.get(i).startup();
                LOGGER.info("Replica {} started successfully", i);
            } catch (Exception e) {
                LOGGER.error("Failed to start replica {}", i, e);
                throw new RuntimeException("Failed to start replica " + i, e);
            }
        }
        quorumState.startup();
    }

    @Override
    public void shutdown() {
        LOGGER.info("Shutting down S3QuorumStorage");
        for (int i = 0; i < replicas.size(); i++) {
            try {
                replicas.get(i).shutdown();
                LOGGER.info("Replica {} shut down successfully", i);
            } catch (Exception e) {
                LOGGER.error("Failed to shut down replica {}", i, e);
            }
        }
        quorumState.shutdown();
    }

    @Override
    public CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord) {
        long sequence = writeSequence.incrementAndGet();
        LOGGER.debug("Starting quorum append with sequence {}", sequence);
        
        // Create quorum write request
        QuorumWriteRequest request = new QuorumWriteRequest(sequence, context, streamRecord);
        
        // Execute quorum write with timeout
        return FutureUtil.withTimeout(
            executeQuorumWrite(request),
            config.getWriteTimeoutMs(),
            java.util.concurrent.TimeUnit.MILLISECONDS
        );
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        // For read, we can read from any replica that has the data
        // Prefer primary replica first, then fallback to others
        return FutureUtil.withTimeout(
            executeQuorumRead(context, streamId, startOffset, endOffset, maxBytes),
            config.getReadTimeoutMs(),
            java.util.concurrent.TimeUnit.MILLISECONDS
        );
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

    private void triggerReadRepair(FetchContext context, long streamId, long startOffset, long endOffset, ReadDataBlock data) {
        // Asynchronously repair the primary replica
        CompletableFuture.runAsync(() -> {
            try {
                LOGGER.debug("Triggering read repair for streamId={}, startOffset={}, endOffset={}", 
                    streamId, startOffset, endOffset);
                // This would require implementing a repair mechanism
                // For now, just log that repair would be triggered
                LOGGER.debug("Read repair would be triggered here for primary replica");
            } catch (Exception e) {
                LOGGER.warn("Read repair failed", e);
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
} 