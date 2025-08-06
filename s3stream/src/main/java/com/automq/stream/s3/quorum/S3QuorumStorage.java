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
        
        // Execute quorum write
        return executeQuorumWrite(request);
    }

    @Override
    public CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        // For read, we can read from any replica that has the data
        // Prefer primary replica first, then fallback to others
        return executeQuorumRead(context, streamId, startOffset, endOffset, maxBytes);
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
        return FutureUtil.waitForMajority(futures, config.getWriteQuorumSize())
            .thenApply(v -> null);
    }

    private CompletableFuture<Void> executeQuorumWrite(QuorumWriteRequest request) {
        List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
        
        // Submit write to all replicas
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;
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
        
        // Wait for write quorum (majority)
        return FutureUtil.waitForMajority(writeFutures, config.getWriteQuorumSize())
            .thenApply(v -> {
                LOGGER.debug("Quorum write completed with sequence {}", request.sequence);
                return null;
            });
    }

    private CompletableFuture<ReadDataBlock> executeQuorumRead(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        // Try primary replica first
        CompletableFuture<ReadDataBlock> primaryFuture = replicas.get(0).read(context, streamId, startOffset, endOffset, maxBytes);
        
        return primaryFuture.exceptionally(ex -> {
            LOGGER.warn("Primary replica read failed, trying secondary replicas", ex);
            
            // If primary fails, try secondary replicas
            List<CompletableFuture<ReadDataBlock>> secondaryFutures = new ArrayList<>();
            for (int i = 1; i < replicas.size(); i++) {
                secondaryFutures.add(replicas.get(i).read(context, streamId, startOffset, endOffset, maxBytes));
            }
            
            // Return first successful read
            return FutureUtil.firstSuccess(secondaryFutures)
                .exceptionally(secondaryEx -> {
                    LOGGER.error("All replicas failed for read", secondaryEx);
                    throw new RuntimeException("All replicas failed for read", secondaryEx);
                }).join();
        });
    }

    /**
     * Get quorum state for monitoring
     */
    public QuorumState getQuorumState() {
        return quorumState;
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