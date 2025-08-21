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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.metadata.S3ObjectMetadata;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QuorumWriter coordinates writes across multiple replica writers to achieve quorum-based durability.
 * It ensures that data is written to at least writeQuorumSize replicas before considering the write successful.
 */
public class QuorumWriter implements Writer {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumWriter.class);
    
    private final List<Writer> replicaWriters;
    private final int writeQuorumSize;
    private final String objectPath;
    private final AtomicInteger successfulWrites = new AtomicInteger(0);
    private final AtomicInteger failedWrites = new AtomicInteger(0);
    
    public QuorumWriter(List<Writer> replicaWriters, int writeQuorumSize, String objectPath) {
        this.replicaWriters = replicaWriters;
        this.writeQuorumSize = writeQuorumSize;
        this.objectPath = objectPath;
        
        if (replicaWriters.isEmpty()) {
            throw new IllegalArgumentException("At least one replica writer is required");
        }
        
        if (writeQuorumSize > replicaWriters.size()) {
            throw new IllegalArgumentException("Write quorum size cannot exceed number of replicas");
        }
        
        LOGGER.debug("QuorumWriter created for object {} with {} replicas, quorum size: {}", 
                    objectPath, replicaWriters.size(), writeQuorumSize);
    }
    
    @Override
    public CompletableFuture<Void> write(ByteBuf data) {
        LOGGER.debug("QuorumWriter writing {} bytes to {} replicas for object {}", 
                    data.readableBytes(), replicaWriters.size(), objectPath);
        
        // Create write futures for all replicas
        List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
        
        for (int i = 0; i < replicaWriters.size(); i++) {
            final int replicaIndex = i;
            Writer replicaWriter = replicaWriters.get(i);
            
            // Create an independent copy of the data for each replica
            ByteBuf dataCopy = data.alloc().buffer(data.readableBytes());
            dataCopy.writeBytes(data, data.readerIndex(), data.readableBytes());
            
            CompletableFuture<Void> writeFuture = replicaWriter.write(dataCopy)
                .whenComplete((result, throwable) -> {
                    // Always release the data copy
                    dataCopy.release();
                    
                    if (throwable == null) {
                        int successCount = successfulWrites.incrementAndGet();
                        LOGGER.debug("Replica {} write succeeded for object {}, success count: {}", 
                                   replicaIndex, objectPath, successCount);
                    } else {
                        int failCount = failedWrites.incrementAndGet();
                        LOGGER.warn("Replica {} write failed for object {}, fail count: {}, error: {}", 
                                  replicaIndex, objectPath, failCount, throwable.getMessage());
                    }
                });
            
            writeFutures.add(writeFuture);
        }
        
        // Wait for write quorum to be satisfied
        return waitForWriteQuorum(writeFutures);
    }
    
    @Override
    public void copyOnWrite() {
        // Apply copyOnWrite to all replica writers
        for (Writer replicaWriter : replicaWriters) {
            replicaWriter.copyOnWrite();
        }
    }
    
    @Override
    public void copyWrite(S3ObjectMetadata s3ObjectMetadata, long start, long end) {
        // Apply copyWrite to all replica writers
        for (Writer replicaWriter : replicaWriters) {
            replicaWriter.copyWrite(s3ObjectMetadata, start, end);
        }
    }
    
    @Override
    public boolean hasBatchingPart() {
        // Return true if any replica writer has batching parts
        return replicaWriters.stream().anyMatch(Writer::hasBatchingPart);
    }
    
    @Override
    public CompletableFuture<Void> close() {
        LOGGER.debug("Closing QuorumWriter for object {}", objectPath);
        
        // Close all replica writers
        List<CompletableFuture<Void>> closeFutures = replicaWriters.stream()
            .map(Writer::close)
            .collect(Collectors.toList());
        
        // Wait for quorum of close operations to succeed
        return waitForWriteQuorum(closeFutures);
    }
    
    @Override
    public CompletableFuture<Void> release() {
        LOGGER.debug("Releasing QuorumWriter for object {}", objectPath);
        
        // Release all replica writers
        List<CompletableFuture<Void>> releaseFutures = replicaWriters.stream()
            .map(Writer::release)
            .collect(Collectors.toList());
        
        // Wait for all releases to complete (best effort)
        return CompletableFuture.allOf(releaseFutures.toArray(new CompletableFuture[0]))
            .exceptionally(throwable -> {
                LOGGER.warn("Some replica writers failed to release: {}", throwable.getMessage());
                return null;
            });
    }
    
    @Override
    public short bucketId() {
        // Return the bucket ID of the first replica writer
        return replicaWriters.isEmpty() ? 0 : replicaWriters.get(0).bucketId();
    }
    
    /**
     * Wait for write quorum to be satisfied
     */
    private CompletableFuture<Void> waitForWriteQuorum(List<CompletableFuture<Void>> futures) {
        CompletableFuture<Void> quorumFuture = new CompletableFuture<>();
        AtomicInteger completedCount = new AtomicInteger(0);
        AtomicInteger successCount = new AtomicInteger(0);
        
        for (CompletableFuture<Void> future : futures) {
            future.whenComplete((result, throwable) -> {
                int completed = completedCount.incrementAndGet();
                
                if (throwable == null) {
                    int success = successCount.incrementAndGet();
                    // Check if we've achieved write quorum
                    if (success >= writeQuorumSize && !quorumFuture.isDone()) {
                        LOGGER.debug("Write quorum achieved for object {}: {}/{} replicas succeeded", 
                                   objectPath, success, replicaWriters.size());
                        quorumFuture.complete(null);
                    }
                }
                
                // Check if we can never achieve quorum
                int maxPossibleSuccess = successCount.get() + (futures.size() - completed);
                if (maxPossibleSuccess < writeQuorumSize && !quorumFuture.isDone()) {
                    String errorMsg = String.format("Write quorum cannot be achieved for object %s: " +
                                                   "only %d/%d replicas can succeed (required: %d)", 
                                                   objectPath, maxPossibleSuccess, replicaWriters.size(), writeQuorumSize);
                    LOGGER.error(errorMsg);
                    quorumFuture.completeExceptionally(new RuntimeException(errorMsg));
                }
                
                // If all operations completed but quorum wasn't achieved
                if (completed == futures.size() && !quorumFuture.isDone()) {
                    int success = successCount.get();
                    if (success < writeQuorumSize) {
                        String errorMsg = String.format("Write quorum not achieved for object %s: " +
                                                       "only %d/%d replicas succeeded (required: %d)", 
                                                       objectPath, success, replicaWriters.size(), writeQuorumSize);
                        LOGGER.error(errorMsg);
                        quorumFuture.completeExceptionally(new RuntimeException(errorMsg));
                    }
                }
            });
        }
        
        return quorumFuture;
    }
    
    /**
     * Get statistics about the current write operation
     */
    public QuorumWriteStats getStats() {
        return new QuorumWriteStats(
            replicaWriters.size(),
            writeQuorumSize,
            successfulWrites.get(),
            failedWrites.get()
        );
    }
    
    /**
     * Statistics for quorum write operations
     */
    public static class QuorumWriteStats {
        private final int totalReplicas;
        private final int writeQuorumSize;
        private final int successfulWrites;
        private final int failedWrites;
        
        public QuorumWriteStats(int totalReplicas, int writeQuorumSize, int successfulWrites, int failedWrites) {
            this.totalReplicas = totalReplicas;
            this.writeQuorumSize = writeQuorumSize;
            this.successfulWrites = successfulWrites;
            this.failedWrites = failedWrites;
        }
        
        public int getTotalReplicas() {
            return totalReplicas;
        }
        
        public int getWriteQuorumSize() {
            return writeQuorumSize;
        }
        
        public int getSuccessfulWrites() {
            return successfulWrites;
        }
        
        public int getFailedWrites() {
            return failedWrites;
        }
        
        public boolean isQuorumAchieved() {
            return successfulWrites >= writeQuorumSize;
        }
        
        @Override
        public String toString() {
            return String.format("QuorumWriteStats{total=%d, quorum=%d, success=%d, failed=%d, achieved=%s}", 
                               totalReplicas, writeQuorumSize, successfulWrites, failedWrites, isQuorumAchieved());
        }
    }
}