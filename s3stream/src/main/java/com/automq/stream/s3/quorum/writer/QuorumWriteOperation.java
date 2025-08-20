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

package com.automq.stream.s3.quorum.writer;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Implements real S3 write operations for quorum storage
 * Handles writing data to multiple replicas and ensuring quorum requirements
 */
public class QuorumWriteOperation {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumWriteOperation.class);
    
    private final QuorumConfig quorumConfig;
    private final List<ObjectStorage> replicaStorages;
    private final QuorumState quorumState;
    
    public QuorumWriteOperation(QuorumConfig quorumConfig, 
                               List<ObjectStorage> replicaStorages,
                               QuorumState quorumState) {
        this.quorumConfig = quorumConfig;
        this.replicaStorages = replicaStorages;
        this.quorumState = quorumState;
    }

    /**
     * Performs a quorum write operation across multiple S3 replicas
     * 
     * @param batches The stream record batches to write
     * @return CompletableFuture that completes when write quorum is achieved
     */
    public CompletableFuture<WriteResult> writeToQuorum(List<StreamRecordBatch> batches) {
        LOGGER.info("Starting quorum write operation for {} batches", batches.size());
        
        // Validate quorum state
        if (!quorumState.hasQuorum()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Quorum not available for write operation"));
        }
        
        // Generate unique object key for this write
        String objectKey = generateObjectKey(batches);
        
        // Start write operations to all replicas in parallel
        List<CompletableFuture<ReplicaWriteResult>> replicaWrites = new ArrayList<>();
        
        for (int i = 0; i < replicaStorages.size(); i++) {
            final int replicaIndex = i;
            ReplicaConfig replicaConfig = quorumConfig.getReplicaConfigs().get(i);
            ObjectStorage storage = replicaStorages.get(i);
            
            CompletableFuture<ReplicaWriteResult> replicaWrite = writeToReplica(
                storage, replicaConfig, objectKey, batches, replicaIndex);
            replicaWrites.add(replicaWrite);
        }
        
        // Wait for write quorum to be satisfied
        return waitForWriteQuorum(replicaWrites, objectKey);
    }

    /**
     * Writes data to a single replica
     */
    private CompletableFuture<ReplicaWriteResult> writeToReplica(
            ObjectStorage storage, 
            ReplicaConfig replicaConfig,
            String objectKey, 
            List<StreamRecordBatch> batches,
            int replicaIndex) {
        
        LOGGER.debug("Writing to replica {}: {}", replicaIndex, replicaConfig.getRegion());
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                long startTime = System.currentTimeMillis();
                
                // Convert batches to writable format
                byte[] data = serializeBatches(batches);
                
                // Write to S3 storage
                // Note: This is a simplified implementation
                // In a real scenario, this would use the actual ObjectStorage.write() method
                writeDataToStorage(storage, objectKey, data);
                
                long duration = System.currentTimeMillis() - startTime;
                
                LOGGER.debug("Successfully wrote to replica {} in {} ms", replicaIndex, duration);
                
                return new ReplicaWriteResult(
                    replicaIndex, 
                    replicaConfig.getRegion(),
                    true, 
                    duration, 
                    data.length,
                    null
                );
                
            } catch (Exception e) {
                LOGGER.warn("Failed to write to replica {}: {}", replicaIndex, e.getMessage());
                return new ReplicaWriteResult(
                    replicaIndex,
                    replicaConfig.getRegion(),
                    false,
                    0,
                    0,
                    e
                );
            }
        }).orTimeout(quorumConfig.getWriteTimeoutMs(), TimeUnit.MILLISECONDS)
          .exceptionally(throwable -> {
              LOGGER.warn("Write to replica {} timed out or failed: {}", replicaIndex, throwable.getMessage());
              Exception exception = throwable instanceof Exception
                  ? (Exception) throwable : new RuntimeException(throwable);
              return new ReplicaWriteResult(
                  replicaIndex,
                  replicaConfig.getRegion(),
                  false,
                  0,
                  0,
                  exception
              );
          });
    }

    /**
     * Waits for the required write quorum to be satisfied
     */
    private CompletableFuture<WriteResult> waitForWriteQuorum(
            List<CompletableFuture<ReplicaWriteResult>> replicaWrites,
            String objectKey) {
        
        return CompletableFuture.supplyAsync(() -> {
            List<ReplicaWriteResult> results = new ArrayList<>();
            List<Exception> failures = new ArrayList<>();
            
            // Wait for all replica writes to complete
            for (CompletableFuture<ReplicaWriteResult> replicaWrite : replicaWrites) {
                try {
                    ReplicaWriteResult result = replicaWrite.join();
                    results.add(result);
                    
                    if (!result.isSuccess() && result.getException() != null) {
                        failures.add(result.getException());
                    }
                    
                } catch (CompletionException e) {
                    failures.add(e);
                    // Add a failed result for this replica
                    results.add(new ReplicaWriteResult(
                        results.size(), "unknown", false, 0, 0, 
                        e instanceof Exception ? (Exception) e : new RuntimeException(e)));
                }
            }
            
            // Count successful writes
            int successCount = results.stream().mapToInt(r -> r.isSuccess() ? 1 : 0).sum();
            
            // Check if we achieved write quorum
            if (successCount >= quorumConfig.getWriteQuorumSize()) {
                LOGGER.info("Write quorum achieved: {}/{} successful writes", 
                           successCount, quorumConfig.getQuorumSize());
                return new WriteResult(objectKey, true, results, successCount, failures);
            } else {
                throw new CompletionException(new IllegalStateException(
                    String.format("Failed to achieve write quorum: %d successful writes, %d required",
                                successCount, quorumConfig.getWriteQuorumSize())));
            }
        });
    }

    /**
     * Generates a unique object key for the write operation
     */
    private String generateObjectKey(List<StreamRecordBatch> batches) {
        if (batches.isEmpty()) {
            return "empty-" + System.currentTimeMillis();
        }
        
        StreamRecordBatch firstBatch = batches.get(0);
        StreamRecordBatch lastBatch = batches.get(batches.size() - 1);
        
        return String.format("stream-%d/epoch-%d/offset-%d-to-%d/%d",
                           firstBatch.getStreamId(),
                           firstBatch.getEpoch(),
                           firstBatch.getBaseOffset(),
                           lastBatch.getBaseOffset() + lastBatch.getCount() - 1,
                           System.currentTimeMillis());
    }

    /**
     * Serializes stream record batches to byte array
     */
    private byte[] serializeBatches(List<StreamRecordBatch> batches) {
        // Simplified serialization - in practice, this would use proper encoding
        int totalSize = batches.stream()
                              .mapToInt(batch -> batch.size())
                              .sum();
        
        byte[] result = new byte[totalSize + batches.size() * 32]; // Extra space for metadata
        int offset = 0;
        
        for (StreamRecordBatch batch : batches) {
            // Write batch metadata (simplified)
            System.arraycopy(longToBytes(batch.getStreamId()), 0, result, offset, 8);
            offset += 8;
            System.arraycopy(longToBytes(batch.getEpoch()), 0, result, offset, 8);
            offset += 8;
            System.arraycopy(longToBytes(batch.getBaseOffset()), 0, result, offset, 8);
            offset += 8;
            System.arraycopy(intToBytes(batch.getCount()), 0, result, offset, 4);
            offset += 4;
            System.arraycopy(intToBytes(batch.size()), 0, result, offset, 4);
            offset += 4;
            
            // Write batch payload
            if (batch.getPayload() != null) {
                byte[] payloadBytes = new byte[batch.getPayload().readableBytes()];
                batch.getPayload().getBytes(batch.getPayload().readerIndex(), payloadBytes);
                System.arraycopy(payloadBytes, 0, result, offset, payloadBytes.length);
                offset += payloadBytes.length;
            }
        }
        
        // Trim to actual size
        byte[] trimmed = new byte[offset];
        System.arraycopy(result, 0, trimmed, 0, offset);
        return trimmed;
    }

    /**
     * Writes data to storage using real ObjectStorage API
     */
    private void writeDataToStorage(ObjectStorage storage, String objectKey, byte[] data) {
        try {
            // Convert byte array to ByteBuf
            io.netty.buffer.ByteBuf byteBuf = io.netty.buffer.Unpooled.wrappedBuffer(data);
            
            // Use actual ObjectStorage write method with WriteOptions
            CompletableFuture<ObjectStorage.WriteResult> writeFuture = storage.write(
                ObjectStorage.WriteOptions.DEFAULT, objectKey, byteBuf);
            ObjectStorage.WriteResult result = writeFuture.join();
            
            LOGGER.debug("Successfully wrote {} bytes to storage with bucket: {}", data.length, result.bucket());
            
        } catch (Exception e) {
            LOGGER.warn("Failed to write to storage with key {}: {}", objectKey, e.getMessage());
            throw new RuntimeException("S3 write operation failed", e);
        }
    }

    // Helper methods for serialization
    private byte[] longToBytes(long value) {
        byte[] bytes = new byte[8];
        for (int i = 7; i >= 0; i--) {
            bytes[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return bytes;
    }

    private byte[] intToBytes(int value) {
        return new byte[] {
            (byte) (value >>> 24),
            (byte) (value >>> 16),
            (byte) (value >>> 8),
            (byte) value
        };
    }

    /**
     * Result of a write operation to a single replica
     */
    public static class ReplicaWriteResult {
        private final int replicaIndex;
        private final String region;
        private final boolean success;
        private final long durationMs;
        private final long bytesWritten;
        private final Exception exception;

        public ReplicaWriteResult(int replicaIndex, String region, boolean success, 
                                long durationMs, long bytesWritten, Exception exception) {
            this.replicaIndex = replicaIndex;
            this.region = region;
            this.success = success;
            this.durationMs = durationMs;
            this.bytesWritten = bytesWritten;
            this.exception = exception;
        }

        public int getReplicaIndex() {
            return replicaIndex;
        }
        
        public String getRegion() {
            return region;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public long getDurationMs() {
            return durationMs;
        }
        
        public long getBytesWritten() {
            return bytesWritten;
        }
        
        public Exception getException() {
            return exception;
        }
    }

    /**
     * Result of a complete quorum write operation
     */
    public static class WriteResult {
        private final String objectKey;
        private final boolean success;
        private final List<ReplicaWriteResult> replicaResults;
        private final int successfulWrites;
        private final List<Exception> failures;

        public WriteResult(String objectKey, boolean success, 
                         List<ReplicaWriteResult> replicaResults,
                         int successfulWrites, List<Exception> failures) {
            this.objectKey = objectKey;
            this.success = success;
            this.replicaResults = new ArrayList<>(replicaResults);
            this.successfulWrites = successfulWrites;
            this.failures = new ArrayList<>(failures);
        }

        public String getObjectKey() {
            return objectKey;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public List<ReplicaWriteResult> getReplicaResults() {
            return replicaResults;
        }
        
        public int getSuccessfulWrites() {
            return successfulWrites;
        }
        
        public List<Exception> getFailures() {
            return failures;
        }
    }
}