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

package com.automq.stream.s3.quorum.reader;

import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Implements quorum read operations for S3 storage
 * Supports different read strategies and ensures data consistency
 */
public class QuorumReadOperation {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumReadOperation.class);
    
    private final QuorumConfig quorumConfig;
    private final List<Storage> replicaStorages;
    private final QuorumState quorumState;
    
    public enum ReadStrategy {
        FAST_READ,      // Read from primary, fallback to secondaries
        QUORUM_READ,    // Read from quorum and verify consistency
        ALL_READ        // Read from all replicas and choose best result
    }
    
    public QuorumReadOperation(QuorumConfig quorumConfig, 
                              List<Storage> replicaStorages,
                              QuorumState quorumState) {
        this.quorumConfig = quorumConfig;
        this.replicaStorages = replicaStorages;
        this.quorumState = quorumState;
    }

    /**
     * Performs a quorum read operation with the specified strategy
     */
    public CompletableFuture<ReadResult> readFromQuorum(
            FetchContext context, 
            long streamId, 
            long startOffset, 
            long endOffset, 
            int maxBytes,
            ReadStrategy strategy) {
        
        LOGGER.info("Starting quorum read operation for stream {} with strategy {}", streamId, strategy);
        
        // Validate quorum state
        if (!quorumState.hasQuorum()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Quorum not available for read operation"));
        }
        
        switch (strategy) {
            case FAST_READ:
                return executeFastRead(context, streamId, startOffset, endOffset, maxBytes);
            case QUORUM_READ:
                return executeQuorumRead(context, streamId, startOffset, endOffset, maxBytes);
            case ALL_READ:
                return executeAllRead(context, streamId, startOffset, endOffset, maxBytes);
            default:
                return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Unsupported read strategy: " + strategy));
        }
    }

    /**
     * Fast read: Try primary first, fallback to secondaries
     */
    private CompletableFuture<ReadResult> executeFastRead(
            FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        
        LOGGER.debug("Executing fast read for stream {}", streamId);
        
        return readFromReplica(0, context, streamId, startOffset, endOffset, maxBytes)
            .thenCompose(primaryResult -> {
                if (primaryResult.isSuccess()) {
                    LOGGER.debug("Fast read succeeded from primary replica");
                    return CompletableFuture.completedFuture(new ReadResult(
                        primaryResult.getData(), true, List.of(primaryResult), 
                        1, List.of(), ReadStrategy.FAST_READ));
                } else {
                    LOGGER.warn("Primary replica read failed, trying secondary replicas");
                    return trySecondaryReplicas(context, streamId, startOffset, endOffset, maxBytes);
                }
            });
    }

    /**
     * Quorum read: Read from read quorum and verify consistency
     */
    private CompletableFuture<ReadResult> executeQuorumRead(
            FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        
        LOGGER.debug("Executing quorum read for stream {}", streamId);
        
        List<CompletableFuture<ReplicaReadResult>> readFutures = new ArrayList<>();
        
        // Start reads from all healthy replicas
        for (int i = 0; i < replicaStorages.size(); i++) {
            if (quorumState.isReplicaHealthy(i)) {
                readFutures.add(readFromReplica(i, context, streamId, startOffset, endOffset, maxBytes));
            }
        }
        
        return waitForReadQuorum(readFutures, ReadStrategy.QUORUM_READ);
    }

    /**
     * All read: Read from all replicas and choose the best result
     */
    private CompletableFuture<ReadResult> executeAllRead(
            FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        
        LOGGER.debug("Executing all read for stream {}", streamId);
        
        List<CompletableFuture<ReplicaReadResult>> readFutures = new ArrayList<>();
        
        // Start reads from all replicas
        for (int i = 0; i < replicaStorages.size(); i++) {
            readFutures.add(readFromReplica(i, context, streamId, startOffset, endOffset, maxBytes));
        }
        
        return waitForAllReads(readFutures);
    }

    /**
     * Reads data from a single replica
     */
    private CompletableFuture<ReplicaReadResult> readFromReplica(
            int replicaIndex, FetchContext context, long streamId, 
            long startOffset, long endOffset, int maxBytes) {
        
        LOGGER.debug("Reading from replica {}", replicaIndex);
        
        ReplicaConfig replicaConfig = quorumConfig.getReplicaConfigs().get(replicaIndex);
        
        return replicaStorages.get(replicaIndex)
            .read(context, streamId, startOffset, endOffset, maxBytes)
            .thenApply(data -> {
                long duration = System.currentTimeMillis();
                byte[] checksum = calculateChecksum(data);
                
                LOGGER.debug("Successfully read from replica {} in {} ms", replicaIndex, duration);
                quorumState.markReplicaSuccess(replicaIndex);
                
                return new ReplicaReadResult(
                    replicaIndex, 
                    replicaConfig.getRegion(),
                    true, 
                    data,
                    checksum,
                    duration,
                    null
                );
            })
            .exceptionally(throwable -> {
                LOGGER.warn("Read failed from replica {}: {}", replicaIndex, throwable.getMessage());
                quorumState.markReplicaFailed(replicaIndex);
                
                Exception exception = throwable instanceof Exception
                    ? (Exception) throwable : new RuntimeException(throwable);
                
                return new ReplicaReadResult(
                    replicaIndex,
                    replicaConfig.getRegion(),
                    false,
                    null,
                    null,
                    0,
                    exception
                );
            })
            .orTimeout(quorumConfig.getReadTimeoutMs(), TimeUnit.MILLISECONDS);
    }

    /**
     * Try reading from secondary replicas when primary fails
     */
    private CompletableFuture<ReadResult> trySecondaryReplicas(
            FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
        
        List<CompletableFuture<ReplicaReadResult>> secondaryFutures = new ArrayList<>();
        
        for (int i = 1; i < replicaStorages.size(); i++) {
            if (quorumState.isReplicaHealthy(i)) {
                secondaryFutures.add(readFromReplica(i, context, streamId, startOffset, endOffset, maxBytes));
            }
        }
        
        if (secondaryFutures.isEmpty()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("No healthy secondary replicas available"));
        }
        
        return CompletableFuture.supplyAsync(() -> {
            // Wait for first successful read
            for (CompletableFuture<ReplicaReadResult> future : secondaryFutures) {
                try {
                    ReplicaReadResult result = future.join();
                    if (result.isSuccess()) {
                        return new ReadResult(
                            result.getData(), true, List.of(result), 
                            1, List.of(), ReadStrategy.FAST_READ);
                    }
                } catch (CompletionException e) {
                    // Continue to next replica
                }
            }
            
            throw new CompletionException(new RuntimeException("All secondary replicas failed"));
        });
    }

    /**
     * Wait for read quorum and verify data consistency
     */
    private CompletableFuture<ReadResult> waitForReadQuorum(
            List<CompletableFuture<ReplicaReadResult>> readFutures, ReadStrategy strategy) {
        
        return CompletableFuture.supplyAsync(() -> {
            List<ReplicaReadResult> results = new ArrayList<>();
            List<Exception> failures = new ArrayList<>();
            
            // Wait for all reads to complete
            for (CompletableFuture<ReplicaReadResult> readFuture : readFutures) {
                try {
                    ReplicaReadResult result = readFuture.join();
                    results.add(result);
                    
                    if (!result.isSuccess() && result.getException() != null) {
                        failures.add(result.getException());
                    }
                    
                } catch (CompletionException e) {
                    failures.add(e);
                    results.add(new ReplicaReadResult(
                        results.size(), "unknown", false, null, null, 0, 
                        e instanceof Exception ? (Exception) e : new RuntimeException(e)));
                }
            }
            
            // Count successful reads
            List<ReplicaReadResult> successfulReads = results.stream()
                .filter(ReplicaReadResult::isSuccess)
                .collect(java.util.stream.Collectors.toList());
            
            if (successfulReads.size() < quorumConfig.getReadQuorumSize()) {
                throw new CompletionException(new IllegalStateException(
                    String.format("Failed to achieve read quorum: %d successful reads, %d required",
                        successfulReads.size(), quorumConfig.getReadQuorumSize())));
            }
            
            // Verify data consistency across successful reads
            ReadDataBlock consistentData = verifyDataConsistency(successfulReads);
            
            LOGGER.info("Read quorum achieved: {}/{} successful reads", 
                       successfulReads.size(), quorumConfig.getQuorumSize());
            
            return new ReadResult(consistentData, true, results, 
                                 successfulReads.size(), failures, strategy);
        });
    }

    /**
     * Wait for all reads to complete and choose the best result
     */
    private CompletableFuture<ReadResult> waitForAllReads(
            List<CompletableFuture<ReplicaReadResult>> readFutures) {
        
        return CompletableFuture.supplyAsync(() -> {
            List<ReplicaReadResult> results = new ArrayList<>();
            List<Exception> failures = new ArrayList<>();
            
            // Wait for all reads to complete
            for (CompletableFuture<ReplicaReadResult> readFuture : readFutures) {
                try {
                    ReplicaReadResult result = readFuture.join();
                    results.add(result);
                    
                    if (!result.isSuccess() && result.getException() != null) {
                        failures.add(result.getException());
                    }
                    
                } catch (CompletionException e) {
                    failures.add(e);
                    results.add(new ReplicaReadResult(
                        results.size(), "unknown", false, null, null, 0, 
                        e instanceof Exception ? (Exception) e : new RuntimeException(e)));
                }
            }
            
            // Choose the best result based on priority and consistency
            ReadDataBlock bestData = chooseBestResult(results);
            
            if (bestData == null) {
                throw new CompletionException(new RuntimeException("No successful reads found"));
            }
            
            return new ReadResult(bestData, true, results, 
                                 results.size(), failures, ReadStrategy.ALL_READ);
        });
    }

    /**
     * Verify data consistency across multiple read results
     */
    private ReadDataBlock verifyDataConsistency(List<ReplicaReadResult> successfulReads) {
        if (successfulReads.isEmpty()) {
            throw new IllegalStateException("No successful reads to verify");
        }
        
        if (successfulReads.size() == 1) {
            return successfulReads.get(0).getData();
        }
        
        // Group reads by checksum to find consistent data
        var checksumGroups = successfulReads.stream()
            .collect(java.util.stream.Collectors.groupingBy(
                result -> Arrays.toString(result.getChecksum())));
        
        // Find the largest group (most consistent data)
        var largestGroup = checksumGroups.values().stream()
            .max(java.util.Comparator.comparingInt(List::size))
            .orElseThrow(() -> new IllegalStateException("Failed to find consistent data"));
        
        if (largestGroup.size() < quorumConfig.getReadQuorumSize()) {
            LOGGER.warn("Data inconsistency detected: only {} out of {} reads are consistent", 
                       largestGroup.size(), successfulReads.size());
        }
        
        // Return data from the primary replica in the consistent group, or first available
        return largestGroup.stream()
            .filter(result -> result.getReplicaIndex() == 0)
            .findFirst()
            .orElse(largestGroup.get(0))
            .getData();
    }

    /**
     * Choose the best result from all reads (prioritize primary, then consistency)
     */
    private ReadDataBlock chooseBestResult(List<ReplicaReadResult> results) {
        List<ReplicaReadResult> successfulReads = results.stream()
            .filter(ReplicaReadResult::isSuccess)
            .collect(java.util.stream.Collectors.toList());
        
        if (successfulReads.isEmpty()) {
            return null;
        }
        
        // First preference: successful read from primary replica
        var primaryRead = successfulReads.stream()
            .filter(result -> result.getReplicaIndex() == 0)
            .findFirst();
        
        if (primaryRead.isPresent()) {
            return primaryRead.get().getData();
        }
        
        // Second preference: most consistent data
        return verifyDataConsistency(successfulReads);
    }

    /**
     * Calculate checksum for data integrity verification
     */
    private byte[] calculateChecksum(ReadDataBlock data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            if (data != null && data.getRecords() != null && !data.getRecords().isEmpty()) {
                // Calculate checksum based on all record batches
                for (var record : data.getRecords()) {
                    if (record.getPayload() != null) {
                        byte[] recordBytes = new byte[record.getPayload().readableBytes()];
                        record.getPayload().getBytes(record.getPayload().readerIndex(), recordBytes);
                        md.update(recordBytes);
                    }
                }
                return md.digest();
            }
            return new byte[0];
        } catch (NoSuchAlgorithmException e) {
            LOGGER.warn("SHA-256 not available, using simple checksum", e);
            return new byte[]{0}; // Fallback
        }
    }

    /**
     * Result of a read operation from a single replica
     */
    public static class ReplicaReadResult {
        private final int replicaIndex;
        private final String region;
        private final boolean success;
        private final ReadDataBlock data;
        private final byte[] checksum;
        private final long durationMs;
        private final Exception exception;

        public ReplicaReadResult(int replicaIndex, String region, boolean success, 
                               ReadDataBlock data, byte[] checksum, long durationMs, Exception exception) {
            this.replicaIndex = replicaIndex;
            this.region = region;
            this.success = success;
            this.data = data;
            this.checksum = checksum;
            this.durationMs = durationMs;
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
        
        public ReadDataBlock getData() {
            return data;
        }
        
        public byte[] getChecksum() {
            return checksum;
        }
        
        public long getDurationMs() {
            return durationMs;
        }
        
        public Exception getException() {
            return exception;
        }
    }

    /**
     * Result of a complete quorum read operation
     */
    public static class ReadResult {
        private final ReadDataBlock data;
        private final boolean success;
        private final List<ReplicaReadResult> replicaResults;
        private final int successfulReads;
        private final List<Exception> failures;
        private final ReadStrategy strategy;

        public ReadResult(ReadDataBlock data, boolean success, 
                         List<ReplicaReadResult> replicaResults,
                         int successfulReads, List<Exception> failures, ReadStrategy strategy) {
            this.data = data;
            this.success = success;
            this.replicaResults = new ArrayList<>(replicaResults);
            this.successfulReads = successfulReads;
            this.failures = new ArrayList<>(failures);
            this.strategy = strategy;
        }

        public ReadDataBlock getData() {
            return data;
        }
        
        public boolean isSuccess() {
            return success;
        }
        
        public List<ReplicaReadResult> getReplicaResults() {
            return replicaResults;
        }
        
        public int getSuccessfulReads() {
            return successfulReads;
        }
        
        public List<Exception> getFailures() {
            return failures;
        }
        
        public ReadStrategy getStrategy() {
            return strategy;
        }
    }
}