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

package com.automq.stream.s3.quorum.consistency;

import com.automq.stream.s3.operator.ObjectStorage;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * DataConsistencyValidator ensures data consistency across S3 Quorum Storage replicas
 * by performing checksum validation and cross-replica verification.
 */
public class DataConsistencyValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataConsistencyValidator.class);
    
    private final List<ObjectStorage> replicas;
    private final int consistencyCheckThreshold;
    private final boolean checksumValidationEnabled;
    private final Map<String, ConsistencyCheckResult> consistencyCache;
    private final AtomicLong validationCount = new AtomicLong(0);
    private final AtomicLong inconsistencyCount = new AtomicLong(0);
    
    public DataConsistencyValidator(List<ObjectStorage> replicas, 
                                  int consistencyCheckThreshold,
                                  boolean checksumValidationEnabled) {
        this.replicas = replicas;
        this.consistencyCheckThreshold = consistencyCheckThreshold;
        this.checksumValidationEnabled = checksumValidationEnabled;
        this.consistencyCache = new ConcurrentHashMap<>();
        
        LOGGER.info("DataConsistencyValidator initialized with {} replicas, checksum validation: {}", 
                   replicas.size(), checksumValidationEnabled);
    }
    
    /**
     * Validate data consistency across all replicas for a given object
     */
    public CompletableFuture<ConsistencyValidationResult> validateConsistency(String objectKey) {
        long validationId = validationCount.incrementAndGet();
        LOGGER.debug("Starting consistency validation {} for object: {}", validationId, objectKey);
        
        // Read object from all replicas in parallel
        List<CompletableFuture<ReplicaReadResult>> readFutures = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;
            ObjectStorage replica = replicas.get(i);
            
            CompletableFuture<ReplicaReadResult> readFuture = replica
                .read(new ObjectStorage.ReadOptions(), objectKey)
                .thenApply(readResult -> {
                    String checksum = null;
                    if (checksumValidationEnabled) {
                        checksum = calculateChecksum(readResult);
                    }
                    return new ReplicaReadResult(replicaIndex, readResult, 
                                               readResult.readableBytes(), checksum, true, null);
                })
                .exceptionally(throwable -> {
                    LOGGER.warn("Failed to read object {} from replica {}: {}", 
                               objectKey, replicaIndex, throwable.getMessage());
                    return new ReplicaReadResult(replicaIndex, null, 0, null, false, throwable);
                });
            
            readFutures.add(readFuture);
        }
        
        // Wait for all reads to complete and analyze results
        return CompletableFuture.allOf(readFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<ReplicaReadResult> results = readFutures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
                
                return analyzeConsistencyResults(objectKey, validationId, results);
            });
    }
    
    /**
     * Perform read repair if inconsistencies are detected
     */
    public CompletableFuture<RepairResult> performReadRepair(String objectKey, 
                                                            ConsistencyValidationResult validationResult) {
        if (validationResult.isConsistent()) {
            return CompletableFuture.completedFuture(new RepairResult(objectKey, true, 0, "No repair needed"));
        }
        
        LOGGER.info("Performing read repair for object: {} due to inconsistencies", objectKey);
        
        // Find the most authoritative version (longest content or most common checksum)
        ReplicaReadResult authoritativeResult = findAuthoritativeVersion(validationResult.getReplicaResults());
        
        if (authoritativeResult == null || !authoritativeResult.isSuccessful()) {
            return CompletableFuture.completedFuture(
                new RepairResult(objectKey, false, 0, "No authoritative version found"));
        }
        
        // Repair inconsistent replicas
        List<CompletableFuture<Void>> repairFutures = new ArrayList<>();
        int repairedCount = 0;
        
        for (ReplicaReadResult result : validationResult.getReplicaResults()) {
            if (!result.isSuccessful() || !isConsistentWith(result, authoritativeResult)) {
                // Repair this replica
                ObjectStorage replica = replicas.get(result.getReplicaIndex());
                CompletableFuture<Void> repairFuture = replica
                    .write(new ObjectStorage.WriteOptions(), objectKey, authoritativeResult.getData().retainedSlice())
                    .thenApply(writeResult -> (Void) null)
                    .whenComplete((v, throwable) -> {
                        if (throwable == null) {
                            LOGGER.info("Repaired replica {} for object: {}", result.getReplicaIndex(), objectKey);
                        } else {
                            LOGGER.error("Failed to repair replica {} for object {}: {}", 
                                        result.getReplicaIndex(), objectKey, throwable.getMessage());
                        }
                    });
                
                repairFutures.add(repairFuture);
                repairedCount++;
            }
        }
        
        final int finalRepairedCount = repairedCount;
        
        return CompletableFuture.allOf(repairFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> new RepairResult(objectKey, true, finalRepairedCount, "Repair completed successfully"));
    }
    
    /**
     * Get consistency validation statistics
     */
    public ConsistencyStatistics getStatistics() {
        return new ConsistencyStatistics(
            validationCount.get(),
            inconsistencyCount.get(),
            consistencyCache.size()
        );
    }
    
    /**
     * Clear consistency cache
     */
    public void clearCache() {
        consistencyCache.clear();
        LOGGER.info("Consistency cache cleared");
    }
    
    private ConsistencyValidationResult analyzeConsistencyResults(String objectKey, 
                                                                 long validationId,
                                                                 List<ReplicaReadResult> results) {
        List<ReplicaReadResult> successfulResults = results.stream()
            .filter(ReplicaReadResult::isSuccessful)
            .collect(Collectors.toList());
        
        boolean isConsistent = true;
        List<String> inconsistencyReasons = new ArrayList<>();
        
        if (successfulResults.isEmpty()) {
            isConsistent = false;
            inconsistencyReasons.add("No successful reads from any replica");
        } else if (successfulResults.size() < consistencyCheckThreshold) {
            isConsistent = false;
            inconsistencyReasons.add(String.format("Only %d/%d replicas returned data (threshold: %d)", 
                                                  successfulResults.size(), replicas.size(), consistencyCheckThreshold));
        } else if (checksumValidationEnabled) {
            // Check checksum consistency
            String firstChecksum = successfulResults.get(0).getChecksum();
            for (ReplicaReadResult result : successfulResults) {
                if (!firstChecksum.equals(result.getChecksum())) {
                    isConsistent = false;
                    inconsistencyReasons.add(String.format("Checksum mismatch between replicas: %s vs %s", 
                                                          firstChecksum, result.getChecksum()));
                    break;
                }
            }
        } else {
            // Check content length consistency
            long firstContentLength = successfulResults.get(0).getContentLength();
            for (ReplicaReadResult result : successfulResults) {
                if (result.getContentLength() != firstContentLength) {
                    isConsistent = false;
                    inconsistencyReasons.add(String.format("Content length mismatch: %d vs %d", 
                                                          firstContentLength, result.getContentLength()));
                    break;
                }
            }
        }
        
        if (!isConsistent) {
            inconsistencyCount.incrementAndGet();
        }
        
        ConsistencyValidationResult validationResult = new ConsistencyValidationResult(
            objectKey, validationId, isConsistent, results, inconsistencyReasons);
        
        // Cache the result
        consistencyCache.put(objectKey, new ConsistencyCheckResult(validationResult, System.currentTimeMillis()));
        
        LOGGER.debug("Consistency validation {} for object {}: consistent={}, reasons={}", 
                    validationId, objectKey, isConsistent, inconsistencyReasons);
        
        return validationResult;
    }
    
    private ReplicaReadResult findAuthoritativeVersion(List<ReplicaReadResult> results) {
        List<ReplicaReadResult> successfulResults = results.stream()
            .filter(ReplicaReadResult::isSuccessful)
            .collect(Collectors.toList());
        
        if (successfulResults.isEmpty()) {
            return null;
        }
        
        if (checksumValidationEnabled) {
            // Find the most common checksum
            Map<String, Long> checksumCounts = successfulResults.stream()
                .collect(Collectors.groupingBy(ReplicaReadResult::getChecksum, Collectors.counting()));
            
            String mostCommonChecksum = checksumCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
            
            return successfulResults.stream()
                .filter(result -> mostCommonChecksum.equals(result.getChecksum()))
                .findFirst()
                .orElse(null);
        } else {
            // Return the result with the longest content
            return successfulResults.stream()
                .max((r1, r2) -> Long.compare(r1.getContentLength(), r2.getContentLength()))
                .orElse(null);
        }
    }
    
    private boolean isConsistentWith(ReplicaReadResult result1, ReplicaReadResult result2) {
        if (!result1.isSuccessful() || !result2.isSuccessful()) {
            return false;
        }
        
        if (checksumValidationEnabled) {
            return result1.getChecksum().equals(result2.getChecksum());
        } else {
            return result1.getContentLength() == result2.getContentLength();
        }
    }
    
    private String calculateChecksum(ByteBuf data) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            
            // Read data from ByteBuf without changing its position
            int readerIndex = data.readerIndex();
            byte[] bytes = new byte[data.readableBytes()];
            data.getBytes(readerIndex, bytes);
            
            byte[] hash = md.digest(bytes);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (Exception e) {
            LOGGER.error("Failed to calculate checksum", e);
            return "checksum-error-" + System.currentTimeMillis();
        }
    }
    
    // Inner classes for result handling
    public static class ReplicaReadResult {
        private final int replicaIndex;
        private final ByteBuf data;
        private final long contentLength;
        private final String checksum;
        private final boolean successful;
        private final Throwable error;
        
        public ReplicaReadResult(int replicaIndex, ByteBuf data, long contentLength, 
                               String checksum, boolean successful, Throwable error) {
            this.replicaIndex = replicaIndex;
            this.data = data;
            this.contentLength = contentLength;
            this.checksum = checksum;
            this.successful = successful;
            this.error = error;
        }
        
        // Getters
        public int getReplicaIndex() {
            return replicaIndex;
        }
        
        public ByteBuf getData() {
            return data;
        }
        
        public long getContentLength() {
            return contentLength;
        }
        
        public String getChecksum() {
            return checksum;
        }
        
        public boolean isSuccessful() {
            return successful;
        }
        
        public Throwable getError() {
            return error;
        }
    }
    
    public static class ConsistencyValidationResult {
        private final String objectKey;
        private final long validationId;
        private final boolean consistent;
        private final List<ReplicaReadResult> replicaResults;
        private final List<String> inconsistencyReasons;
        
        public ConsistencyValidationResult(String objectKey, long validationId, boolean consistent,
                                         List<ReplicaReadResult> replicaResults, List<String> inconsistencyReasons) {
            this.objectKey = objectKey;
            this.validationId = validationId;
            this.consistent = consistent;
            this.replicaResults = replicaResults;
            this.inconsistencyReasons = inconsistencyReasons;
        }
        
        // Getters
        public String getObjectKey() {
            return objectKey;
        }
        
        public long getValidationId() {
            return validationId;
        }
        
        public boolean isConsistent() {
            return consistent;
        }
        
        public List<ReplicaReadResult> getReplicaResults() {
            return replicaResults;
        }
        
        public List<String> getInconsistencyReasons() {
            return inconsistencyReasons;
        }
    }
    
    public static class RepairResult {
        private final String objectKey;
        private final boolean successful;
        private final int repairedReplicaCount;
        private final String message;
        
        public RepairResult(String objectKey, boolean successful, int repairedReplicaCount, String message) {
            this.objectKey = objectKey;
            this.successful = successful;
            this.repairedReplicaCount = repairedReplicaCount;
            this.message = message;
        }
        
        // Getters
        public String getObjectKey() {
            return objectKey;
        }
        
        public boolean isSuccessful() {
            return successful;
        }
        
        public int getRepairedReplicaCount() {
            return repairedReplicaCount;
        }
        
        public String getMessage() {
            return message;
        }
    }
    
    public static class ConsistencyStatistics {
        private final long totalValidations;
        private final long inconsistencyCount;
        private final int cacheSize;
        
        public ConsistencyStatistics(long totalValidations, long inconsistencyCount, int cacheSize) {
            this.totalValidations = totalValidations;
            this.inconsistencyCount = inconsistencyCount;
            this.cacheSize = cacheSize;
        }
        
        // Getters
        public long getTotalValidations() {
            return totalValidations;
        }
        
        public long getInconsistencyCount() {
            return inconsistencyCount;
        }
        
        public int getCacheSize() {
            return cacheSize;
        }
        public double getInconsistencyRate() { 
            return totalValidations > 0 ? (double) inconsistencyCount / totalValidations : 0.0; 
        }
    }
    
    private static class ConsistencyCheckResult {
        private final ConsistencyValidationResult result;
        private final long timestamp;
        
        public ConsistencyCheckResult(ConsistencyValidationResult result, long timestamp) {
            this.result = result;
            this.timestamp = timestamp;
        }
        
        public ConsistencyValidationResult getResult() {
            return result;
        }
        
        public long getTimestamp() {
            return timestamp;
        }
    }
}