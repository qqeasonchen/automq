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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QuorumDataValidator provides data consistency validation and repair capabilities
 * across multiple S3 storage replicas in a quorum-based system.
 */
public class QuorumDataValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumDataValidator.class);
    
    private final List<ObjectStorage> replicas;
    private final int readQuorumSize;
    private final ValidationConfig config;
    private final AtomicLong validationCounter = new AtomicLong(0);
    private final AtomicLong repairCounter = new AtomicLong(0);
    
    public QuorumDataValidator(List<ObjectStorage> replicas, int readQuorumSize, ValidationConfig config) {
        this.replicas = replicas;
        this.readQuorumSize = readQuorumSize;
        this.config = config;
    }
    
    /**
     * Validates data consistency across replicas for a given object key
     */
    public CompletableFuture<ValidationResult> validateConsistency(String objectKey) {
        validationCounter.incrementAndGet();
        LOGGER.debug("Starting consistency validation for object: {}", objectKey);
        
        // Read from all available replicas
        List<CompletableFuture<ReplicaData>> replicaReads = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;
            ObjectStorage replica = replicas.get(i);
            
            CompletableFuture<ReplicaData> replicaFuture = replica.read(new ObjectStorage.ReadOptions(), objectKey)
                .thenApply(data -> {
                    String checksum = calculateChecksum(data);
                    return new ReplicaData(replicaIndex, data, checksum, true);
                })
                .exceptionally(throwable -> {
                    LOGGER.warn("Failed to read from replica {}: {}", replicaIndex, throwable.getMessage());
                    return new ReplicaData(replicaIndex, null, null, false);
                });
            
            replicaReads.add(replicaFuture);
        }
        
        // Wait for all reads to complete
        return CompletableFuture.allOf(replicaReads.toArray(new CompletableFuture[0]))
            .thenApply(v -> {
                List<ReplicaData> results = new ArrayList<>();
                for (CompletableFuture<ReplicaData> future : replicaReads) {
                    results.add(future.join());
                }
                return analyzeConsistency(objectKey, results);
            });
    }
    
    /**
     * Performs read repair when inconsistencies are detected
     */
    public CompletableFuture<RepairResult> performReadRepair(String objectKey, ValidationResult validationResult) {
        if (validationResult.isConsistent()) {
            return CompletableFuture.completedFuture(new RepairResult(objectKey, true, "No repair needed", 0));
        }
        
        repairCounter.incrementAndGet();
        LOGGER.info("Performing read repair for object: {}", objectKey);
        
        // Find the canonical version (most common checksum)
        String canonicalChecksum = validationResult.getCanonicalChecksum();
        ReplicaData canonicalData = validationResult.getReplicaData().stream()
            .filter(data -> data.isReadSuccessful() && canonicalChecksum.equals(data.getChecksum()))
            .findFirst()
            .orElse(null);
        
        if (canonicalData == null) {
            return CompletableFuture.completedFuture(new RepairResult(objectKey, false, "No canonical version found", 0));
        }
        
        // Repair inconsistent replicas
        List<CompletableFuture<Void>> repairFutures = new ArrayList<>();
        int repairedCount = 0;
        
        for (ReplicaData replicaData : validationResult.getReplicaData()) {
            if (!replicaData.isReadSuccessful() || !canonicalChecksum.equals(replicaData.getChecksum())) {
                ObjectStorage replica = replicas.get(replicaData.getReplicaIndex());
                
                // Create independent copy for repair
                ByteBuf repairData = canonicalData.getData().alloc().buffer(canonicalData.getData().readableBytes());
                repairData.writeBytes(canonicalData.getData(), canonicalData.getData().readerIndex(), canonicalData.getData().readableBytes());
                
                CompletableFuture<Void> repairFuture = replica.write(new ObjectStorage.WriteOptions(), objectKey, repairData)
                    .thenApply(result -> (Void) null)
                    .exceptionally(throwable -> {
                        LOGGER.error("Failed to repair replica {}: {}", replicaData.getReplicaIndex(), throwable.getMessage());
                        repairData.release();
                        return null;
                    });
                
                repairFutures.add(repairFuture);
                repairedCount++;
            }
        }
        
        final int finalRepairedCount = repairedCount;
        return CompletableFuture.allOf(repairFutures.toArray(new CompletableFuture[0]))
            .thenApply(v -> new RepairResult(objectKey, true, "Repair completed", finalRepairedCount));
    }
    
    private ValidationResult analyzeConsistency(String objectKey, List<ReplicaData> replicaData) {
        Map<String, Integer> checksumCounts = new HashMap<>();
        int successfulReads = 0;
        
        for (ReplicaData data : replicaData) {
            if (data.isReadSuccessful()) {
                successfulReads++;
                checksumCounts.merge(data.getChecksum(), 1, Integer::sum);
            }
        }
        
        boolean isConsistent = checksumCounts.size() <= 1;
        boolean hasQuorum = successfulReads >= readQuorumSize;
        
        String canonicalChecksum = null;
        if (!checksumCounts.isEmpty()) {
            canonicalChecksum = checksumCounts.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse(null);
        }
        
        LOGGER.debug("Validation result for {}: consistent={}, hasQuorum={}, successfulReads={}/{}", 
                    objectKey, isConsistent, hasQuorum, successfulReads, replicas.size());
        
        return new ValidationResult(objectKey, isConsistent, hasQuorum, replicaData, canonicalChecksum, checksumCounts);
    }
    
    private String calculateChecksum(ByteBuf data) {
        if (data == null) {
            return null;
        }
        
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] dataBytes = new byte[data.readableBytes()];
            data.getBytes(data.readerIndex(), dataBytes);
            byte[] hash = md.digest(dataBytes);
            
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            LOGGER.error("SHA-256 algorithm not available", e);
            return "checksum-error";
        }
    }
    
    public ValidationStats getValidationStats() {
        return new ValidationStats(validationCounter.get(), repairCounter.get());
    }
    
    // Configuration class
    public static class ValidationConfig {
        private final boolean enableAutoRepair;
        private final long repairTimeoutMs;
        private final int maxRepairRetries;
        
        public ValidationConfig(boolean enableAutoRepair, long repairTimeoutMs, int maxRepairRetries) {
            this.enableAutoRepair = enableAutoRepair;
            this.repairTimeoutMs = repairTimeoutMs;
            this.maxRepairRetries = maxRepairRetries;
        }
        
        public boolean isAutoRepairEnabled() { return enableAutoRepair; }
        public long getRepairTimeoutMs() { return repairTimeoutMs; }
        public int getMaxRepairRetries() { return maxRepairRetries; }
        
        public static ValidationConfig defaultConfig() {
            return new ValidationConfig(true, 5000, 2);
        }
    }
    
    // Data classes
    public static class ReplicaData {
        private final int replicaIndex;
        private final ByteBuf data;
        private final String checksum;
        private final boolean readSuccessful;
        
        public ReplicaData(int replicaIndex, ByteBuf data, String checksum, boolean readSuccessful) {
            this.replicaIndex = replicaIndex;
            this.data = data;
            this.checksum = checksum;
            this.readSuccessful = readSuccessful;
        }
        
        public int getReplicaIndex() { return replicaIndex; }
        public ByteBuf getData() { return data; }
        public String getChecksum() { return checksum; }
        public boolean isReadSuccessful() { return readSuccessful; }
    }
    
    public static class ValidationResult {
        private final String objectKey;
        private final boolean isConsistent;
        private final boolean hasQuorum;
        private final List<ReplicaData> replicaData;
        private final String canonicalChecksum;
        private final Map<String, Integer> checksumCounts;
        
        public ValidationResult(String objectKey, boolean isConsistent, boolean hasQuorum, 
                              List<ReplicaData> replicaData, String canonicalChecksum, Map<String, Integer> checksumCounts) {
            this.objectKey = objectKey;
            this.isConsistent = isConsistent;
            this.hasQuorum = hasQuorum;
            this.replicaData = replicaData;
            this.canonicalChecksum = canonicalChecksum;
            this.checksumCounts = checksumCounts;
        }
        
        public String getObjectKey() { return objectKey; }
        public boolean isConsistent() { return isConsistent; }
        public boolean hasQuorum() { return hasQuorum; }
        public List<ReplicaData> getReplicaData() { return replicaData; }
        public String getCanonicalChecksum() { return canonicalChecksum; }
        public Map<String, Integer> getChecksumCounts() { return checksumCounts; }
    }
    
    public static class RepairResult {
        private final String objectKey;
        private final boolean success;
        private final String message;
        private final int repairedReplicaCount;
        
        public RepairResult(String objectKey, boolean success, String message, int repairedReplicaCount) {
            this.objectKey = objectKey;
            this.success = success;
            this.message = message;
            this.repairedReplicaCount = repairedReplicaCount;
        }
        
        public String getObjectKey() { return objectKey; }
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public int getRepairedReplicaCount() { return repairedReplicaCount; }
    }
    
    public static class ValidationStats {
        private final long validationCount;
        private final long repairCount;
        
        public ValidationStats(long validationCount, long repairCount) {
            this.validationCount = validationCount;
            this.repairCount = repairCount;
        }
        
        public long getValidationCount() { return validationCount; }
        public long getRepairCount() { return repairCount; }
    }
}