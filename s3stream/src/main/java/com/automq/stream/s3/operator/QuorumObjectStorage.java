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

import com.automq.stream.s3.quorum.monitoring.QuorumMetricsCollector;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * QuorumObjectStorage implements multi-replica object storage with quorum-based reads and writes.
 * This ensures data durability and availability across multiple S3 storage backends.
 */
public class QuorumObjectStorage implements ObjectStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumObjectStorage.class);
    
    private final List<ObjectStorage> replicas;
    private final int writeQuorumSize;
    private final int readQuorumSize;
    private final boolean quorumEnabled;
    private final QuorumMetricsCollector metricsCollector;
    
    /**
     * Constructor for QuorumObjectStorage
     */
    public QuorumObjectStorage(ObjectStorageFactory.Builder builder) {
        this.writeQuorumSize = builder.writeQuorumSize();
        this.readQuorumSize = builder.readQuorumSize();
        this.quorumEnabled = builder.quorumEnabled();
        
        // Initialize metrics collector
        QuorumMetricsCollector.MetricsConfig metricsConfig = 
            new QuorumMetricsCollector.MetricsConfig(true, 30, true); // Enable JMX, 30s reporting interval
        this.metricsCollector = new QuorumMetricsCollector(metricsConfig);
        this.metricsCollector.start();
        
        // Create individual ObjectStorage instances for each bucket
        this.replicas = createReplicasInternal(builder);
        
        // Initialize replica count metric
        this.metricsCollector.setGauge("replica-count", replicas.size());
        this.metricsCollector.setGauge("write-quorum-size", writeQuorumSize);
        this.metricsCollector.setGauge("read-quorum-size", readQuorumSize);
        
        LOGGER.info("QuorumObjectStorage initialized with {} replicas, writeQuorum={}, readQuorum={}", 
                   replicas.size(), writeQuorumSize, readQuorumSize);
    }
    
    /**
     * Create replica ObjectStorage instances - can be overridden for testing
     */
    protected List<ObjectStorage> createReplicas(ObjectStorageFactory.Builder builder) {
        return createReplicasInternal(builder);
    }
    
    /**
     * Internal method to create replicas without virtual method call during construction
     */
    private List<ObjectStorage> createReplicasInternal(ObjectStorageFactory.Builder builder) {
        List<ObjectStorage> replicaList = new ArrayList<>();
        List<BucketURI> buckets = builder.buckets();
        
        if (buckets == null || buckets.isEmpty()) {
            throw new IllegalArgumentException("No buckets configured for QuorumObjectStorage");
        }
        
        for (BucketURI bucketURI : buckets) {
            ObjectStorage replica = ObjectStorageFactory.instance()
                .builder(bucketURI)
                .tagging(builder.tagging())
                .inboundLimiter(builder.inboundLimiter())
                .outboundLimiter(builder.outboundLimiter())
                .readWriteIsolate(builder.readWriteIsolate())
                .checkS3ApiModel(builder.checkS3ApiModel())
                .threadPrefix(builder.threadPrefix() + "-" + bucketURI.bucketId())
                .build();
            replicaList.add(replica);
        }
        
        return replicaList;
    }
    
    @Override
    public CompletableFuture<WriteResult> write(WriteOptions options, String objectKey, ByteBuf data) {
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("write")) {
            metricsCollector.incrementGauge("write-requests-total");
            
            LOGGER.debug("QuorumObjectStorage.write() - quorumEnabled: {}, replicas: {}, objectKey: {}", 
                        quorumEnabled, replicas.size(), objectKey);
            
            if (!quorumEnabled || replicas.size() == 1) {
                // Fall back to single replica write
                return replicas.get(0).write(options, objectKey, data)
                    .whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            timer.success();
                            metricsCollector.incrementGauge("write-successes-single");
                        } else {
                            timer.failure();
                            metricsCollector.incrementGauge("write-failures-single");
                        }
                    });
            }
            
            return performQuorumWrite(options, objectKey, data)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        timer.success();
                        metricsCollector.incrementGauge("write-successes-quorum");
                    } else {
                        timer.failure();
                        metricsCollector.incrementGauge("write-failures-quorum");
                    }
                });
        }
    }
    
    private CompletableFuture<WriteResult> performQuorumWrite(WriteOptions options, String objectKey, ByteBuf data) {
        return performQuorumWriteWithRetry(options, objectKey, data, 2); // Default: 2 retries
    }
    
    private CompletableFuture<WriteResult> performQuorumWriteWithRetry(WriteOptions options, String objectKey, ByteBuf data, int retryCount) {
        LOGGER.debug("Performing quorum write for object: {} to {} replicas (retries left: {})", objectKey, replicas.size(), retryCount);
        
        // Create write tasks for all replicas
        List<CompletableFuture<WriteResult>> writeFutures = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            final int replicaIndex = i;  // Make final for lambda
            ObjectStorage replica = replicas.get(i);
            // Create an independent copy of the data for each replica
            ByteBuf dataCopy = data.alloc().buffer(data.readableBytes());
            dataCopy.writeBytes(data, data.readerIndex(), data.readableBytes());
            
            CompletableFuture<WriteResult> writeFuture = replica.write(options, objectKey, dataCopy)
                .whenComplete((result, throwable) -> {
                    // Release the data copy when done
                    dataCopy.release();
                    if (throwable != null) {
                        LOGGER.warn("Write failed for replica {}: {}", replicaIndex, throwable.getMessage());
                    } else {
                        LOGGER.debug("Write succeeded for replica {}: {}", replicaIndex, result);
                    }
                });
            
            writeFutures.add(writeFuture);
        }
        
        // Wait for write quorum to succeed
        return waitForQuorum(writeFutures, writeQuorumSize, "write", objectKey)
            .exceptionally(throwable -> {
                if (retryCount > 0) {
                    LOGGER.info("Quorum write failed for {}, retrying... (retries left: {})", objectKey, retryCount - 1);
                    try {
                        Thread.sleep(100 * (3 - retryCount)); // Exponential backoff: 100ms, 200ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return performQuorumWriteWithRetry(options, objectKey, data, retryCount - 1).join();
                } else {
                    LOGGER.error("Quorum write failed for {} after all retries: {}", objectKey, throwable.getMessage());
                    throw new RuntimeException(throwable);
                }
            });
    }
    
    @Override
    public CompletableFuture<ByteBuf> read(ReadOptions options, String objectKey) {
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("read")) {
            metricsCollector.incrementGauge("read-requests-total");
            
            if (!quorumEnabled || replicas.size() == 1) {
                // Fall back to single replica read
                return replicas.get(0).read(options, objectKey)
                    .whenComplete((result, throwable) -> {
                        if (throwable == null) {
                            timer.success();
                            metricsCollector.incrementGauge("read-successes-single");
                        } else {
                            timer.failure();
                            metricsCollector.incrementGauge("read-failures-single");
                        }
                    });
            }
            
            return performQuorumRead(options, objectKey)
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        timer.success();
                        metricsCollector.incrementGauge("read-successes-quorum");
                    } else {
                        timer.failure();
                        metricsCollector.incrementGauge("read-failures-quorum");
                    }
                });
        }
    }
    
    private CompletableFuture<ByteBuf> performQuorumRead(ReadOptions options, String objectKey) {
        return performQuorumReadWithRetry(options, objectKey, 2); // Default: 2 retries
    }
    
    private CompletableFuture<ByteBuf> performQuorumReadWithRetry(ReadOptions options, String objectKey, int retryCount) {
        LOGGER.debug("Performing quorum read for object: {} from {} replicas (retries left: {})", objectKey, replicas.size(), retryCount);
        
        // Try to read from healthy replicas first, then fallback to all replicas
        List<ObjectStorage> healthyReplicas = replicas.stream()
            .filter(ObjectStorage::readinessCheck)
            .collect(Collectors.toList());
        
        List<ObjectStorage> replicasToTry = healthyReplicas.isEmpty() ? replicas : healthyReplicas;
        LOGGER.debug("Using {} healthy replicas out of {} total for read operation", replicasToTry.size(), replicas.size());
        
        // Track read consistency for monitoring
        metricsCollector.setGauge("healthy-replicas-count", healthyReplicas.size());
        
        List<CompletableFuture<ByteBuf>> readFutures = new ArrayList<>();
        
        for (int i = 0; i < replicasToTry.size(); i++) {
            final int replicaIndex = i;  // Make final for lambda
            ObjectStorage replica = replicasToTry.get(i);
            CompletableFuture<ByteBuf> readFuture = replica.read(options, objectKey)
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        LOGGER.warn("Read failed for replica {}: {}", replicaIndex, throwable.getMessage());
                    } else {
                        LOGGER.debug("Read succeeded for replica {}: size={}", replicaIndex, result.readableBytes());
                    }
                });
            
            readFutures.add(readFuture);
        }
        
        // Return the first successful read with retry logic
        return waitForQuorum(readFutures, Math.min(readQuorumSize, replicasToTry.size()), "read", objectKey)
            .exceptionally(throwable -> {
                if (retryCount > 0) {
                    LOGGER.info("Quorum read failed for {}, retrying... (retries left: {})", objectKey, retryCount - 1);
                    try {
                        Thread.sleep(50 * (3 - retryCount)); // Exponential backoff: 50ms, 100ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return performQuorumReadWithRetry(options, objectKey, retryCount - 1).join();
                } else {
                    LOGGER.error("Quorum read failed for {} after all retries: {}", objectKey, throwable.getMessage());
                    throw new RuntimeException(throwable);
                }
            });
    }
    
    @Override
    public CompletableFuture<List<ObjectInfo>> list(String prefix) {
        // For list operations, we'll use the first available replica
        // In a production system, you might want to merge results from multiple replicas
        return findHealthyReplica()
            .thenCompose(replica -> replica.list(prefix));
    }
    
    @Override
    public CompletableFuture<Void> delete(List<ObjectPath> objectPaths) {
        if (!quorumEnabled || replicas.size() == 1) {
            return replicas.get(0).delete(objectPaths);
        }
        
        // Delete from all replicas
        List<CompletableFuture<Void>> deleteFutures = replicas.stream()
            .map(replica -> replica.delete(objectPaths)
                .exceptionally(throwable -> {
                    LOGGER.warn("Delete failed for replica: {}", throwable.getMessage());
                    return null; // Continue with other deletes
                }))
            .collect(Collectors.toList());
        
        // Wait for majority of deletes to complete
        return waitForQuorum(deleteFutures, writeQuorumSize, "delete", "batch-objects")
            .thenApply(result -> null);
    }
    
    @Override
    public boolean readinessCheck() {
        // Check if at least read quorum replicas are ready
        int readyCount = 0;
        for (ObjectStorage replica : replicas) {
            if (replica.readinessCheck()) {
                readyCount++;
            }
        }
        
        // Update metrics for monitoring
        metricsCollector.setGauge("ready-replicas-count", readyCount);
        metricsCollector.setGauge("quorum-health-percentage", (readyCount * 100) / replicas.size());
        
        boolean isHealthy = readyCount >= readQuorumSize;
        if (!isHealthy) {
            LOGGER.warn("Quorum not healthy: only {}/{} replicas ready (required: {})", 
                       readyCount, replicas.size(), readQuorumSize);
            metricsCollector.incrementGauge("quorum-unhealthy-checks");
        }
        
        return isHealthy;
    }
    
    @Override
    public Writer writer(WriteOptions options, String objectPath) {
        if (!quorumEnabled || replicas.size() == 1) {
            // Fall back to single replica writer
            return replicas.get(0).writer(options, objectPath);
        }
        
        // Create writers for all replicas
        List<Writer> replicaWriters = new ArrayList<>();
        for (ObjectStorage replica : replicas) {
            Writer replicaWriter = replica.writer(options, objectPath);
            replicaWriters.add(replicaWriter);
        }
        
        // Return QuorumWriter that coordinates all replica writers
        return new QuorumWriter(replicaWriters, writeQuorumSize, objectPath);
    }
    
    @Override
    public CompletableFuture<ByteBuf> rangeRead(ReadOptions options, String objectPath, long start, long end) {
        if (!quorumEnabled || replicas.size() == 1) {
            return replicas.get(0).rangeRead(options, objectPath, start, end);
        }
        
        // Try range read from multiple replicas
        List<CompletableFuture<ByteBuf>> readFutures = new ArrayList<>();
        
        for (ObjectStorage replica : replicas) {
            readFutures.add(replica.rangeRead(options, objectPath, start, end)
                .exceptionally(throwable -> {
                    LOGGER.warn("Range read failed for replica: {}", throwable.getMessage());
                    return null;
                }));
        }
        
        return waitForQuorum(readFutures, readQuorumSize, "range-read", objectPath);
    }
    
    @Override
    public short bucketId() {
        // Return the bucket ID of the first replica
        return replicas.isEmpty() ? 0 : replicas.get(0).bucketId();
    }
    
    @Override
    public void close() {
        LOGGER.info("Closing QuorumObjectStorage with {} replicas", replicas.size());
        
        // Stop metrics collection
        try {
            metricsCollector.stop();
            LOGGER.info("Metrics collector stopped successfully");
        } catch (Exception e) {
            LOGGER.warn("Error stopping metrics collector: {}", e.getMessage());
        }
        
        // Close all replicas
        for (ObjectStorage replica : replicas) {
            try {
                replica.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing replica: {}", e.getMessage());
            }
        }
    }
    
    /**
     * Wait for a quorum of operations to complete successfully
     */
    private <T> CompletableFuture<T> waitForQuorum(List<CompletableFuture<T>> futures, 
                                                  int requiredSuccess, 
                                                  String operation, 
                                                  String objectKey) {
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger completedCount = new AtomicInteger(0);
        
        for (CompletableFuture<T> future : futures) {
            future.whenComplete((value, throwable) -> {
                int completed = completedCount.incrementAndGet();
                
                if (throwable == null) {
                    int success = successCount.incrementAndGet();
                    if (success >= requiredSuccess && !result.isDone()) {
                        LOGGER.debug("Quorum {} achieved for {}: {}/{} replicas succeeded", 
                                   operation, objectKey, success, replicas.size());
                        result.complete(value);
                    }
                } else {
                    LOGGER.debug("Replica failed for {} on {}: {}", operation, objectKey, throwable.getMessage());
                }
                
                // If all operations completed but we don't have quorum, fail
                if (completed == futures.size() && !result.isDone()) {
                    int success = successCount.get();
                    String errorMsg = String.format("Quorum %s failed for %s: only %d/%d replicas succeeded (required: %d)", 
                                                   operation, objectKey, success, replicas.size(), requiredSuccess);
                    LOGGER.error(errorMsg);
                    result.completeExceptionally(new CompletionException(errorMsg, null));
                }
            });
        }
        
        return result;
    }
    
    /**
     * Find a healthy replica for operations that don't require quorum
     */
    private CompletableFuture<ObjectStorage> findHealthyReplica() {
        // Try replicas in order until we find a healthy one
        for (ObjectStorage replica : replicas) {
            if (replica.readinessCheck()) {
                return CompletableFuture.completedFuture(replica);
            }
        }
        
        // If no replica is immediately ready, return first available
        // This maintains backward compatibility while providing better resilience
        LOGGER.warn("No healthy replicas found in readiness check, using first replica");
        return CompletableFuture.completedFuture(replicas.get(0));
    }
    
    /**
     * Get the number of configured replicas
     */
    public int getReplicaCount() {
        return replicas.size();
    }
    
    /**
     * Get write quorum size
     */
    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }
    
    /**
     * Get read quorum size
     */
    public int getReadQuorumSize() {
        return readQuorumSize;
    }
}