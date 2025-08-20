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

package com.automq.stream.s3.quorum.performance;

import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.model.StreamRecordBatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * Manager for batching write operations to improve throughput and reduce latency
 * Collects individual write requests and batches them for more efficient processing
 */
public class BatchOperationManager {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchOperationManager.class);
    
    private final int maxBatchSize;
    private final long maxBatchWaitTimeMs;
    private final long maxBatchMemoryBytes;
    
    private final BlockingQueue<BatchedWriteRequest> pendingWrites = new LinkedBlockingQueue<>();
    private final ScheduledExecutorService batchProcessor;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong totalBatchesProcessed = new AtomicLong(0);
    private final AtomicLong totalRequestsBatched = new AtomicLong(0);
    private final AtomicLong totalBytesProcessed = new AtomicLong(0);
    
    // Function to execute the actual batch write operation
    private final Function<List<StreamRecordBatch>, CompletableFuture<Void>> batchWriteExecutor;
    
    public BatchOperationManager(int maxBatchSize, 
                                long maxBatchWaitTimeMs,
                                long maxBatchMemoryBytes,
                                Function<List<StreamRecordBatch>, CompletableFuture<Void>> batchWriteExecutor) {
        this.maxBatchSize = maxBatchSize;
        this.maxBatchWaitTimeMs = maxBatchWaitTimeMs;
        this.maxBatchMemoryBytes = maxBatchMemoryBytes;
        this.batchWriteExecutor = batchWriteExecutor;
        
        this.batchProcessor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "batch-operation-processor");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Start the batch operation manager
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Starting batch operation manager with maxBatchSize={}, maxWaitTime={}ms, maxMemory={}bytes",
                       maxBatchSize, maxBatchWaitTimeMs, maxBatchMemoryBytes);
            
            // Start the batch processing loop
            batchProcessor.scheduleWithFixedDelay(
                this::processBatch,
                0,
                Math.min(maxBatchWaitTimeMs / 4, 50), // Check frequently for batching opportunities
                TimeUnit.MILLISECONDS
            );
            
            LOGGER.info("Batch operation manager started");
        }
    }
    
    /**
     * Stop the batch operation manager
     */
    public void stop() {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Stopping batch operation manager");
            
            // Process any remaining requests
            processBatch();
            
            batchProcessor.shutdown();
            try {
                if (!batchProcessor.awaitTermination(5, TimeUnit.SECONDS)) {
                    batchProcessor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                batchProcessor.shutdownNow();
            }
            
            // Complete any remaining pending requests with error
            List<BatchedWriteRequest> remaining = new ArrayList<>();
            pendingWrites.drainTo(remaining);
            for (BatchedWriteRequest request : remaining) {
                request.future.completeExceptionally(
                    new IllegalStateException("Batch operation manager stopped"));
            }
            
            LOGGER.info("Batch operation manager stopped. Processed {} batches, {} requests, {} bytes",
                       totalBatchesProcessed.get(), totalRequestsBatched.get(), totalBytesProcessed.get());
        }
    }
    
    /**
     * Submit a write request for batching
     */
    public CompletableFuture<Void> submitWrite(AppendContext context, StreamRecordBatch streamRecord) {
        if (!started.get()) {
            return CompletableFuture.failedFuture(
                new IllegalStateException("Batch operation manager not started"));
        }
        
        BatchedWriteRequest request = new BatchedWriteRequest(
            context, streamRecord, System.currentTimeMillis());
        
        if (!pendingWrites.offer(request)) {
            // Queue is full, complete with error
            request.future.completeExceptionally(
                new IllegalStateException("Batch operation queue is full"));
        }
        
        return request.future;
    }
    
    /**
     * Force processing of current batch (useful for shutdown or testing)
     */
    public void flush() {
        processBatch();
    }
    
    /**
     * Get batch operation statistics
     */
    public BatchOperationStats getStats() {
        return new BatchOperationStats(
            started.get(),
            pendingWrites.size(),
            totalBatchesProcessed.get(),
            totalRequestsBatched.get(),
            totalBytesProcessed.get(),
            maxBatchSize,
            maxBatchWaitTimeMs,
            maxBatchMemoryBytes
        );
    }
    
    private void processBatch() {
        if (pendingWrites.isEmpty()) {
            return;
        }
        
        try {
            List<BatchedWriteRequest> batch = collectBatch();
            
            if (batch.isEmpty()) {
                return;
            }
            
            executeBatch(batch);
            
        } catch (Exception e) {
            LOGGER.error("Error processing batch", e);
        }
    }
    
    private List<BatchedWriteRequest> collectBatch() {
        List<BatchedWriteRequest> batch = new ArrayList<>();
        long currentTime = System.currentTimeMillis();
        long totalBytes = 0;
        
        // Collect requests for batching
        while (!pendingWrites.isEmpty() && batch.size() < maxBatchSize) {
            BatchedWriteRequest request = pendingWrites.peek();
            
            if (request == null) {
                break;
            }
            
            // Check if we should wait longer for more requests
            if (batch.isEmpty()) {
                // Always take the first request
                request = pendingWrites.poll();
                if (request != null) {
                    batch.add(request);
                    totalBytes += request.getEstimatedSize();
                }
            } else {
                // Check various batching criteria
                boolean shouldBatch = false;
                
                // Time-based batching: if oldest request is old enough, batch now
                BatchedWriteRequest oldest = batch.get(0);
                if (currentTime - oldest.submissionTime >= maxBatchWaitTimeMs) {
                    shouldBatch = true;
                }
                
                // Size-based batching: if adding this request would exceed memory limit
                long requestSize = request.getEstimatedSize();
                if (totalBytes + requestSize > maxBatchMemoryBytes) {
                    shouldBatch = true;
                    break; // Don't include this request
                }
                
                // Count-based batching: if we're at max batch size
                if (batch.size() >= maxBatchSize) {
                    shouldBatch = true;
                    break; // Don't include this request
                }
                
                if (shouldBatch) {
                    break;
                }
                
                // Include this request in the batch
                request = pendingWrites.poll();
                if (request != null) {
                    batch.add(request);
                    totalBytes += requestSize;
                }
            }
        }
        
        return batch;
    }
    
    private void executeBatch(List<BatchedWriteRequest> batch) {
        if (batch.isEmpty()) {
            return;
        }
        
        long startTime = System.currentTimeMillis();
        
        LOGGER.debug("Executing batch of {} requests", batch.size());
        
        // Extract stream records for batch execution
        List<StreamRecordBatch> streamRecords = new ArrayList<>();
        for (BatchedWriteRequest request : batch) {
            streamRecords.add(request.streamRecord);
        }
        
        // Execute the batch write
        CompletableFuture<Void> batchFuture = batchWriteExecutor.apply(streamRecords);
        
        batchFuture.whenComplete((result, throwable) -> {
            long duration = System.currentTimeMillis() - startTime;
            
            if (throwable == null) {
                LOGGER.debug("Batch of {} requests completed successfully in {}ms", 
                           batch.size(), duration);
                
                // Complete all individual futures successfully
                for (BatchedWriteRequest request : batch) {
                    request.future.complete(null);
                }
                
                // Update statistics
                totalBatchesProcessed.incrementAndGet();
                totalRequestsBatched.addAndGet(batch.size());
                
                long totalBytes = batch.stream()
                    .mapToLong(BatchedWriteRequest::getEstimatedSize)
                    .sum();
                totalBytesProcessed.addAndGet(totalBytes);
                
            } else {
                LOGGER.warn("Batch of {} requests failed in {}ms", batch.size(), duration, throwable);
                
                // Complete all individual futures with the same error
                for (BatchedWriteRequest request : batch) {
                    request.future.completeExceptionally(throwable);
                }
            }
        });
    }
    
    /**
     * Represents a batched write request
     */
    private static class BatchedWriteRequest {
        final AppendContext context;
        final StreamRecordBatch streamRecord;
        final long submissionTime;
        final CompletableFuture<Void> future;
        
        BatchedWriteRequest(AppendContext context, StreamRecordBatch streamRecord, long submissionTime) {
            this.context = context;
            this.streamRecord = streamRecord;
            this.submissionTime = submissionTime;
            this.future = new CompletableFuture<>();
        }
        
        long getEstimatedSize() {
            // Estimate the size of the stream record
            return streamRecord.encoded().readableBytes();
        }
    }
    
    /**
     * Statistics about batch operations
     */
    public static class BatchOperationStats {
        private final boolean running;
        private final int pendingRequests;
        private final long totalBatchesProcessed;
        private final long totalRequestsBatched;
        private final long totalBytesProcessed;
        private final int maxBatchSize;
        private final long maxBatchWaitTimeMs;
        private final long maxBatchMemoryBytes;
        
        public BatchOperationStats(boolean running, int pendingRequests, long totalBatchesProcessed,
                                  long totalRequestsBatched, long totalBytesProcessed,
                                  int maxBatchSize, long maxBatchWaitTimeMs, long maxBatchMemoryBytes) {
            this.running = running;
            this.pendingRequests = pendingRequests;
            this.totalBatchesProcessed = totalBatchesProcessed;
            this.totalRequestsBatched = totalRequestsBatched;
            this.totalBytesProcessed = totalBytesProcessed;
            this.maxBatchSize = maxBatchSize;
            this.maxBatchWaitTimeMs = maxBatchWaitTimeMs;
            this.maxBatchMemoryBytes = maxBatchMemoryBytes;
        }
        
        public boolean isRunning() { return running; }
        public int getPendingRequests() { return pendingRequests; }
        public long getTotalBatchesProcessed() { return totalBatchesProcessed; }
        public long getTotalRequestsBatched() { return totalRequestsBatched; }
        public long getTotalBytesProcessed() { return totalBytesProcessed; }
        public int getMaxBatchSize() { return maxBatchSize; }
        public long getMaxBatchWaitTimeMs() { return maxBatchWaitTimeMs; }
        public long getMaxBatchMemoryBytes() { return maxBatchMemoryBytes; }
        
        public double getAverageBatchSize() {
            return totalBatchesProcessed > 0 ? 
                (double) totalRequestsBatched / totalBatchesProcessed : 0.0;
        }
        
        public double getAverageBatchBytes() {
            return totalBatchesProcessed > 0 ? 
                (double) totalBytesProcessed / totalBatchesProcessed : 0.0;
        }
        
        @Override
        public String toString() {
            return String.format(
                "BatchOperationStats{running=%s, pending=%d, batches=%d, requests=%d, bytes=%d, " +
                "avgBatchSize=%.1f, avgBatchBytes=%.1f}",
                running, pendingRequests, totalBatchesProcessed, totalRequestsBatched, totalBytesProcessed,
                getAverageBatchSize(), getAverageBatchBytes()
            );
        }
    }
}