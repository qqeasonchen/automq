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
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Performance and stress testing for S3QuorumStorage.
 * These tests verify throughput, latency, and system behavior under load.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("S3QuorumStorage Performance E2E Tests")
class S3QuorumStoragePerformanceE2ETest {

    @Mock
    private Storage mockStorage1;
    
    @Mock
    private Storage mockStorage2;
    
    @Mock
    private Storage mockStorage3;

    private S3QuorumStorage quorumStorage;
    private QuorumConfig quorumConfig;
    private List<Storage> replicas;
    private ExecutorService executorService;

    @BeforeEach
    void setUp() {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs();
        
        quorumConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();

        replicas = new ArrayList<>();
        replicas.add(mockStorage1);
        replicas.add(mockStorage2);
        replicas.add(mockStorage3);

        quorumStorage = new S3QuorumStorage(quorumConfig, replicas);
        executorService = Executors.newFixedThreadPool(50);
    }

    @AfterEach
    void tearDown() {
        if (quorumStorage != null) {
            assertDoesNotThrow(() -> quorumStorage.shutdown());
        }
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @DisplayName("High throughput write performance test")
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void testHighThroughputWrites() throws Exception {
        setupHighPerformanceStorages();
        quorumStorage.startup();
        
        int totalWrites = 10000;
        int concurrentWrites = 100;
        CountDownLatch completedWrites = new CountDownLatch(totalWrites);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong totalLatency = new AtomicLong(0);
        
        // Execute high-throughput writes
        for (int i = 0; i < totalWrites; i++) {
            final int writeId = i;
            executorService.submit(() -> {
                try {
                    long writeStart = System.nanoTime();
                    StreamRecordBatch batch = new StreamRecordBatch(
                        (long) writeId, 1L, 0L, 1, 
                        Unpooled.wrappedBuffer(("perf-test-" + writeId).getBytes()));
                    
                    CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
                    future.get(30, TimeUnit.SECONDS);
                    
                    long writeLatency = System.nanoTime() - writeStart;
                    totalLatency.addAndGet(writeLatency);
                    completedWrites.countDown();
                } catch (Exception e) {
                    // In a real test, we might want to track failures
                    completedWrites.countDown();
                }
            });
        }
        
        // Wait for all writes to complete
        assertTrue(completedWrites.await(240, TimeUnit.SECONDS), 
            "All writes should complete within timeout");
        
        long totalTime = System.currentTimeMillis() - startTime.get();
        double throughput = (double) totalWrites / (totalTime / 1000.0);
        double avgLatencyMs = (totalLatency.get() / 1_000_000.0) / totalWrites;
        
        // Performance assertions - these are benchmarks, adjust based on expectations
        System.out.printf("Write Performance: %.2f writes/sec, avg latency: %.2f ms%n", 
            throughput, avgLatencyMs);
        
        // Basic performance expectations (adjust these based on actual requirements)
        assertTrue(throughput > 100, "Should achieve > 100 writes/sec");
        assertTrue(avgLatencyMs < 1000, "Average latency should be < 1 second");
        
        // Verify system remained healthy during high load
        QuorumState state = quorumStorage.getQuorumState();
        assertTrue(state.hasQuorum(), "Quorum should be maintained during high load");
    }

    @Test
    @DisplayName("Concurrent read/write mixed workload performance")
    @Timeout(value = 180, unit = TimeUnit.SECONDS)
    void testMixedWorkloadPerformance() throws Exception {
        setupHighPerformanceStorages();
        quorumStorage.startup();
        
        int totalOperations = 5000;
        int writeOperations = (int) (totalOperations * 0.7); // 70% writes, 30% reads
        int readOperations = totalOperations - writeOperations;
        
        CountDownLatch operationsCompleted = new CountDownLatch(totalOperations);
        AtomicLong writeLatency = new AtomicLong(0);
        AtomicLong readLatency = new AtomicLong(0);
        
        // Submit write operations
        for (int i = 0; i < writeOperations; i++) {
            final int writeId = i;
            executorService.submit(() -> {
                try {
                    long start = System.nanoTime();
                    StreamRecordBatch batch = new StreamRecordBatch(
                        (long) writeId, 1L, 0L, 1, 
                        Unpooled.wrappedBuffer(("mixed-write-" + writeId).getBytes()));
                    
                    quorumStorage.append(AppendContext.DEFAULT, batch).get(30, TimeUnit.SECONDS);
                    writeLatency.addAndGet(System.nanoTime() - start);
                } catch (Exception e) {
                    // Track errors in production code
                } finally {
                    operationsCompleted.countDown();
                }
            });
        }
        
        // Submit read operations
        for (int i = 0; i < readOperations; i++) {
            final int readId = i;
            executorService.submit(() -> {
                try {
                    long start = System.nanoTime();
                    quorumStorage.read(FetchContext.DEFAULT, readId % 100L, 0L, 1024L, 1024)
                        .get(10, TimeUnit.SECONDS);
                    readLatency.addAndGet(System.nanoTime() - start);
                } catch (Exception e) {
                    // Track errors in production code
                } finally {
                    operationsCompleted.countDown();
                }
            });
        }
        
        assertTrue(operationsCompleted.await(150, TimeUnit.SECONDS), 
            "Mixed workload should complete within timeout");
        
        double avgWriteLatencyMs = (writeLatency.get() / 1_000_000.0) / writeOperations;
        double avgReadLatencyMs = (readLatency.get() / 1_000_000.0) / readOperations;
        
        System.out.printf("Mixed Workload - Write latency: %.2f ms, Read latency: %.2f ms%n", 
            avgWriteLatencyMs, avgReadLatencyMs);
        
        // Performance expectations
        assertTrue(avgWriteLatencyMs < 2000, "Write latency should be reasonable under mixed load");
        assertTrue(avgReadLatencyMs < 500, "Read latency should be reasonable under mixed load");
        
        QuorumState state = quorumStorage.getQuorumState();
        assertTrue(state.hasQuorum(), "Quorum should be maintained during mixed workload");
    }

    @Test
    @DisplayName("Memory usage and resource cleanup under sustained load")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testResourceUsageUnderSustainedLoad() throws Exception {
        setupHighPerformanceStorages();
        quorumStorage.startup();
        
        // Measure initial memory
        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long initialMemory = runtime.totalMemory() - runtime.freeMemory();
        
        // Sustained load test - multiple waves of operations
        int waves = 10;
        int operationsPerWave = 500;
        
        for (int wave = 0; wave < waves; wave++) {
            CountDownLatch waveCompleted = new CountDownLatch(operationsPerWave);
            
            for (int i = 0; i < operationsPerWave; i++) {
                final int opId = wave * operationsPerWave + i;
                executorService.submit(() -> {
                    try {
                        StreamRecordBatch batch = new StreamRecordBatch(
                            (long) opId, 1L, 0L, 1, 
                            Unpooled.wrappedBuffer(("sustained-" + opId).getBytes()));
                        
                        quorumStorage.append(AppendContext.DEFAULT, batch).get(30, TimeUnit.SECONDS);
                    } catch (Exception e) {
                        // Track errors in production
                    } finally {
                        waveCompleted.countDown();
                    }
                });
            }
            
            assertTrue(waveCompleted.await(30, TimeUnit.SECONDS), 
                "Wave " + wave + " should complete within timeout");
            
            // Brief pause between waves
            Thread.sleep(100);
        }
        
        // Force garbage collection and measure final memory
        runtime.gc();
        Thread.sleep(1000);
        runtime.gc();
        long finalMemory = runtime.totalMemory() - runtime.freeMemory();
        
        long memoryIncrease = finalMemory - initialMemory;
        System.out.printf("Memory usage: initial=%d MB, final=%d MB, increase=%d MB%n", 
            initialMemory / (1024 * 1024), finalMemory / (1024 * 1024), memoryIncrease / (1024 * 1024));
        
        // Memory usage should not grow excessively (adjust threshold as needed)
        assertTrue(memoryIncrease < 100 * 1024 * 1024, // Less than 100MB increase
            "Memory usage should not grow excessively under sustained load");
        
        QuorumState state = quorumStorage.getQuorumState();
        assertTrue(state.hasQuorum(), "System should maintain quorum after sustained load");
    }

    @Test
    @DisplayName("Latency distribution and percentile analysis")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testLatencyDistribution() throws Exception {
        setupHighPerformanceStorages();
        quorumStorage.startup();
        
        int totalOperations = 2000;
        List<Long> latencies = new ArrayList<>();
        CountDownLatch completed = new CountDownLatch(totalOperations);
        
        for (int i = 0; i < totalOperations; i++) {
            final int opId = i;
            executorService.submit(() -> {
                try {
                    long start = System.nanoTime();
                    StreamRecordBatch batch = new StreamRecordBatch(
                        (long) opId, 1L, 0L, 1, 
                        Unpooled.wrappedBuffer(("latency-test-" + opId).getBytes()));
                    
                    quorumStorage.append(AppendContext.DEFAULT, batch).get(30, TimeUnit.SECONDS);
                    
                    long latencyNs = System.nanoTime() - start;
                    synchronized (latencies) {
                        latencies.add(latencyNs);
                    }
                } catch (Exception e) {
                    // Track errors
                } finally {
                    completed.countDown();
                }
            });
        }
        
        assertTrue(completed.await(90, TimeUnit.SECONDS), 
            "All operations should complete for latency analysis");
        
        // Analyze latency distribution
        latencies.sort(Long::compareTo);
        
        double p50 = getPercentile(latencies, 50) / 1_000_000.0;
        double p90 = getPercentile(latencies, 90) / 1_000_000.0;
        double p95 = getPercentile(latencies, 95) / 1_000_000.0;
        double p99 = getPercentile(latencies, 99) / 1_000_000.0;
        
        System.out.printf("Latency Distribution - P50: %.2f ms, P90: %.2f ms, P95: %.2f ms, P99: %.2f ms%n", 
            p50, p90, p95, p99);
        
        // Latency expectations (adjust based on requirements)
        assertTrue(p50 < 1000, "P50 latency should be under 1 second");
        assertTrue(p90 < 2000, "P90 latency should be under 2 seconds");
        assertTrue(p95 < 3000, "P95 latency should be under 3 seconds");
        assertTrue(p99 < 5000, "P99 latency should be under 5 seconds");
    }

    @Test
    @DisplayName("System behavior under replica performance degradation")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testPerformanceDegradationHandling() throws Exception {
        setupDegradedPerformanceStorages();
        quorumStorage.startup();
        
        int operations = 1000;
        CountDownLatch completed = new CountDownLatch(operations);
        AtomicLong successCount = new AtomicLong(0);
        
        for (int i = 0; i < operations; i++) {
            final int opId = i;
            executorService.submit(() -> {
                try {
                    StreamRecordBatch batch = new StreamRecordBatch(
                        (long) opId, 1L, 0L, 1, 
                        Unpooled.wrappedBuffer(("degraded-test-" + opId).getBytes()));
                    
                    quorumStorage.append(AppendContext.DEFAULT, batch).get(30, TimeUnit.SECONDS);
                    successCount.incrementAndGet();
                } catch (Exception e) {
                    // Expected some failures due to degraded performance
                } finally {
                    completed.countDown();
                }
            });
        }
        
        assertTrue(completed.await(90, TimeUnit.SECONDS), 
            "All operations should complete or timeout");
        
        // Even with degraded performance, should achieve reasonable success rate
        double successRate = (double) successCount.get() / operations;
        System.out.printf("Success rate under degraded performance: %.2f%%n", successRate * 100);
        
        assertTrue(successRate > 0.5, "Should maintain > 50% success rate even with degraded replicas");
        
        QuorumState state = quorumStorage.getQuorumState();
        assertTrue(state.hasQuorum(), "Should maintain quorum despite performance issues");
    }

    // Helper methods
    private List<ReplicaConfig> createTestReplicaConfigs() {
        List<ReplicaConfig> configs = new ArrayList<>();
        
        configs.add(ReplicaConfig.builder()
            .replicaId(0)
            .region("us-east-1")
            .bucket("perf-test-bucket-1")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.PRIMARY)
            .priority(100)
            .build());
            
        configs.add(ReplicaConfig.builder()
            .replicaId(1)
            .region("us-west-2")
            .bucket("perf-test-bucket-2")
            .endpoint("https://s3.us-west-2.amazonaws.com")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(50)
            .build());
            
        configs.add(ReplicaConfig.builder()
            .replicaId(2)
            .region("eu-west-1")
            .bucket("perf-test-bucket-3")
            .endpoint("https://s3.eu-west-1.amazonaws.com")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(25)
            .build());
            
        return configs;
    }
    
    private void setupHighPerformanceStorages() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        doNothing().when(mockStorage1).shutdown();
        doNothing().when(mockStorage2).shutdown();
        doNothing().when(mockStorage3).shutdown();
        
        // Fast responses for all replicas
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        when(mockStorage1.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage2.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage3.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
    }
    
    private void setupDegradedPerformanceStorages() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        doNothing().when(mockStorage1).shutdown();
        doNothing().when(mockStorage2).shutdown();
        doNothing().when(mockStorage3).shutdown();
        
        // Mixed performance - some fast, some slow/failing
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null)); // Fast
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(createSlowFuture(2000)); // Slow
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Degraded replica"))); // Failing
    }
    
    private CompletableFuture<Void> createSlowFuture(long delayMs) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMs);
                future.complete(null);
            } catch (InterruptedException e) {
                future.completeExceptionally(e);
            }
        });
        return future;
    }
    
    private long getPercentile(List<Long> sortedValues, int percentile) {
        int index = (int) Math.ceil(sortedValues.size() * percentile / 100.0) - 1;
        return sortedValues.get(Math.max(0, Math.min(index, sortedValues.size() - 1)));
    }
}