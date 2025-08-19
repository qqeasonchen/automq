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
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Integration tests for Quorum write operations.
 * These tests focus specifically on the write quorum mechanism,
 * ensuring proper coordination across replicas.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("Quorum Write Integration Tests")
class QuorumWriteIntegrationTest {

    @Mock
    private Storage mockStorage1;
    
    @Mock
    private Storage mockStorage2;
    
    @Mock
    private Storage mockStorage3;

    private S3QuorumStorage quorumStorage;
    private QuorumConfig quorumConfig;
    private List<Storage> replicas;

    @BeforeEach
    void setUp() {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs();
        
        quorumConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2) // Requires 2/3 for successful write
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
    }

    @Test
    @DisplayName("Quorum write succeeds with all replicas responding")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQuorumWriteAllReplicasSuccess() throws Exception {
        setupAllReplicasSuccess();
        quorumStorage.startup();
        
        StreamRecordBatch batch = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("quorum-test-all-success".getBytes()));
        
        // Execute write
        CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
        future.get(30, TimeUnit.SECONDS);
        
        // Verify all replicas were contacted
        verify(mockStorage1).append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        verify(mockStorage2).append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        verify(mockStorage3).append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Quorum write succeeds with minimum required replicas (2/3)")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQuorumWriteMinimumReplicas() throws Exception {
        setupTwoSuccessOneFailure();
        quorumStorage.startup();
        
        StreamRecordBatch batch = new StreamRecordBatch(2L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("quorum-test-minimum".getBytes()));
        
        // Execute write - should succeed with 2/3 quorum
        CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
        future.get(30, TimeUnit.SECONDS);
        
        // Verify all replicas were attempted
        verify(mockStorage1).append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        verify(mockStorage2).append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        verify(mockStorage3).append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Quorum write fails with insufficient replicas (1/3)")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQuorumWriteInsufficientReplicas() throws Exception {
        setupOneSuccessTwoFailures();
        quorumStorage.startup();
        
        StreamRecordBatch batch = new StreamRecordBatch(3L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("quorum-test-insufficient".getBytes()));
        
        // Execute write - should fail with only 1/3 quorum
        CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
        
        assertThrows(Exception.class, () -> future.get(30, TimeUnit.SECONDS));
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Concurrent quorum writes maintain consistency")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testConcurrentQuorumWrites() throws Exception {
        setupAllReplicasSuccess();
        quorumStorage.startup();
        
        int concurrentWrites = 5;
        CountDownLatch latch = new CountDownLatch(concurrentWrites);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (int i = 0; i < concurrentWrites; i++) {
            final int writeId = i;
            StreamRecordBatch batch = new StreamRecordBatch((long) writeId, 1L, 0L, 1, 
                Unpooled.wrappedBuffer(("concurrent-write-" + writeId).getBytes()));
            
            CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch)
                .whenComplete((result, throwable) -> latch.countDown());
            futures.add(future);
        }
        
        // Wait for all writes to complete
        latch.await(60, TimeUnit.SECONDS);
        
        // Verify all writes succeeded
        for (CompletableFuture<Void> future : futures) {
            future.get(1, TimeUnit.SECONDS); // Should be completed by now
        }
        
        // Verify each replica received all writes
        verify(mockStorage1, times(concurrentWrites))
            .append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        verify(mockStorage2, times(concurrentWrites))
            .append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        verify(mockStorage3, times(concurrentWrites))
            .append(eq(AppendContext.DEFAULT), any(StreamRecordBatch.class));
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Quorum write respects write timeout")
    @Timeout(value = 45, unit = TimeUnit.SECONDS)
    void testQuorumWriteTimeout() throws Exception {
        setupSlowReplicas();
        
        // Create config with shorter timeout
        QuorumConfig shortTimeoutConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(5000) // 5 second timeout
            .readTimeoutMs(10000)
            .replicaConfigs(createTestReplicaConfigs())
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();
            
        S3QuorumStorage timeoutQuorumStorage = new S3QuorumStorage(shortTimeoutConfig, replicas);
        timeoutQuorumStorage.startup();
        
        StreamRecordBatch batch = new StreamRecordBatch(4L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("timeout-test".getBytes()));
        
        CompletableFuture<Void> future = timeoutQuorumStorage.append(AppendContext.DEFAULT, batch);
        
        // Should timeout and fail
        assertThrows(Exception.class, () -> future.get(30, TimeUnit.SECONDS));
        
        timeoutQuorumStorage.shutdown();
    }

    @Test
    @DisplayName("Quorum write data consistency across replicas")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testQuorumWriteDataConsistency() throws Exception {
        setupAllReplicasSuccess();
        quorumStorage.startup();
        
        String testData = "consistency-test-data-12345";
        StreamRecordBatch batch = new StreamRecordBatch(5L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer(testData.getBytes()));
        
        // Execute write
        CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
        future.get(30, TimeUnit.SECONDS);
        
        // Capture the data sent to each replica
        ArgumentCaptor<StreamRecordBatch> batchCaptor1 = ArgumentCaptor.forClass(StreamRecordBatch.class);
        ArgumentCaptor<StreamRecordBatch> batchCaptor2 = ArgumentCaptor.forClass(StreamRecordBatch.class);
        ArgumentCaptor<StreamRecordBatch> batchCaptor3 = ArgumentCaptor.forClass(StreamRecordBatch.class);
        
        verify(mockStorage1).append(eq(AppendContext.DEFAULT), batchCaptor1.capture());
        verify(mockStorage2).append(eq(AppendContext.DEFAULT), batchCaptor2.capture());
        verify(mockStorage3).append(eq(AppendContext.DEFAULT), batchCaptor3.capture());
        
        // Verify same data was sent to all replicas
        StreamRecordBatch captured1 = batchCaptor1.getValue();
        StreamRecordBatch captured2 = batchCaptor2.getValue();
        StreamRecordBatch captured3 = batchCaptor3.getValue();
        
        assertEquals(captured1.getStreamId(), captured2.getStreamId());
        assertEquals(captured2.getStreamId(), captured3.getStreamId());
        assertEquals(captured1.getBaseOffset(), captured2.getBaseOffset());
        assertEquals(captured2.getBaseOffset(), captured3.getBaseOffset());
        
        quorumStorage.shutdown();
    }

    // Helper methods
    private List<ReplicaConfig> createTestReplicaConfigs() {
        List<ReplicaConfig> configs = new ArrayList<>();
        
        configs.add(ReplicaConfig.builder()
            .replicaId(0)
            .region("us-east-1")
            .bucket("test-bucket-1")
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
            .bucket("test-bucket-2")
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
            .bucket("test-bucket-3")
            .endpoint("https://s3.eu-west-1.amazonaws.com")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(25)
            .build());
            
        return configs;
    }
    
    private void setupAllReplicasSuccess() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        doNothing().when(mockStorage1).shutdown();
        doNothing().when(mockStorage2).shutdown();
        doNothing().when(mockStorage3).shutdown();
        
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
    }
    
    private void setupTwoSuccessOneFailure() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Replica 3 failure")));
    }
    
    private void setupOneSuccessTwoFailures() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Replica 2 failure")));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Replica 3 failure")));
    }
    
    private void setupSlowReplicas() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        // Setup slow responses that exceed timeout
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(createDelayedFuture(10000)); // 10 second delay
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(createDelayedFuture(10000)); // 10 second delay
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(createDelayedFuture(10000)); // 10 second delay
    }
    
    private CompletableFuture<Void> createDelayedFuture(long delayMs) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        // Simulate delay without actually blocking the test thread
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
}