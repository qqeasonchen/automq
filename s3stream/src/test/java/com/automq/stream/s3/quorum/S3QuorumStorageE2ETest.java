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
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-End tests for S3QuorumStorage basic functionality.
 * These tests verify the core read/write operations work correctly
 * with multiple replicas in various scenarios.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("S3QuorumStorage E2E Basic Functionality Tests")
class S3QuorumStorageE2ETest {

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
        // Create 3-replica quorum configuration
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
    }

    @AfterEach
    void tearDown() {
        if (quorumStorage != null) {
            assertDoesNotThrow(() -> quorumStorage.shutdown());
        }
    }

    @Test
    @DisplayName("End-to-End write and read with all replicas healthy")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testE2EWriteAndReadAllHealthy() throws Exception {
        // Setup all storages as healthy
        setupHealthyStorages();
        
        quorumStorage.startup();
        
        // Create test data
        StreamRecordBatch batch = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("test-data-e2e".getBytes()));
        
        // Write data
        CompletableFuture<Void> writeFuture = quorumStorage.append(AppendContext.DEFAULT, batch);
        writeFuture.get(30, TimeUnit.SECONDS);
        
        // Read data back
        CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
            FetchContext.DEFAULT, 1L, 0L, 100L, 1024);
        ReadDataBlock result = readFuture.get(10, TimeUnit.SECONDS);
        
        // Verify
        assertNotNull(result);
        assertTrue(quorumStorage.getQuorumState().hasQuorum());
        assertEquals(3, quorumStorage.getQuorumState().getHealthyReplicaCount());
    }

    @Test
    @DisplayName("End-to-End write with one replica failure")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testE2EWriteWithOneReplicaFailure() throws Exception {
        // Setup: 2 healthy, 1 failing
        setupMixedHealthStorages();
        
        quorumStorage.startup();
        
        // Create test data
        StreamRecordBatch batch = new StreamRecordBatch(2L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("test-failure-tolerance".getBytes()));
        
        // Write should succeed with 2/3 quorum
        CompletableFuture<Void> writeFuture = quorumStorage.append(AppendContext.DEFAULT, batch);
        writeFuture.get(30, TimeUnit.SECONDS);
        
        // System should still maintain quorum
        assertTrue(quorumStorage.getQuorumState().hasQuorum());
        assertTrue(quorumStorage.getQuorumState().getHealthyReplicaCount() >= 2);
    }

    @Test
    @DisplayName("End-to-End read with primary replica failure and failover")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testE2EReadWithPrimaryFailureAndFailover() throws Exception {
        // Setup: Primary fails, secondary succeeds
        setupPrimaryFailureScenario();
        
        quorumStorage.startup();
        
        // Read should succeed via failover to secondary
        CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
            FetchContext.DEFAULT, 1L, 0L, 100L, 1024);
        ReadDataBlock result = readFuture.get(10, TimeUnit.SECONDS);
        
        // Verify failover worked
        assertNotNull(result);
    }

    @Test
    @DisplayName("End-to-End stress test with multiple concurrent operations")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testE2EConcurrentOperations() throws Exception {
        setupHealthyStorages();
        quorumStorage.startup();
        
        // Run multiple concurrent write operations
        List<CompletableFuture<Void>> writeFutures = new ArrayList<>();
        
        for (int i = 0; i < 10; i++) {
            StreamRecordBatch batch = new StreamRecordBatch(
                (long) i, 1L, 0L, 1, 
                Unpooled.wrappedBuffer(("concurrent-test-" + i).getBytes()));
                
            CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
            writeFutures.add(future);
        }
        
        // Wait for all writes to complete
        CompletableFuture<Void> allWrites = CompletableFuture.allOf(
            writeFutures.toArray(new CompletableFuture[0]));
        allWrites.get(60, TimeUnit.SECONDS);
        
        // Verify system state
        assertTrue(quorumStorage.getQuorumState().hasQuorum());
        assertEquals(3, quorumStorage.getQuorumState().getHealthyReplicaCount());
    }

    @Test
    @DisplayName("End-to-End lifecycle test - startup, operations, shutdown")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testE2ELifecycle() throws Exception {
        setupHealthyStorages();
        
        // Test startup
        assertDoesNotThrow(() -> quorumStorage.startup());
        assertTrue(quorumStorage.getQuorumState().hasQuorum());
        
        // Test operations
        StreamRecordBatch batch = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("lifecycle-test".getBytes()));
        CompletableFuture<Void> writeFuture = quorumStorage.append(AppendContext.DEFAULT, batch);
        writeFuture.get(30, TimeUnit.SECONDS);
        
        // Test shutdown
        assertDoesNotThrow(() -> quorumStorage.shutdown());
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
    
    private void setupHealthyStorages() {
        // Setup all storages as healthy
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
        
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        when(mockStorage1.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage2.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage3.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
    }
    
    private void setupMixedHealthStorages() {
        // Setup: 2 healthy, 1 failing
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Simulated failure")));
    }
    
    private void setupPrimaryFailureScenario() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        
        // Primary fails, secondary succeeds
        when(mockStorage1.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Primary failure")));
        when(mockStorage2.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage3.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Third replica failure")));
    }
}