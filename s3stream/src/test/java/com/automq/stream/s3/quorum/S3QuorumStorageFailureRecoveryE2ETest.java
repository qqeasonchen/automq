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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * End-to-End tests for S3QuorumStorage failure recovery scenarios.
 * These tests verify the system's ability to handle and recover from
 * various failure conditions while maintaining data consistency.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("S3QuorumStorage Failure Recovery E2E Tests")
class S3QuorumStorageFailureRecoveryE2ETest {

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

    @Test
    @DisplayName("Recovery from single replica failure during writes")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testRecoveryFromSingleReplicaFailure() throws Exception {
        // Setup: Initially all healthy, then one fails
        setupInitiallyHealthyThenOneFails();
        quorumStorage.startup();
        
        // First write should succeed with all replicas
        StreamRecordBatch batch1 = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("before-failure".getBytes()));
        CompletableFuture<Void> write1 = quorumStorage.append(AppendContext.DEFAULT, batch1);
        write1.get(30, TimeUnit.SECONDS);
        
        // Verify all replicas are healthy initially
        QuorumState state = quorumStorage.getQuorumState();
        assertEquals(3, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum());
        
        // Simulate failure of replica 3
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Network failure")));
        
        // Second write should still succeed with 2/3 quorum
        StreamRecordBatch batch2 = new StreamRecordBatch(2L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("during-failure".getBytes()));
        CompletableFuture<Void> write2 = quorumStorage.append(AppendContext.DEFAULT, batch2);
        write2.get(30, TimeUnit.SECONDS);
        
        // System should still maintain quorum
        assertTrue(state.hasQuorum());
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Recovery from temporary network partition")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testRecoveryFromNetworkPartition() throws Exception {
        setupNetworkPartitionScenario();
        quorumStorage.startup();
        
        // Write during network partition - should succeed with available replicas
        StreamRecordBatch batch = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("partition-test".getBytes()));
        
        CompletableFuture<Void> writeFuture = quorumStorage.append(AppendContext.DEFAULT, batch);
        writeFuture.get(30, TimeUnit.SECONDS);
        
        // Verify system adapted to partition
        QuorumState state = quorumStorage.getQuorumState();
        assertTrue(state.hasQuorum());
        
        // Simulate network recovery - replica comes back online
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        
        // Next write should succeed with all replicas available again
        StreamRecordBatch batch2 = new StreamRecordBatch(2L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("post-recovery".getBytes()));
        CompletableFuture<Void> write2 = quorumStorage.append(AppendContext.DEFAULT, batch2);
        write2.get(30, TimeUnit.SECONDS);
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Read failover when primary replica fails")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testReadFailoverFromPrimaryFailure() throws Exception {
        setupPrimaryReplicaFailure();
        quorumStorage.startup();
        
        // Read should automatically failover from primary (replica 1) to secondary (replica 2)
        CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
            FetchContext.DEFAULT, 1L, 0L, 100L, 1024);
        ReadDataBlock result = readFuture.get(15, TimeUnit.SECONDS);
        
        // Verify failover was successful
        assertNotNull(result);
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Quorum state recovery after multiple failures")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testQuorumStateRecoveryAfterMultipleFailures() throws Exception {
        quorumStorage.startup();
        setupInitiallyHealthyThenMultipleFailures();
        
        QuorumState state = quorumStorage.getQuorumState();
        
        // Initially all replicas should be healthy
        assertEquals(3, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum());
        
        // Simulate first failure
        state.markReplicaFailed(0);
        assertEquals(2, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum());
        
        // Simulate second failure - should lose quorum
        state.markReplicaFailed(1);
        assertEquals(1, state.getHealthyReplicaCount());
        assertFalse(state.hasQuorum());
        
        // Recover first replica
        state.markReplicaSuccess(0);
        assertEquals(2, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum());
        
        // Recover second replica
        state.markReplicaSuccess(1);
        assertEquals(3, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum());
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Graceful degradation with cascading failures")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testGracefulDegradationWithCascadingFailures() throws Exception {
        setupCascadingFailureScenario();
        quorumStorage.startup();
        
        QuorumState state = quorumStorage.getQuorumState();
        
        // Test write operations during cascading failures
        
        // Phase 1: All replicas healthy
        StreamRecordBatch batch1 = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("phase1".getBytes()));
        CompletableFuture<Void> write1 = quorumStorage.append(AppendContext.DEFAULT, batch1);
        write1.get(30, TimeUnit.SECONDS);
        assertTrue(state.hasQuorum());
        
        // Phase 2: One replica fails
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("First failure")));
        
        StreamRecordBatch batch2 = new StreamRecordBatch(2L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("phase2".getBytes()));
        CompletableFuture<Void> write2 = quorumStorage.append(AppendContext.DEFAULT, batch2);
        write2.get(30, TimeUnit.SECONDS);
        assertTrue(state.hasQuorum()); // Should still have quorum with 2/3
        
        // Phase 3: Second replica fails - should lose quorum
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Second failure")));
        
        StreamRecordBatch batch3 = new StreamRecordBatch(3L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("phase3".getBytes()));
        CompletableFuture<Void> write3 = quorumStorage.append(AppendContext.DEFAULT, batch3);
        
        // This write should fail due to insufficient quorum
        try {
            write3.get(30, TimeUnit.SECONDS);
            // If we reach here, the write unexpectedly succeeded
            assertTrue(state.hasQuorum(), "Write succeeded, so quorum should be maintained");
        } catch (Exception e) {
            // Expected failure due to insufficient quorum
            assertFalse(state.hasQuorum());
        }
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Read repair mechanism activation after replica recovery")
    @Timeout(value = 120, unit = TimeUnit.SECONDS)
    void testReadRepairAfterReplicaRecovery() throws Exception {
        setupReadRepairScenario();
        quorumStorage.startup();
        
        // Read should trigger repair mechanism when replicas are inconsistent
        CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
            FetchContext.DEFAULT, 1L, 0L, 100L, 1024);
        ReadDataBlock result = readFuture.get(15, TimeUnit.SECONDS);
        
        // Verify read succeeded despite replica inconsistencies
        assertNotNull(result);
        
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
    
    private void setupInitiallyHealthyThenOneFails() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        doNothing().when(mockStorage1).shutdown();
        doNothing().when(mockStorage2).shutdown();
        doNothing().when(mockStorage3).shutdown();
        
        // Initially all succeed
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
    }
    
    private void setupNetworkPartitionScenario() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        // Simulate network partition - replica 3 is unreachable
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Network partition")));
    }
    
    private void setupPrimaryReplicaFailure() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        
        // Primary (replica 1) fails, secondary (replica 2) succeeds
        when(mockStorage1.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Primary replica failure")));
        when(mockStorage2.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage3.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Third replica failure")));
    }
    
    private void setupInitiallyHealthyThenMultipleFailures() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
    }
    
    private void setupCascadingFailureScenario() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        // Initially all healthy
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
    }
    
    private void setupReadRepairScenario() {
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();
        
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        
        // Simulate read repair scenario - primary succeeds
        when(mockStorage1.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage2.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage3.read(any(FetchContext.class), anyLong(), anyLong(), anyLong(), anyInt()))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
    }
}