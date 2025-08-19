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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class S3QuorumStorageTest {

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
        // Create replica configurations
        List<ReplicaConfig> replicaConfigs = new ArrayList<>();
        replicaConfigs.add(ReplicaConfig.builder()
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
        replicaConfigs.add(ReplicaConfig.builder()
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
        replicaConfigs.add(ReplicaConfig.builder()
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

        // Create quorum configuration
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

        // Create mock replicas
        replicas = new ArrayList<>();
        replicas.add(mockStorage1);
        replicas.add(mockStorage2);
        replicas.add(mockStorage3);

        // Create quorum storage
        quorumStorage = new S3QuorumStorage(quorumConfig, replicas);
    }

    @Test
    void testStartup() {
        // Setup mocks
        doNothing().when(mockStorage1).startup();
        doNothing().when(mockStorage2).startup();
        doNothing().when(mockStorage3).startup();

        // Execute
        assertDoesNotThrow(() -> quorumStorage.startup());

        // Verify
        verify(mockStorage1).startup();
        verify(mockStorage2).startup();
        verify(mockStorage3).startup();
    }

    @Test
    void testShutdown() {
        // Setup mocks
        doNothing().when(mockStorage1).shutdown();
        doNothing().when(mockStorage2).shutdown();
        doNothing().when(mockStorage3).shutdown();

        // Execute
        assertDoesNotThrow(() -> quorumStorage.shutdown());

        // Verify
        verify(mockStorage1).shutdown();
        verify(mockStorage2).shutdown();
        verify(mockStorage3).shutdown();
    }

    @Test
    void testSuccessfulQuorumWrite() throws ExecutionException, InterruptedException, TimeoutException {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Setup mocks for successful write
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        // Create test data
        StreamRecordBatch recordBatch = new StreamRecordBatch(1L, 1L, 0L, 1, Unpooled.wrappedBuffer("test".getBytes()));
        AppendContext context = AppendContext.DEFAULT;

        // Execute
        CompletableFuture<Void> future = quorumStorage.append(context, recordBatch);
        future.get(5, TimeUnit.SECONDS);

        // Verify
        verify(mockStorage1).append(context, recordBatch);
        verify(mockStorage2).append(context, recordBatch);
        verify(mockStorage3).append(context, recordBatch);
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testQuorumWriteWithOneFailure() throws ExecutionException, InterruptedException, TimeoutException {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Setup mocks - one failure, two successes
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Storage failure")));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));

        // Create test data
        StreamRecordBatch recordBatch = new StreamRecordBatch(1L, 1L, 0L, 1, Unpooled.wrappedBuffer("test".getBytes()));
        AppendContext context = AppendContext.DEFAULT;

        // Execute - should succeed with 2/3 quorum
        CompletableFuture<Void> future = quorumStorage.append(context, recordBatch);
        future.get(5, TimeUnit.SECONDS);

        // Verify
        verify(mockStorage1).append(context, recordBatch);
        verify(mockStorage2).append(context, recordBatch);
        verify(mockStorage3).append(context, recordBatch);
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testQuorumWriteWithTwoFailures() {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Setup mocks - two failures, one success
        when(mockStorage1.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Storage failure")));
        when(mockStorage3.append(any(AppendContext.class), any(StreamRecordBatch.class)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Storage failure")));

        // Create test data
        StreamRecordBatch recordBatch = new StreamRecordBatch(1L, 1L, 0L, 1, Unpooled.wrappedBuffer("test".getBytes()));
        AppendContext context = AppendContext.DEFAULT;

        // Execute - should fail with only 1/3 quorum
        CompletableFuture<Void> future = quorumStorage.append(context, recordBatch);
        
        // Verify that the future fails
        assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        
        // Verify all replicas were attempted
        verify(mockStorage1).append(context, recordBatch);
        verify(mockStorage2).append(context, recordBatch);
        verify(mockStorage3).append(context, recordBatch);
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testSuccessfulQuorumRead() throws ExecutionException, InterruptedException, TimeoutException {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Setup mocks for successful read
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        when(mockStorage1.read(any(FetchContext.class), eq(1L), eq(0L), eq(100L), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));

        // Execute
        FetchContext context = FetchContext.DEFAULT;
        CompletableFuture<ReadDataBlock> future = quorumStorage.read(context, 1L, 0L, 100L, 1024);
        ReadDataBlock result = future.get(5, TimeUnit.SECONDS);

        // Verify
        assertNotNull(result);
        verify(mockStorage1).read(context, 1L, 0L, 100L, 1024);
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testQuorumReadWithPrimaryFailure() throws ExecutionException, InterruptedException, TimeoutException {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Setup mocks - primary fails, secondary succeeds
        ReadDataBlock mockDataBlock = mock(ReadDataBlock.class);
        when(mockStorage1.read(any(FetchContext.class), eq(1L), eq(0L), eq(100L), eq(1024)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Primary failure")));
        when(mockStorage2.read(any(FetchContext.class), eq(1L), eq(0L), eq(100L), eq(1024)))
            .thenReturn(CompletableFuture.completedFuture(mockDataBlock));
        when(mockStorage3.read(any(FetchContext.class), eq(1L), eq(0L), eq(100L), eq(1024)))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Third replica failure")));

        // Execute
        FetchContext context = FetchContext.DEFAULT;
        CompletableFuture<ReadDataBlock> future = quorumStorage.read(context, 1L, 0L, 100L, 1024);
        
        // Wait for result with timeout
        ReadDataBlock result = future.get(5, TimeUnit.SECONDS);

        // Verify
        assertNotNull(result);
        verify(mockStorage1).read(context, 1L, 0L, 100L, 1024);
        verify(mockStorage2).read(context, 1L, 0L, 100L, 1024);
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testQuorumState() {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Get quorum state
        QuorumState quorumState = quorumStorage.getQuorumState();
        
        // Verify initial state
        assertEquals(3, quorumState.getHealthyReplicaCount());
        assertTrue(quorumState.hasQuorum());
        
        // Test marking replica as failed
        quorumState.markReplicaFailed(0);
        assertEquals(2, quorumState.getHealthyReplicaCount());
        assertTrue(quorumState.hasQuorum());
        
        // Test marking replica as recovered
        quorumState.markReplicaSuccess(0);
        assertEquals(3, quorumState.getHealthyReplicaCount());
        assertTrue(quorumState.hasQuorum());
        
        // Test multiple failures
        quorumState.markReplicaFailed(0);
        quorumState.markReplicaFailed(1);
        assertEquals(1, quorumState.getHealthyReplicaCount());
        assertFalse(quorumState.hasQuorum());
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testForceUpload() throws ExecutionException, InterruptedException, TimeoutException {
        // Start the quorum storage
        quorumStorage.startup();
        
        // Setup mocks
        when(mockStorage1.forceUpload(1L)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage2.forceUpload(1L)).thenReturn(CompletableFuture.completedFuture(null));
        when(mockStorage3.forceUpload(1L)).thenReturn(CompletableFuture.completedFuture(null));

        // Execute
        CompletableFuture<Void> future = quorumStorage.forceUpload(1L);
        future.get(5, TimeUnit.SECONDS);

        // Verify
        verify(mockStorage1).forceUpload(1L);
        verify(mockStorage2).forceUpload(1L);
        verify(mockStorage3).forceUpload(1L);
        
        // Shutdown the quorum storage
        quorumStorage.shutdown();
    }

    @Test
    void testInvalidQuorumSize() {
        // Test with invalid replica count - quorum size 3 but only 2 replicas
        List<Storage> invalidReplicas = new ArrayList<>();
        invalidReplicas.add(mockStorage1);
        invalidReplicas.add(mockStorage2);
        // Missing third replica

        // Should throw IllegalArgumentException when replica count doesn't match quorum size
        assertThrows(IllegalArgumentException.class, () -> {
            new S3QuorumStorage(quorumConfig, invalidReplicas);
        });
    }
} 