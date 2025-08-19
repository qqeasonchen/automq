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
import com.automq.stream.s3.quorum.state.QuorumState;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

/**
 * Configuration validation and edge case testing for S3QuorumStorage.
 * These tests verify proper handling of various configuration scenarios
 * and system behavior under different configuration parameters.
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("S3QuorumStorage Configuration E2E Tests")
class S3QuorumStorageConfigurationE2ETest {

    @Mock
    private Storage mockStorage1;
    
    @Mock
    private Storage mockStorage2;
    
    @Mock
    private Storage mockStorage3;
    
    @Mock
    private Storage mockStorage4;
    
    @Mock
    private Storage mockStorage5;

    @Test
    @DisplayName("Valid quorum configurations with different sizes")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testValidQuorumConfigurations() throws Exception {
        // Test 3-replica configuration (2/3 write quorum, 1/3 read quorum) - minimum allowed
        testQuorumConfiguration(3, 2, 1, true);
        
        // Test 5-replica configuration (3/5 write quorum, 2/5 read quorum)
        testQuorumConfiguration(5, 3, 2, true);
        
        // Test 3-replica with different quorum sizes
        testQuorumConfiguration(3, 3, 3, true); // All replicas required
    }

    @Test
    @DisplayName("Invalid quorum configurations should be rejected")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testInvalidQuorumConfigurations() {
        // Quorum size less than 3 (minimum requirement)
        assertThrows(IllegalArgumentException.class, () -> 
            testQuorumConfiguration(2, 1, 1, false));
        
        // Single replica (less than minimum 3)
        assertThrows(IllegalArgumentException.class, () -> 
            testQuorumConfiguration(1, 1, 1, false));
        
        // Write quorum larger than total replicas
        assertThrows(IllegalArgumentException.class, () -> 
            testQuorumConfiguration(3, 4, 1, false));
        
        // Read quorum larger than total replicas
        assertThrows(IllegalArgumentException.class, () -> 
            testQuorumConfiguration(3, 2, 4, false));
        
        // Zero quorum size
        assertThrows(IllegalArgumentException.class, () -> 
            testQuorumConfiguration(0, 0, 0, false));
        
        // Negative values
        assertThrows(IllegalArgumentException.class, () -> 
            testQuorumConfiguration(-1, 1, 1, false));
    }

    @Test
    @DisplayName("Timeout configuration validation and behavior")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testTimeoutConfigurations() throws Exception {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs(3);
        
        // Test very short timeout
        QuorumConfig shortTimeoutConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(100) // Very short
            .readTimeoutMs(50)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(false)
            .build();
        
        List<Storage> replicas = Arrays.asList(mockStorage1, mockStorage2, mockStorage3);
        S3QuorumStorage quorumStorage = new S3QuorumStorage(shortTimeoutConfig, replicas);
        
        setupSlowStorages(); // Simulate slow responses
        
        assertDoesNotThrow(() -> quorumStorage.startup());
        
        // Operations should timeout quickly
        StreamRecordBatch batch = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("timeout-test".getBytes()));
        
        CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
        
        // Should timeout due to short timeout configuration
        assertThrows(Exception.class, () -> future.get(5, TimeUnit.SECONDS));
        
        quorumStorage.shutdown();
        
        // Test reasonable timeout configuration
        QuorumConfig reasonableTimeoutConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();
        
        S3QuorumStorage reasonableQuorumStorage = new S3QuorumStorage(reasonableTimeoutConfig, replicas);
        setupFastStorages(); // Fast responses
        
        assertDoesNotThrow(() -> reasonableQuorumStorage.startup());
        assertDoesNotThrow(() -> reasonableQuorumStorage.append(AppendContext.DEFAULT, batch).get(10, TimeUnit.SECONDS));
        reasonableQuorumStorage.shutdown();
    }

    @Test
    @DisplayName("Replica configuration validation and priority handling")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testReplicaConfigurationValidation() throws Exception {
        // Test replica priority ordering
        List<ReplicaConfig> priorityConfigs = new ArrayList<>();
        
        priorityConfigs.add(ReplicaConfig.builder()
            .replicaId(0)
            .region("us-east-1")
            .bucket("high-priority-bucket")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .accessKey("test-key")
            .secretKey("test-secret")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.PRIMARY)
            .priority(100) // Highest priority
            .build());
            
        priorityConfigs.add(ReplicaConfig.builder()
            .replicaId(1)
            .region("us-west-2")
            .bucket("medium-priority-bucket")
            .endpoint("https://s3.us-west-2.amazonaws.com")
            .accessKey("test-key")
            .secretKey("test-secret")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(50) // Medium priority
            .build());
            
        priorityConfigs.add(ReplicaConfig.builder()
            .replicaId(2)
            .region("eu-west-1")
            .bucket("low-priority-bucket")
            .endpoint("https://s3.eu-west-1.amazonaws.com")
            .accessKey("test-key")
            .secretKey("test-secret")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(10) // Lowest priority
            .build());
        
        QuorumConfig priorityConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(priorityConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();
        
        List<Storage> replicas = Arrays.asList(mockStorage1, mockStorage2, mockStorage3);
        S3QuorumStorage quorumStorage = new S3QuorumStorage(priorityConfig, replicas);
        
        setupFastStorages();
        
        assertDoesNotThrow(() -> quorumStorage.startup());
        assertNotNull(quorumStorage.getQuorumState());
        assertTrue(quorumStorage.getQuorumState().hasQuorum());
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Read repair configuration validation")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testReadRepairConfiguration() throws Exception {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs(3);
        
        // Test with read repair enabled
        QuorumConfig readRepairEnabledConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();
        
        List<Storage> replicas = Arrays.asList(mockStorage1, mockStorage2, mockStorage3);
        S3QuorumStorage quorumStorageWithRepair = new S3QuorumStorage(readRepairEnabledConfig, replicas);
        
        setupFastStorages();
        assertDoesNotThrow(() -> quorumStorageWithRepair.startup());
        assertTrue(quorumStorageWithRepair.getQuorumState().hasQuorum());
        quorumStorageWithRepair.shutdown();
        
        // Test with read repair disabled
        QuorumConfig readRepairDisabledConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(false)
            .readRepairTimeoutMs(0)
            .build();
        
        S3QuorumStorage quorumStorageWithoutRepair = new S3QuorumStorage(readRepairDisabledConfig, replicas);
        
        assertDoesNotThrow(() -> quorumStorageWithoutRepair.startup());
        assertTrue(quorumStorageWithoutRepair.getQuorumState().hasQuorum());
        quorumStorageWithoutRepair.shutdown();
    }

    @Test
    @DisplayName("Minimum viable quorum configuration (3 replicas)")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testMinimumViableConfiguration() throws Exception {
        // Minimum 3-replica configuration as required by validation
        List<ReplicaConfig> minReplicaConfig = createTestReplicaConfigs(3);
        
        QuorumConfig minConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2) // Minimum viable write quorum
            .readQuorumSize(1)  // Minimum viable read quorum
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(minReplicaConfig)
            .enableReadRepair(false)
            .readRepairTimeoutMs(0)
            .build();
        
        List<Storage> minReplicas = Arrays.asList(mockStorage1, mockStorage2, mockStorage3);
        S3QuorumStorage quorumStorage = new S3QuorumStorage(minConfig, minReplicas);
        
        setupFastStorages();
        
        assertDoesNotThrow(() -> quorumStorage.startup());
        
        QuorumState state = quorumStorage.getQuorumState();
        assertNotNull(state);
        assertTrue(state.hasQuorum());
        assertEquals(3, state.getHealthyReplicaCount());
        
        // Test basic operation
        StreamRecordBatch batch = new StreamRecordBatch(1L, 1L, 0L, 1, 
            Unpooled.wrappedBuffer("min-config-test".getBytes()));
        
        CompletableFuture<Void> future = quorumStorage.append(AppendContext.DEFAULT, batch);
        assertDoesNotThrow(() -> future.get(10, TimeUnit.SECONDS));
        
        quorumStorage.shutdown();
    }

    @Test
    @DisplayName("Configuration consistency validation")
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void testConfigurationConsistency() {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs(3);
        
        // Test inconsistent replica count vs configuration
        assertThrows(IllegalArgumentException.class, () -> {
            QuorumConfig.builder()
                .quorumSize(5) // Expecting 5 replicas
                .writeQuorumSize(3)
                .readQuorumSize(2)
                .writeTimeoutMs(30000)
                .readTimeoutMs(10000)
                .replicaConfigs(replicaConfigs) // But only providing 3
                .enableReadRepair(true)
                .readRepairTimeoutMs(5000)
                .build();
        });
        
        // Test with too few replicas for the specified quorum size
        List<ReplicaConfig> twoReplicaConfigs = createTestReplicaConfigs(2);
        assertThrows(IllegalArgumentException.class, () -> {
            QuorumConfig.builder()
                .quorumSize(3) // Expecting 3 replicas
                .writeQuorumSize(2)
                .readQuorumSize(1)
                .writeTimeoutMs(30000)
                .readTimeoutMs(10000)
                .replicaConfigs(twoReplicaConfigs) // But only providing 2
                .enableReadRepair(true)
                .readRepairTimeoutMs(5000)
                .build();
        });
    }

    @Test
    @DisplayName("Dynamic configuration behavior validation")
    @Timeout(value = 60, unit = TimeUnit.SECONDS)
    void testDynamicConfigurationBehavior() throws Exception {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs(3);
        
        QuorumConfig config = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();
        
        List<Storage> replicas = Arrays.asList(mockStorage1, mockStorage2, mockStorage3);
        S3QuorumStorage quorumStorage = new S3QuorumStorage(config, replicas);
        
        setupFastStorages();
        quorumStorage.startup();
        
        QuorumState state = quorumStorage.getQuorumState();
        
        // Test state changes under different replica health conditions
        assertEquals(3, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum());
        
        // Simulate replica failure
        state.markReplicaFailed(2);
        assertEquals(2, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum()); // Should still have quorum with 2/3
        
        // Simulate another failure
        state.markReplicaFailed(1);
        assertEquals(1, state.getHealthyReplicaCount());
        assertFalse(state.hasQuorum()); // Should lose quorum with 1/3
        
        // Recovery
        state.markReplicaSuccess(1);
        assertEquals(2, state.getHealthyReplicaCount());
        assertTrue(state.hasQuorum()); // Should regain quorum
        
        quorumStorage.shutdown();
    }

    // Helper methods
    private void testQuorumConfiguration(int quorumSize, int writeQuorum, int readQuorum, boolean shouldSucceed) throws Exception {
        List<ReplicaConfig> replicaConfigs = createTestReplicaConfigs(quorumSize);
        
        if (shouldSucceed) {
            QuorumConfig config = QuorumConfig.builder()
                .quorumSize(quorumSize)
                .writeQuorumSize(writeQuorum)
                .readQuorumSize(readQuorum)
                .writeTimeoutMs(30000)
                .readTimeoutMs(10000)
                .replicaConfigs(replicaConfigs)
                .enableReadRepair(true)
                .readRepairTimeoutMs(5000)
                .build();
            
            List<Storage> replicas = createMockReplicas(quorumSize);
            S3QuorumStorage quorumStorage = new S3QuorumStorage(config, replicas);
            
            setupMultipleFastStorages(quorumSize);
            
            assertDoesNotThrow(() -> quorumStorage.startup());
            assertNotNull(quorumStorage.getQuorumState());
            assertEquals(quorumSize, quorumStorage.getQuorumState().getHealthyReplicaCount());
            assertTrue(quorumStorage.getQuorumState().hasQuorum());
            
            quorumStorage.shutdown();
        } else {
            assertThrows(IllegalArgumentException.class, () -> {
                QuorumConfig.builder()
                    .quorumSize(quorumSize)
                    .writeQuorumSize(writeQuorum)
                    .readQuorumSize(readQuorum)
                    .writeTimeoutMs(30000)
                    .readTimeoutMs(10000)
                    .replicaConfigs(replicaConfigs)
                    .enableReadRepair(true)
                    .readRepairTimeoutMs(5000)
                    .build();
            });
        }
    }
    
    private List<ReplicaConfig> createTestReplicaConfigs(int count) {
        List<ReplicaConfig> configs = new ArrayList<>();
        String[] regions = {"us-east-1", "us-west-2", "eu-west-1", "ap-south-1", "ap-northeast-1"};
        
        for (int i = 0; i < count; i++) {
            String region = regions[i % regions.length];
            configs.add(createReplicaConfig(i, region, "test-bucket-" + (i + 1)));
        }
        
        return configs;
    }
    
    private ReplicaConfig createReplicaConfig(int id, String region, String bucket) {
        return ReplicaConfig.builder()
            .replicaId(id)
            .region(region)
            .bucket(bucket)
            .endpoint("https://s3." + region + ".amazonaws.com")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(new com.automq.stream.s3.Config())
            .role(id == 0 ? ReplicaConfig.ReplicaRole.PRIMARY : ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(100 - (id * 10))
            .build();
    }
    
    private List<Storage> createMockReplicas(int count) {
        List<Storage> replicas = new ArrayList<>();
        Storage[] mocks = {mockStorage1, mockStorage2, mockStorage3, mockStorage4, mockStorage5};
        
        for (int i = 0; i < count; i++) {
            replicas.add(mocks[i]);
        }
        
        return replicas;
    }
    
    private void setupFastStorages() {
        Storage[] storages = {mockStorage1, mockStorage2, mockStorage3};
        for (Storage storage : storages) {
            doNothing().when(storage).startup();
            doNothing().when(storage).shutdown();
            when(storage.append(any(AppendContext.class), any(StreamRecordBatch.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        }
    }
    
    private void setupMultipleFastStorages(int count) {
        Storage[] storages = {mockStorage1, mockStorage2, mockStorage3, mockStorage4, mockStorage5};
        for (int i = 0; i < count; i++) {
            doNothing().when(storages[i]).startup();
            doNothing().when(storages[i]).shutdown();
            when(storages[i].append(any(AppendContext.class), any(StreamRecordBatch.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
        }
    }
    
    private void setupSlowStorages() {
        Storage[] storages = {mockStorage1, mockStorage2, mockStorage3};
        for (Storage storage : storages) {
            doNothing().when(storage).startup();
            doNothing().when(storage).shutdown();
            when(storage.append(any(AppendContext.class), any(StreamRecordBatch.class)))
                .thenReturn(createSlowFuture(5000)); // 5 second delay
        }
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
}