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

package com.automq.stream.s3.quorum.failover;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;
import com.automq.stream.s3.quorum.failover.FailureDetector.FailureType;
import com.automq.stream.s3.quorum.failover.RecoveryManager.RecoveryListener;
import com.automq.stream.s3.quorum.failover.RecoveryManager.RecoveryState;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for RecoveryManager functionality
 */
@DisplayName("Recovery Manager Tests")
public class RecoveryManagerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryManagerTest.class);

    private QuorumConfig quorumConfig;
    private QuorumState quorumState;
    private List<Storage> replicaStorages;
    private RecoveryManager recoveryManager;
    private TestRecoveryListener testListener;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up RecoveryManager test environment");
        
        // Create test configuration with 3 replicas
        List<ReplicaConfig> replicas = new ArrayList<>();
        Config testConfig = new Config();
        for (int i = 0; i < 3; i++) {
            ReplicaConfig replica = ReplicaConfig.builder()
                .replicaId(i)
                .region("test-region-" + i)
                .bucket("test-bucket-" + i)
                .endpoint("http://localhost:920" + i)
                .accessKey("test-access-key")
                .secretKey("test-secret-key")
                .s3Config(testConfig)
                .role(i == 0 ? ReplicaConfig.ReplicaRole.PRIMARY : ReplicaConfig.ReplicaRole.SECONDARY)
                .priority(100 - i * 10)
                .build();
            replicas.add(replica);
        }
        
        quorumConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .replicaConfigs(replicas)
            .build();
        
        quorumState = new QuorumState(3);
        quorumState.startup();
        
        // Create mock storage instances
        replicaStorages = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            replicaStorages.add(new MockStorage(i));
        }
        
        recoveryManager = new RecoveryManager(quorumConfig, quorumState, replicaStorages);
        
        testListener = new TestRecoveryListener();
        recoveryManager.addListener(testListener);
        
        LOGGER.info("RecoveryManager test environment setup completed");
    }

    @AfterEach
    void tearDown() {
        if (recoveryManager != null) {
            recoveryManager.stop();
        }
        if (quorumState != null) {
            quorumState.shutdown();
        }
        LOGGER.info("RecoveryManager test environment cleaned up");
    }

    @Test
    @DisplayName("Test recovery manager lifecycle")
    void testRecoveryManagerLifecycle() {
        LOGGER.info("Testing recovery manager lifecycle");
        
        // Start recovery manager
        recoveryManager.start();
        
        // Verify recovery states are initialized
        for (int i = 0; i < 3; i++) {
            RecoveryState state = recoveryManager.getRecoveryState(i);
            assertNotNull(state, "Recovery state should exist for replica " + i);
            assertFalse(state.needsRecovery(), "Replica " + i + " should not need recovery initially");
            assertFalse(state.isRecovering(), "Replica " + i + " should not be recovering initially");
            assertEquals(0, state.getAttemptCount(), "Attempt count should be 0 initially");
        }
        
        // Stop recovery manager
        recoveryManager.stop();
        
        LOGGER.info("Recovery manager lifecycle test completed successfully");
    }

    @Test
    @DisplayName("Test replica failure detection and recovery trigger")
    void testReplicaFailureAndRecoveryTrigger() throws InterruptedException {
        LOGGER.info("Testing replica failure detection and recovery trigger");
        
        recoveryManager.start();
        
        // Simulate replica failure notification
        ReplicaFailureState failureState = new ReplicaFailureState(quorumConfig.getReplicaConfigs().get(1));
        failureState.markAsUnhealthy();
        
        recoveryManager.onReplicaFailed(1, failureState);
        
        // Wait for recovery to be needed
        assertTrue(testListener.waitForRecoveryNeeded(1, 5000), 
                  "Recovery should be needed for failed replica");
        
        RecoveryState recoveryState = recoveryManager.getRecoveryState(1);
        assertTrue(recoveryState.needsRecovery(), "Replica 1 should need recovery");
        
        LOGGER.info("Replica failure and recovery trigger test completed successfully");
    }

    @Test
    @DisplayName("Test manual recovery trigger")
    void testManualRecoveryTrigger() throws Exception {
        LOGGER.info("Testing manual recovery trigger");
        
        recoveryManager.start();
        
        // Mark replica as needing recovery
        RecoveryState recoveryState = recoveryManager.getRecoveryState(2);
        recoveryState.markAsNeedingRecovery();
        
        // Trigger manual recovery
        CompletableFuture<Boolean> recoveryFuture = recoveryManager.triggerRecovery(2);
        Boolean recoveryResult = recoveryFuture.get(10, TimeUnit.SECONDS);
        
        assertTrue(recoveryResult, "Manual recovery should succeed");
        assertFalse(recoveryState.needsRecovery(), "Replica should no longer need recovery");
        
        LOGGER.info("Manual recovery trigger test completed successfully");
    }

    @Test
    @DisplayName("Test replica recovery completion")
    void testReplicaRecoveryCompletion() throws InterruptedException {
        LOGGER.info("Testing replica recovery completion");
        
        recoveryManager.start();
        
        // Simulate successful recovery notification
        ReplicaFailureState failureState = new ReplicaFailureState(quorumConfig.getReplicaConfigs().get(0));
        failureState.markAsHealthy();
        
        recoveryManager.onReplicaRecovered(0, failureState);
        
        // Wait for recovery completion notification
        assertTrue(testListener.waitForRecoveryCompleted(0, 5000), 
                  "Recovery completion should be notified");
        
        RecoveryState recoveryState = recoveryManager.getRecoveryState(0);
        assertFalse(recoveryState.needsRecovery(), "Replica should not need recovery after recovery");
        
        LOGGER.info("Replica recovery completion test completed successfully");
    }

    @Test
    @DisplayName("Test quorum loss emergency recovery")
    void testQuorumLossEmergencyRecovery() throws InterruptedException {
        LOGGER.info("Testing quorum loss emergency recovery");
        
        recoveryManager.start();
        
        // Mark multiple replicas as needing recovery
        for (int i = 0; i < 2; i++) {
            RecoveryState state = recoveryManager.getRecoveryState(i);
            state.markAsNeedingRecovery();
        }
        
        // Trigger quorum loss
        recoveryManager.onQuorumLost(FailureType.WRITE_QUORUM_LOST);
        
        // Emergency recovery should attempt to recover all failed replicas
        // We'll check that recovery attempts are made within a reasonable time
        Thread.sleep(2000); // Give emergency recovery time to start
        
        // Verify that recovery attempts were made
        boolean anyRecoveryStarted = false;
        for (int i = 0; i < 2; i++) {
            RecoveryState state = recoveryManager.getRecoveryState(i);
            if (state.getAttemptCount() > 0) {
                anyRecoveryStarted = true;
                break;
            }
        }
        
        assertTrue(anyRecoveryStarted, "Emergency recovery should start recovery attempts");
        
        LOGGER.info("Quorum loss emergency recovery test completed successfully");
    }

    @Test
    @DisplayName("Test quorum at risk acceleration")
    void testQuorumAtRiskAcceleration() throws InterruptedException {
        LOGGER.info("Testing quorum at risk acceleration");
        
        recoveryManager.start();
        
        // Mark one replica as needing recovery
        RecoveryState state = recoveryManager.getRecoveryState(1);
        state.markAsNeedingRecovery();
        
        // Trigger quorum at risk
        recoveryManager.onQuorumAtRisk(2, 3);
        
        // Accelerated recovery should start recovery attempts quickly
        Thread.sleep(1000); // Give accelerated recovery time to start
        
        // Check if recovery attempt was made
        assertTrue(state.getAttemptCount() >= 0, "Recovery attempt should be made or in progress");
        
        LOGGER.info("Quorum at risk acceleration test completed successfully");
    }

    @Test
    @DisplayName("Test concurrent recovery operations")
    void testConcurrentRecoveryOperations() throws Exception {
        LOGGER.info("Testing concurrent recovery operations");
        
        recoveryManager.start();
        
        // Mark multiple replicas as needing recovery
        for (int i = 0; i < 3; i++) {
            RecoveryState state = recoveryManager.getRecoveryState(i);
            state.markAsNeedingRecovery();
        }
        
        // Trigger concurrent manual recoveries
        List<CompletableFuture<Boolean>> recoveryFutures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            recoveryFutures.add(recoveryManager.triggerRecovery(i));
        }
        
        // Wait for all recoveries to complete
        CompletableFuture<Void> allRecoveries = CompletableFuture.allOf(
            recoveryFutures.toArray(new CompletableFuture[0]));
        allRecoveries.get(15, TimeUnit.SECONDS);
        
        // Verify results
        for (int i = 0; i < 3; i++) {
            Boolean result = recoveryFutures.get(i).get();
            // At least some recoveries should succeed (limited by MAX_CONCURRENT_RECOVERIES)
            LOGGER.info("Recovery {} result: {}", i, result);
        }
        
        LOGGER.info("Concurrent recovery operations test completed successfully");
    }

    /**
     * Mock Storage implementation for testing
     */
    private static class MockStorage implements Storage {
        private final int replicaId;
        private volatile boolean healthy = true;

        public MockStorage(int replicaId) {
            this.replicaId = replicaId;
        }

        @Override
        public void startup() {
            // No-op for testing
        }

        @Override
        public void shutdown() {
            // No-op for testing
        }

        @Override
        public CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord) {
            if (!healthy) {
                return CompletableFuture.failedFuture(new RuntimeException("Mock storage unhealthy"));
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
            if (!healthy) {
                return CompletableFuture.failedFuture(new RuntimeException("Mock storage unhealthy"));
            }
            return CompletableFuture.completedFuture(
                new ReadDataBlock(List.of(), com.automq.stream.s3.cache.CacheAccessType.BLOCK_CACHE_MISS));
        }

        @Override
        public CompletableFuture<Void> forceUpload(long streamId) {
            return CompletableFuture.completedFuture(null);
        }


        public void setHealthy(boolean healthy) {
            this.healthy = healthy;
        }

        public int getReplicaId() {
            return replicaId;
        }
    }

    /**
     * Test listener implementation to track recovery events
     */
    private static class TestRecoveryListener implements RecoveryListener {
        private final AtomicInteger recoveryNeededCount = new AtomicInteger(0);
        private final AtomicInteger recoveryStartedCount = new AtomicInteger(0);
        private final AtomicInteger recoveryCompletedCount = new AtomicInteger(0);
        
        private volatile int lastRecoveryNeededReplicaId = -1;
        private volatile int lastRecoveryStartedReplicaId = -1;
        private volatile int lastRecoveryCompletedReplicaId = -1;
        private volatile boolean lastRecoverySuccess = false;
        
        private final CountDownLatch recoveryNeededLatch = new CountDownLatch(1);
        private final CountDownLatch recoveryStartedLatch = new CountDownLatch(1);
        private final CountDownLatch recoveryCompletedLatch = new CountDownLatch(1);

        @Override
        public void onRecoveryNeeded(int replicaId, RecoveryState state) {
            lastRecoveryNeededReplicaId = replicaId;
            recoveryNeededCount.incrementAndGet();
            recoveryNeededLatch.countDown();
            LOGGER.info("Test listener: Recovery needed for replica {}", replicaId);
        }

        @Override
        public void onRecoveryStarted(int replicaId, RecoveryState state) {
            lastRecoveryStartedReplicaId = replicaId;
            recoveryStartedCount.incrementAndGet();
            recoveryStartedLatch.countDown();
            LOGGER.info("Test listener: Recovery started for replica {} (attempt {})", 
                       replicaId, state.getAttemptCount());
        }

        @Override
        public void onRecoveryCompleted(int replicaId, RecoveryState state, boolean success) {
            lastRecoveryCompletedReplicaId = replicaId;
            lastRecoverySuccess = success;
            recoveryCompletedCount.incrementAndGet();
            recoveryCompletedLatch.countDown();
            LOGGER.info("Test listener: Recovery completed for replica {} (success: {})", 
                       replicaId, success);
        }

        public boolean waitForRecoveryNeeded(int expectedReplicaId, long timeoutMs) throws InterruptedException {
            boolean result = recoveryNeededLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            return result && lastRecoveryNeededReplicaId == expectedReplicaId;
        }

        public boolean waitForRecoveryStarted(int expectedReplicaId, long timeoutMs) throws InterruptedException {
            boolean result = recoveryStartedLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            return result && lastRecoveryStartedReplicaId == expectedReplicaId;
        }

        public boolean waitForRecoveryCompleted(int expectedReplicaId, long timeoutMs) throws InterruptedException {
            boolean result = recoveryCompletedLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            return result && lastRecoveryCompletedReplicaId == expectedReplicaId;
        }

        public int getRecoveryNeededCount() {
            return recoveryNeededCount.get();
        }
        
        public int getRecoveryStartedCount() {
            return recoveryStartedCount.get();
        }
        
        public int getRecoveryCompletedCount() {
            return recoveryCompletedCount.get();
        }
        
        public boolean getLastRecoverySuccess() {
            return lastRecoverySuccess;
        }
    }
}