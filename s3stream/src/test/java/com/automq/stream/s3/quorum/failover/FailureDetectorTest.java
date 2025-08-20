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
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.state.QuorumState;
import com.automq.stream.s3.quorum.failover.FailureDetector.FailureDetectorListener;
import com.automq.stream.s3.quorum.failover.FailureDetector.FailureType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for FailureDetector functionality
 */
@DisplayName("Failure Detector Tests")
public class FailureDetectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailureDetectorTest.class);

    private QuorumConfig quorumConfig;
    private QuorumState quorumState;
    private FailureDetector failureDetector;
    private TestFailureDetectorListener testListener;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up FailureDetector test environment");
        
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
        
        // Create failure detector with shorter intervals for testing
        failureDetector = new FailureDetector(
            quorumConfig, 
            quorumState, 
            1000, // 1 second heartbeat
            3000, // 3 second timeout
            2,    // 2 failures threshold
            5.0   // PHI threshold
        );
        
        testListener = new TestFailureDetectorListener();
        failureDetector.addListener(testListener);
        
        LOGGER.info("FailureDetector test environment setup completed");
    }

    @AfterEach
    void tearDown() {
        if (failureDetector != null) {
            failureDetector.stop();
        }
        if (quorumState != null) {
            quorumState.shutdown();
        }
        LOGGER.info("FailureDetector test environment cleaned up");
    }

    @Test
    @DisplayName("Test failure detector startup and shutdown")
    void testFailureDetectorLifecycle() {
        LOGGER.info("Testing failure detector lifecycle");
        
        // Start failure detector
        failureDetector.start();
        
        // Verify all replicas start as healthy
        for (int i = 0; i < 3; i++) {
            ReplicaFailureState state = failureDetector.getReplicaState(i);
            assertNotNull(state, "Replica state should exist for replica " + i);
            assertTrue(state.isHealthy(), "Replica " + i + " should start as healthy");
        }
        
        // Stop failure detector
        failureDetector.stop();
        
        LOGGER.info("Failure detector lifecycle test completed successfully");
    }

    @Test
    @DisplayName("Test replica failure detection")
    void testReplicaFailureDetection() throws InterruptedException {
        LOGGER.info("Testing replica failure detection");
        
        failureDetector.start();
        
        // Record multiple failures for replica 1
        Exception testException = new RuntimeException("Test failure");
        failureDetector.recordFailure(1, testException);
        failureDetector.recordFailure(1, testException);
        
        // Wait for failure to be detected
        assertTrue(testListener.waitForReplicaFailed(1, 5000), 
                  "Replica 1 should be marked as failed");
        
        // Verify replica state
        ReplicaFailureState state = failureDetector.getReplicaState(1);
        assertFalse(state.isHealthy(), "Replica 1 should be marked as unhealthy");
        assertEquals(2, state.getFailureCount(), "Replica 1 should have 2 failures recorded");
        
        LOGGER.info("Replica failure detection test completed successfully");
    }

    @Test
    @DisplayName("Test replica recovery detection")
    void testReplicaRecoveryDetection() throws InterruptedException {
        LOGGER.info("Testing replica recovery detection");
        
        failureDetector.start();
        
        // First, cause a failure
        Exception testException = new RuntimeException("Test failure");
        failureDetector.recordFailure(2, testException);
        failureDetector.recordFailure(2, testException);
        
        assertTrue(testListener.waitForReplicaFailed(2, 5000), 
                  "Replica 2 should be marked as failed");
        
        // Then record successive successes to trigger recovery
        failureDetector.recordSuccess(2);
        failureDetector.recordSuccess(2);
        failureDetector.recordSuccess(2);
        
        assertTrue(testListener.waitForReplicaRecovered(2, 5000), 
                  "Replica 2 should be marked as recovered");
        
        // Verify replica state
        ReplicaFailureState state = failureDetector.getReplicaState(2);
        assertTrue(state.isHealthy(), "Replica 2 should be marked as healthy again");
        assertEquals(0, state.getFailureCount(), "Replica 2 failure count should be reset");
        
        LOGGER.info("Replica recovery detection test completed successfully");
    }

    @Test
    @DisplayName("Test quorum loss detection")
    void testQuorumLossDetection() throws InterruptedException {
        LOGGER.info("Testing quorum loss detection");
        
        failureDetector.start();
        
        // Fail enough replicas to cause write quorum loss (need to fail 2 out of 3)
        Exception testException = new RuntimeException("Test quorum loss failure");
        
        // Fail replica 0
        failureDetector.recordFailure(0, testException);
        failureDetector.recordFailure(0, testException);
        
        assertTrue(testListener.waitForReplicaFailed(0, 5000), 
                  "Replica 0 should be marked as failed");
        
        // Fail replica 1 - this should trigger quorum loss
        failureDetector.recordFailure(1, testException);
        failureDetector.recordFailure(1, testException);
        
        assertTrue(testListener.waitForReplicaFailed(1, 5000), 
                  "Replica 1 should be marked as failed");
        
        assertTrue(testListener.waitForQuorumLost(5000), 
                  "Quorum loss should be detected");
        
        LOGGER.info("Quorum loss detection test completed successfully");
    }

    @Test
    @DisplayName("Test PHI accrual failure detection")
    void testPhiAccrualFailureDetection() throws InterruptedException {
        LOGGER.info("Testing PHI accrual failure detection");
        
        failureDetector.start();
        
        // Record regular heartbeats for replica 0
        for (int i = 0; i < 10; i++) {
            failureDetector.recordSuccess(0);
            Thread.sleep(100); // Regular 100ms intervals
        }
        
        ReplicaFailureState state = failureDetector.getReplicaState(0);
        double initialPhi = state.calculatePhi(System.currentTimeMillis());
        LOGGER.info("Initial PHI value: {}", initialPhi);
        
        // Wait longer than expected interval to trigger PHI threshold
        Thread.sleep(5000); // Much longer than 100ms interval
        
        double laterPhi = state.calculatePhi(System.currentTimeMillis());
        LOGGER.info("PHI value after delay: {}", laterPhi);
        
        assertTrue(laterPhi > initialPhi, 
                  "PHI value should increase with longer intervals");
        
        LOGGER.info("PHI accrual failure detection test completed successfully");
    }

    @Test
    @DisplayName("Test concurrent failure recording")
    void testConcurrentFailureRecording() throws InterruptedException {
        LOGGER.info("Testing concurrent failure recording");
        
        failureDetector.start();
        
        int numThreads = 5;
        int failuresPerThread = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        
        // Create multiple threads recording failures concurrently
        for (int i = 0; i < numThreads; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < failuresPerThread; j++) {
                        failureDetector.recordFailure(0, new RuntimeException("Concurrent test failure"));
                        Thread.sleep(10); // Small delay between failures
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to complete
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), 
                  "All concurrent failure recording threads should complete");
        
        // Verify failure count
        ReplicaFailureState state = failureDetector.getReplicaState(0);
        assertEquals(numThreads * failuresPerThread, state.getFailureCount(), 
                    "All failures should be recorded correctly");
        
        LOGGER.info("Concurrent failure recording test completed successfully");
    }

    /**
     * Test listener implementation to track failure detector events
     */
    private static class TestFailureDetectorListener implements FailureDetectorListener {
        private final AtomicInteger replicaFailedCount = new AtomicInteger(0);
        private final AtomicInteger replicaRecoveredCount = new AtomicInteger(0);
        private final AtomicInteger quorumLostCount = new AtomicInteger(0);
        private final AtomicInteger quorumAtRiskCount = new AtomicInteger(0);
        
        private volatile int lastFailedReplicaId = -1;
        private volatile int lastRecoveredReplicaId = -1;
        private volatile FailureType lastFailureType = null;
        
        private final CountDownLatch replicaFailedLatch = new CountDownLatch(1);
        private final CountDownLatch replicaRecoveredLatch = new CountDownLatch(1);
        private final CountDownLatch quorumLostLatch = new CountDownLatch(1);

        @Override
        public void onReplicaFailed(int replicaId, ReplicaFailureState state) {
            lastFailedReplicaId = replicaId;
            replicaFailedCount.incrementAndGet();
            replicaFailedLatch.countDown();
            LOGGER.info("Test listener: Replica {} failed (failures: {})", 
                       replicaId, state.getFailureCount());
        }

        @Override
        public void onReplicaRecovered(int replicaId, ReplicaFailureState state) {
            lastRecoveredReplicaId = replicaId;
            replicaRecoveredCount.incrementAndGet();
            replicaRecoveredLatch.countDown();
            LOGGER.info("Test listener: Replica {} recovered (successes: {})", 
                       replicaId, state.getConsecutiveSuccesses());
        }

        @Override
        public void onQuorumLost(FailureType type) {
            lastFailureType = type;
            quorumLostCount.incrementAndGet();
            quorumLostLatch.countDown();
            LOGGER.info("Test listener: Quorum lost (type: {})", type);
        }

        @Override
        public void onQuorumAtRisk(int healthyReplicas, int totalReplicas) {
            quorumAtRiskCount.incrementAndGet();
            LOGGER.info("Test listener: Quorum at risk ({}/{})", healthyReplicas, totalReplicas);
        }

        public boolean waitForReplicaFailed(int expectedReplicaId, long timeoutMs) throws InterruptedException {
            boolean result = replicaFailedLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            return result && lastFailedReplicaId == expectedReplicaId;
        }

        public boolean waitForReplicaRecovered(int expectedReplicaId, long timeoutMs) throws InterruptedException {
            boolean result = replicaRecoveredLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
            return result && lastRecoveredReplicaId == expectedReplicaId;
        }

        public boolean waitForQuorumLost(long timeoutMs) throws InterruptedException {
            return quorumLostLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }

        public int getReplicaFailedCount() {
            return replicaFailedCount.get();
        }
        
        public int getReplicaRecoveredCount() {
            return replicaRecoveredCount.get();
        }
        
        public int getQuorumLostCount() {
            return quorumLostCount.get();
        }
        
        public int getQuorumAtRiskCount() {
            return quorumAtRiskCount.get();
        }
        
        public FailureType getLastFailureType() {
            return lastFailureType;
        }
    }
}