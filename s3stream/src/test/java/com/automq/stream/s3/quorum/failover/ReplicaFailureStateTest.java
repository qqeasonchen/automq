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

import com.automq.stream.s3.quorum.config.ReplicaConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for ReplicaFailureState functionality
 */
@DisplayName("Replica Failure State Tests")
public class ReplicaFailureStateTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaFailureStateTest.class);

    private ReplicaConfig replicaConfig;
    private ReplicaFailureState failureState;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up ReplicaFailureState test environment");
        
        replicaConfig = ReplicaConfig.builder()
            .replicaId(1)
            .region("test-region")
            .bucket("test-bucket")
            .endpoint("http://localhost:9200")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(new com.automq.stream.s3.Config())
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(75)
            .build();
        
        failureState = new ReplicaFailureState(replicaConfig);
        
        LOGGER.info("ReplicaFailureState test environment setup completed");
    }

    @Test
    @DisplayName("Test initial state")
    void testInitialState() {
        LOGGER.info("Testing initial replica failure state");
        
        assertTrue(failureState.isHealthy(), "Replica should start as healthy");
        assertEquals(0, failureState.getFailureCount(), "Initial failure count should be 0");
        assertEquals(0, failureState.getConsecutiveFailures(), "Initial consecutive failures should be 0");
        assertEquals(0, failureState.getConsecutiveSuccesses(), "Initial consecutive successes should be 0");
        assertTrue(failureState.getLastSuccessTime() > 0, "Last success time should be initialized");
        assertEquals(0, failureState.getLastFailureTime(), "Last failure time should be 0 initially");
        
        LOGGER.info("Initial state test completed successfully");
    }

    @Test
    @DisplayName("Test success recording")
    void testSuccessRecording() {
        LOGGER.info("Testing success recording");
        
        long initialSuccessTime = failureState.getLastSuccessTime();
        int initialSuccesses = failureState.getConsecutiveSuccesses();
        
        // Record a success
        failureState.recordSuccess();
        
        assertTrue(failureState.getLastSuccessTime() >= initialSuccessTime, 
                  "Last success time should be updated");
        assertEquals(initialSuccesses + 1, failureState.getConsecutiveSuccesses(), 
                    "Consecutive successes should increment");
        assertEquals(0, failureState.getConsecutiveFailures(), 
                    "Consecutive failures should be reset to 0");
        
        LOGGER.info("Success recording test completed successfully");
    }

    @Test
    @DisplayName("Test failure recording")
    void testFailureRecording() {
        LOGGER.info("Testing failure recording");
        
        Exception testException = new RuntimeException("Test failure");
        
        // Record a failure
        failureState.recordFailure(testException);
        
        assertEquals(1, failureState.getFailureCount(), "Failure count should increment");
        assertEquals(1, failureState.getConsecutiveFailures(), "Consecutive failures should increment");
        assertEquals(0, failureState.getConsecutiveSuccesses(), "Consecutive successes should be reset");
        assertTrue(failureState.getLastFailureTime() > 0, "Last failure time should be updated");
        assertEquals(testException, failureState.getLastException(), "Last exception should be stored");
        assertEquals("Test failure", failureState.getLastFailureReason(), "Failure reason should be stored");
        
        LOGGER.info("Failure recording test completed successfully");
    }

    @Test
    @DisplayName("Test failure threshold detection")
    void testFailureThresholdDetection() {
        LOGGER.info("Testing failure threshold detection");
        
        Exception testException = new RuntimeException("Test failure for threshold");
        
        // Record failures below threshold
        failureState.recordFailure(testException);
        failureState.recordFailure(testException);
        
        assertFalse(failureState.shouldMarkAsFailed(), 
                   "Should not mark as failed before reaching threshold");
        
        // Record one more failure to reach threshold (default is 3)
        failureState.recordFailure(testException);
        
        assertTrue(failureState.shouldMarkAsFailed(), 
                  "Should mark as failed after reaching threshold");
        
        LOGGER.info("Failure threshold detection test completed successfully");
    }

    @Test
    @DisplayName("Test recovery threshold detection")
    void testRecoveryThresholdDetection() {
        LOGGER.info("Testing recovery threshold detection");
        
        // First mark as failed
        failureState.markAsUnhealthy();
        
        // Record successes below recovery threshold
        failureState.recordSuccess();
        failureState.recordSuccess();
        
        assertFalse(failureState.shouldRecover(), 
                   "Should not recover before reaching threshold");
        
        // Record one more success to reach recovery threshold (default is 3)
        failureState.recordSuccess();
        
        assertTrue(failureState.shouldRecover(), 
                  "Should recover after reaching threshold");
        
        LOGGER.info("Recovery threshold detection test completed successfully");
    }

    @Test
    @DisplayName("Test PHI calculation")
    void testPhiCalculation() throws InterruptedException {
        LOGGER.info("Testing PHI calculation");
        
        // Record some regular heartbeats to establish baseline
        for (int i = 0; i < 10; i++) {
            failureState.recordSuccess();
            Thread.sleep(100); // 100ms intervals
        }
        
        // Calculate PHI at expected interval
        long currentTime = System.currentTimeMillis();
        double phi1 = failureState.calculatePhi(currentTime);
        
        // Calculate PHI after a delay
        double phi2 = failureState.calculatePhi(currentTime + 1000); // 1 second later
        
        assertTrue(phi2 > phi1, "PHI should increase with longer intervals");
        assertTrue(phi1 >= 0, "PHI should be non-negative");
        
        LOGGER.info("PHI at expected time: {}, PHI after delay: {}", phi1, phi2);
        LOGGER.info("PHI calculation test completed successfully");
    }

    @Test
    @DisplayName("Test health state transitions")
    void testHealthStateTransitions() {
        LOGGER.info("Testing health state transitions");
        
        // Start healthy
        assertTrue(failureState.isHealthy(), "Should start as healthy");
        
        // Mark as unhealthy
        failureState.markAsUnhealthy();
        assertFalse(failureState.isHealthy(), "Should be marked as unhealthy");
        
        // Mark as healthy again
        failureState.markAsHealthy();
        assertTrue(failureState.isHealthy(), "Should be marked as healthy again");
        assertEquals(0, failureState.getConsecutiveFailures(), 
                    "Consecutive failures should be reset on recovery");
        assertEquals(0, failureState.getFailureCount(), 
                    "Failure count should be reset on recovery");
        
        LOGGER.info("Health state transitions test completed successfully");
    }

    @Test
    @DisplayName("Test statistics update")
    void testStatisticsUpdate() throws InterruptedException {
        LOGGER.info("Testing statistics update");
        
        // Record heartbeats with consistent intervals
        for (int i = 0; i < 15; i++) {
            failureState.recordSuccess();
            Thread.sleep(50); // 50ms intervals
        }
        
        // Update statistics
        failureState.updatePhiStatistics();
        
        double meanInterval = failureState.getMeanInterval();
        double variance = failureState.getVariance();
        
        assertTrue(meanInterval > 0, "Mean interval should be positive");
        assertTrue(variance > 0, "Variance should be positive");
        assertTrue(failureState.getHistorySize() > 0, "History should contain intervals");
        
        LOGGER.info("Mean interval: {}ms, Variance: {}, History size: {}", 
                   meanInterval, variance, failureState.getHistorySize());
        LOGGER.info("Statistics update test completed successfully");
    }

    @Test
    @DisplayName("Test failure state snapshot")
    void testFailureStateSnapshot() {
        LOGGER.info("Testing failure state snapshot");
        
        // Record some failures and successes
        Exception testException = new RuntimeException("Test snapshot failure");
        failureState.recordFailure(testException);
        failureState.recordSuccess();
        failureState.recordFailure(testException);
        
        // Get snapshot
        ReplicaFailureState.FailureStateSnapshot snapshot = failureState.getSnapshot();
        
        assertNotNull(snapshot, "Snapshot should not be null");
        assertEquals(1, snapshot.getReplicaId(), "Replica ID should match");
        assertEquals("test-region", snapshot.getRegion(), "Region should match");
        assertEquals(2, snapshot.getFailureCount(), "Failure count should match");
        assertEquals(1, snapshot.getConsecutiveFailures(), "Consecutive failures should match");
        assertEquals(1, snapshot.getConsecutiveSuccesses(), "Consecutive successes should match");
        assertEquals("Test snapshot failure", snapshot.getLastFailureReason(), 
                    "Last failure reason should match");
        assertTrue(snapshot.getPhi() >= 0, "PHI should be non-negative");
        
        // Test snapshot string representation
        String snapshotString = snapshot.toString();
        assertTrue(snapshotString.contains("replica=1"), "Snapshot string should contain replica ID");
        assertTrue(snapshotString.contains("region=test-region"), "Snapshot string should contain region");
        
        LOGGER.info("Snapshot: {}", snapshotString);
        LOGGER.info("Failure state snapshot test completed successfully");
    }

    @Test
    @DisplayName("Test concurrent access")
    void testConcurrentAccess() throws InterruptedException {
        LOGGER.info("Testing concurrent access to failure state");
        
        int numThreads = 5;
        int operationsPerThread = 100;
        
        Thread[] threads = new Thread[numThreads];
        
        // Create threads that perform mixed operations
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    if (j % 2 == 0) {
                        failureState.recordSuccess();
                    } else {
                        failureState.recordFailure(new RuntimeException("Concurrent test failure " + threadId));
                    }
                    
                    // Occasionally read state
                    if (j % 10 == 0) {
                        failureState.isHealthy();
                        failureState.calculatePhi(System.currentTimeMillis());
                        failureState.getSnapshot();
                    }
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(5000); // 5 second timeout
        }
        
        // Verify final state consistency
        int expectedFailures = (numThreads * operationsPerThread) / 2;
        assertEquals(expectedFailures, failureState.getFailureCount(), 
                    "Total failure count should match expected");
        
        LOGGER.info("Final failure count: {}, Expected: {}", 
                   failureState.getFailureCount(), expectedFailures);
        LOGGER.info("Concurrent access test completed successfully");
    }
}