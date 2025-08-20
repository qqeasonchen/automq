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

package com.automq.stream.s3.quorum.metrics;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for QuorumMetrics functionality
 */
@DisplayName("Quorum Metrics Tests")
public class QuorumMetricsTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumMetricsTest.class);

    private QuorumConfig quorumConfig;
    private QuorumMetrics metrics;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up QuorumMetrics test environment");
        
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
        
        metrics = new QuorumMetrics(quorumConfig);
        
        LOGGER.info("QuorumMetrics test environment setup completed");
    }

    @Test
    @DisplayName("Test initial metrics state")
    void testInitialMetricsState() {
        LOGGER.info("Testing initial metrics state");
        
        // Verify initial write metrics
        assertEquals(0, metrics.getWriteRequestsTotal());
        assertEquals(0, metrics.getWriteRequestsSuccessful());
        assertEquals(0, metrics.getWriteRequestsFailed());
        assertEquals(0.0, metrics.getWriteSuccessRate());
        assertEquals(0.0, metrics.getWriteAverageLatency());
        assertEquals(0, metrics.getWriteBytesTotal());
        
        // Verify initial read metrics
        assertEquals(0, metrics.getReadRequestsTotal());
        assertEquals(0, metrics.getReadRequestsSuccessful());
        assertEquals(0, metrics.getReadRequestsFailed());
        assertEquals(0.0, metrics.getReadSuccessRate());
        assertEquals(0.0, metrics.getReadAverageLatency());
        assertEquals(0, metrics.getReadBytesTotal());
        
        // Verify initial health metrics
        assertEquals(3, metrics.getTotalReplicas());
        assertEquals(0, metrics.getHealthyReplicas()); // Initially 0, updated by health checks
        
        // Verify initial failure metrics
        assertEquals(0, metrics.getReplicaFailuresTotal());
        assertEquals(0, metrics.getReplicaRecoveriesTotal());
        assertEquals(0, metrics.getEmergencyRecoveriesTotal());
        assertEquals(0, metrics.getQuorumLossEvents());
        
        LOGGER.info("Initial metrics state test completed successfully");
    }

    @Test
    @DisplayName("Test write operation metrics recording")
    void testWriteOperationMetrics() {
        LOGGER.info("Testing write operation metrics recording");
        
        // Record write operations
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(100, 1024);
        
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(150, 2048);
        
        metrics.recordWriteRequest();
        metrics.recordWriteFailure(200);
        
        // Verify metrics
        assertEquals(3, metrics.getWriteRequestsTotal());
        assertEquals(2, metrics.getWriteRequestsSuccessful());
        assertEquals(1, metrics.getWriteRequestsFailed());
        assertEquals(2.0/3.0, metrics.getWriteSuccessRate(), 0.001);
        assertEquals(150.0, metrics.getWriteAverageLatency(), 0.001); // (100+150+200)/3
        assertEquals(3072, metrics.getWriteBytesTotal()); // 1024+2048
        
        LOGGER.info("Write operation metrics test completed successfully");
    }

    @Test
    @DisplayName("Test read operation metrics recording")
    void testReadOperationMetrics() {
        LOGGER.info("Testing read operation metrics recording");
        
        // Record read operations
        metrics.recordReadRequest();
        metrics.recordReadSuccess(50, 512);
        
        metrics.recordReadRequest();
        metrics.recordReadSuccess(75, 256);
        
        metrics.recordReadRequest();
        metrics.recordReadFailure(100);
        
        // Verify metrics
        assertEquals(3, metrics.getReadRequestsTotal());
        assertEquals(2, metrics.getReadRequestsSuccessful());
        assertEquals(1, metrics.getReadRequestsFailed());
        assertEquals(2.0/3.0, metrics.getReadSuccessRate(), 0.001);
        assertEquals(75.0, metrics.getReadAverageLatency(), 0.001); // (50+75+100)/3
        assertEquals(768, metrics.getReadBytesTotal()); // 512+256
        
        LOGGER.info("Read operation metrics test completed successfully");
    }

    @Test
    @DisplayName("Test quorum health metrics")
    void testQuorumHealthMetrics() throws InterruptedException {
        LOGGER.info("Testing quorum health metrics");
        
        // Update health with all replicas healthy
        metrics.updateQuorumHealth(3, true);
        assertEquals(3, metrics.getHealthyReplicas());
        assertEquals(1.0, metrics.getQuorumHealthRatio());
        assertTrue(metrics.getQuorumAvailableTime() > 0);
        
        // Wait a bit and simulate one replica failure
        Thread.sleep(100);
        metrics.updateQuorumHealth(2, true); // Still has quorum
        assertEquals(2, metrics.getHealthyReplicas());
        assertEquals(2.0/3.0, metrics.getQuorumHealthRatio(), 0.001);
        
        // Simulate quorum loss
        Thread.sleep(100);
        metrics.updateQuorumHealth(1, false);
        assertEquals(1, metrics.getHealthyReplicas());
        assertEquals(1.0/3.0, metrics.getQuorumHealthRatio(), 0.001);
        assertTrue(metrics.getQuorumUnavailableTime() > 0);
        
        LOGGER.info("Quorum health metrics test completed successfully");
    }

    @Test
    @DisplayName("Test failure and recovery metrics")
    void testFailureAndRecoveryMetrics() {
        LOGGER.info("Testing failure and recovery metrics");
        
        // Record replica failures
        metrics.recordReplicaFailure(0);
        metrics.recordReplicaFailure(1);
        metrics.recordReplicaFailure(0); // Same replica fails again
        
        assertEquals(3, metrics.getReplicaFailuresTotal());
        
        // Record replica recoveries
        metrics.recordReplicaRecovery(0);
        metrics.recordReplicaRecovery(1);
        
        assertEquals(2, metrics.getReplicaRecoveriesTotal());
        
        // Record emergency recovery and quorum loss
        metrics.recordEmergencyRecovery();
        metrics.recordQuorumLoss();
        
        assertEquals(1, metrics.getEmergencyRecoveriesTotal());
        assertEquals(1, metrics.getQuorumLossEvents());
        
        LOGGER.info("Failure and recovery metrics test completed successfully");
    }

    @Test
    @DisplayName("Test per-replica metrics")
    void testPerReplicaMetrics() {
        LOGGER.info("Testing per-replica metrics");
        
        // Record operations for different replicas
        metrics.recordReplicaWrite(0, true, 100);
        metrics.recordReplicaWrite(0, false, 150);
        metrics.recordReplicaWrite(1, true, 80);
        
        metrics.recordReplicaRead(0, true, 50);
        metrics.recordReplicaRead(1, true, 60);
        metrics.recordReplicaRead(2, false, 200);
        
        // Verify replica 0 metrics
        ReplicaMetrics replica0 = metrics.getReplicaMetrics(0);
        assertNotNull(replica0);
        assertEquals(0, replica0.getReplicaId());
        assertEquals(2, replica0.getWriteRequestsTotal());
        assertEquals(1, replica0.getWriteRequestsSuccessful());
        assertEquals(1, replica0.getWriteRequestsFailed());
        assertEquals(1, replica0.getReadRequestsTotal());
        assertEquals(1, replica0.getReadRequestsSuccessful());
        
        // Verify replica 1 metrics
        ReplicaMetrics replica1 = metrics.getReplicaMetrics(1);
        assertNotNull(replica1);
        assertEquals(1, replica1.getReplicaId());
        assertEquals(1, replica1.getWriteRequestsSuccessful());
        assertEquals(1, replica1.getReadRequestsSuccessful());
        
        // Verify replica 2 metrics
        ReplicaMetrics replica2 = metrics.getReplicaMetrics(2);
        assertNotNull(replica2);
        assertEquals(2, replica2.getReplicaId());
        assertEquals(0, replica2.getWriteRequestsTotal());
        assertEquals(1, replica2.getReadRequestsTotal());
        assertEquals(0, replica2.getReadRequestsSuccessful());
        assertEquals(1, replica2.getReadRequestsFailed());
        
        LOGGER.info("Per-replica metrics test completed successfully");
    }

    @Test
    @DisplayName("Test metrics snapshot")
    void testMetricsSnapshot() throws InterruptedException {
        LOGGER.info("Testing metrics snapshot");
        
        // Record some operations
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(100, 1024);
        metrics.recordReadRequest();
        metrics.recordReadSuccess(50, 512);
        metrics.updateQuorumHealth(3, true);
        
        Thread.sleep(10); // Ensure some time passes
        
        // Get snapshot
        MetricsSnapshot snapshot = metrics.getSnapshot();
        
        assertNotNull(snapshot);
        assertEquals(1, snapshot.getWriteRequestsTotal());
        assertEquals(1, snapshot.getWriteRequestsSuccessful());
        assertEquals(1, snapshot.getReadRequestsTotal());
        assertEquals(1, snapshot.getReadRequestsSuccessful());
        assertEquals(3, snapshot.getHealthyReplicas());
        assertEquals(3, snapshot.getTotalReplicas());
        assertTrue(snapshot.getTimestamp() > 0);
        
        // Test computed metrics
        assertEquals(2, snapshot.getTotalRequests());
        assertEquals(2, snapshot.getTotalSuccessfulRequests());
        assertEquals(1536, snapshot.getTotalBytesProcessed()); // 1024+512
        assertEquals(1.0, snapshot.getOverallSuccessRate());
        assertTrue(snapshot.isHealthy());
        assertEquals("HEALTHY", snapshot.getHealthStatus());
        
        LOGGER.info("Metrics snapshot: {}", snapshot.toString());
        LOGGER.info("Metrics snapshot test completed successfully");
    }

    @Test
    @DisplayName("Test metrics reset")
    void testMetricsReset() {
        LOGGER.info("Testing metrics reset");
        
        // Record some operations
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(100, 1024);
        metrics.recordReadRequest();
        metrics.recordReadFailure(200);
        metrics.recordReplicaFailure(0);
        
        // Verify metrics are recorded
        assertTrue(metrics.getWriteRequestsTotal() > 0);
        assertTrue(metrics.getReadRequestsTotal() > 0);
        assertTrue(metrics.getReplicaFailuresTotal() > 0);
        
        // Reset metrics
        metrics.reset();
        
        // Verify metrics are reset
        assertEquals(0, metrics.getWriteRequestsTotal());
        assertEquals(0, metrics.getWriteRequestsSuccessful());
        assertEquals(0, metrics.getWriteRequestsFailed());
        assertEquals(0, metrics.getReadRequestsTotal());
        assertEquals(0, metrics.getReadRequestsSuccessful());
        assertEquals(0, metrics.getReadRequestsFailed());
        assertEquals(0, metrics.getReplicaFailuresTotal());
        assertEquals(0, metrics.getReplicaRecoveriesTotal());
        assertEquals(3, metrics.getHealthyReplicas()); // Reset to default healthy state
        
        LOGGER.info("Metrics reset test completed successfully");
    }

    @Test
    @DisplayName("Test concurrent metrics recording")
    void testConcurrentMetricsRecording() throws InterruptedException {
        LOGGER.info("Testing concurrent metrics recording");
        
        int numThreads = 5;
        int operationsPerThread = 100;
        
        Thread[] threads = new Thread[numThreads];
        
        // Create threads that record metrics concurrently
        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    metrics.recordWriteRequest();
                    metrics.recordWriteSuccess(50 + threadId * 10, 1024);
                    
                    metrics.recordReadRequest();
                    metrics.recordReadSuccess(30 + threadId * 5, 512);
                    
                    metrics.recordReplicaWrite(threadId % 3, true, 100);
                    metrics.recordReplicaRead(threadId % 3, true, 50);
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
        
        // Verify final metrics
        int expectedOperations = numThreads * operationsPerThread;
        assertEquals(expectedOperations, metrics.getWriteRequestsTotal());
        assertEquals(expectedOperations, metrics.getWriteRequestsSuccessful());
        assertEquals(expectedOperations, metrics.getReadRequestsTotal());
        assertEquals(expectedOperations, metrics.getReadRequestsSuccessful());
        
        // Verify all operations were successful
        assertEquals(1.0, metrics.getWriteSuccessRate());
        assertEquals(1.0, metrics.getReadSuccessRate());
        
        LOGGER.info("Concurrent metrics recording test completed successfully");
        LOGGER.info("Final metrics: {} writes, {} reads", 
                   metrics.getWriteRequestsTotal(), metrics.getReadRequestsTotal());
    }
}