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
import com.automq.stream.s3.quorum.state.QuorumState;

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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test suite for MetricsCollector functionality
 */
@DisplayName("Metrics Collector Tests")
public class MetricsCollectorTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsCollectorTest.class);

    private QuorumConfig quorumConfig;
    private QuorumState quorumState;
    private QuorumMetrics metrics;
    private MetricsCollector collector;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up MetricsCollector test environment");
        
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
        
        metrics = new QuorumMetrics(quorumConfig);
        
        // Create collector with shorter intervals for testing
        collector = new MetricsCollector(metrics, quorumState, 1000, 500); // 1s collection, 0.5s health check
        
        LOGGER.info("MetricsCollector test environment setup completed");
    }

    @AfterEach
    void tearDown() {
        if (collector != null) {
            collector.stop();
        }
        if (quorumState != null) {
            quorumState.shutdown();
        }
        LOGGER.info("MetricsCollector test environment cleaned up");
    }

    @Test
    @DisplayName("Test metrics collector lifecycle")
    void testMetricsCollectorLifecycle() {
        LOGGER.info("Testing metrics collector lifecycle");
        
        // Initially not started
        MetricsCollector.CollectionStats stats = collector.getCollectionStats();
        assertTrue(!stats.isRunning());
        assertEquals(0, stats.getListenerCount());
        
        // Start collector
        collector.start();
        stats = collector.getCollectionStats();
        assertTrue(stats.isRunning());
        assertTrue(stats.getLastCollectionTime() > 0);
        
        // Stop collector
        collector.stop();
        stats = collector.getCollectionStats();
        assertTrue(!stats.isRunning());
        
        LOGGER.info("Metrics collector lifecycle test completed successfully");
    }

    @Test
    @DisplayName("Test periodic metrics collection")
    void testPeriodicMetricsCollection() throws InterruptedException {
        LOGGER.info("Testing periodic metrics collection");
        
        TestMetricsListener listener = new TestMetricsListener();
        collector.addListener(listener);
        
        // Record some metrics
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(100, 1024);
        
        // Start collector
        collector.start();
        
        // Wait for at least 2 collections
        assertTrue(listener.waitForCollections(2, 5000), "Should receive at least 2 metric collections");
        
        // Verify listener received metrics
        assertTrue(listener.getCollectionCount() >= 2);
        assertNotNull(listener.getLastSnapshot());
        
        MetricsSnapshot snapshot = listener.getLastSnapshot();
        assertEquals(1, snapshot.getWriteRequestsTotal());
        assertEquals(1, snapshot.getWriteRequestsSuccessful());
        
        LOGGER.info("Periodic metrics collection test completed successfully");
    }

    @Test
    @DisplayName("Test health status change detection")
    void testHealthStatusChangeDetection() throws InterruptedException {
        LOGGER.info("Testing health status change detection");
        
        TestHealthAwareListener healthListener = new TestHealthAwareListener();
        collector.addListener(new MetricsCollector.HealthAwareMetricsListener(healthListener));
        
        // Start collector
        collector.start();
        
        // Initially healthy
        metrics.updateQuorumHealth(3, true);
        Thread.sleep(600); // Wait for health check
        
        // Simulate health degradation
        metrics.updateQuorumHealth(1, false);
        Thread.sleep(600); // Wait for health check
        
        // Simulate recovery
        metrics.updateQuorumHealth(3, true);
        Thread.sleep(600); // Wait for health check
        
        // Wait for events to be processed
        assertTrue(healthListener.waitForHealthChange(3000), "Should detect health status changes");
        
        assertTrue(healthListener.getHealthChangeCount() > 0);
        assertTrue(healthListener.getCriticalEventCount() > 0);
        
        LOGGER.info("Health status change detection test completed successfully");
    }

    @Test
    @DisplayName("Test metrics listener management")
    void testMetricsListenerManagement() throws InterruptedException {
        LOGGER.info("Testing metrics listener management");
        
        TestMetricsListener listener1 = new TestMetricsListener();
        TestMetricsListener listener2 = new TestMetricsListener();
        
        // Add listeners
        collector.addListener(listener1);
        collector.addListener(listener2);
        
        MetricsCollector.CollectionStats stats = collector.getCollectionStats();
        assertEquals(2, stats.getListenerCount());
        
        // Start collector and record some metrics
        collector.start();
        metrics.recordReadRequest();
        metrics.recordReadSuccess(50, 512);
        
        // Wait for collection
        Thread.sleep(1200);
        
        // Both listeners should receive events
        assertTrue(listener1.getCollectionCount() > 0);
        assertTrue(listener2.getCollectionCount() > 0);
        
        // Remove one listener
        collector.removeListener(listener1);
        stats = collector.getCollectionStats();
        assertEquals(1, stats.getListenerCount());
        
        LOGGER.info("Metrics listener management test completed successfully");
    }

    @Test
    @DisplayName("Test forced metrics collection")
    void testForcedMetricsCollection() {
        LOGGER.info("Testing forced metrics collection");
        
        TestMetricsListener listener = new TestMetricsListener();
        collector.addListener(listener);
        
        // Record some metrics
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(150, 2048);
        
        // Force collection without starting periodic collection
        MetricsSnapshot snapshot = collector.forceCollection();
        
        assertNotNull(snapshot);
        assertEquals(1, snapshot.getWriteRequestsTotal());
        assertEquals(1, snapshot.getWriteRequestsSuccessful());
        assertEquals(2048, snapshot.getWriteBytesTotal());
        
        // Verify listener was notified
        assertEquals(1, listener.getCollectionCount());
        
        LOGGER.info("Forced metrics collection test completed successfully");
    }

    @Test
    @DisplayName("Test current vs last snapshot")
    void testCurrentVsLastSnapshot() throws InterruptedException {
        LOGGER.info("Testing current vs last snapshot");
        
        // Start collector
        collector.start();
        
        // Record initial metrics
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(100, 1024);
        
        // Force collection to get baseline
        MetricsSnapshot snapshot1 = collector.forceCollection();
        assertEquals(1, snapshot1.getWriteRequestsTotal());
        
        // Record more metrics
        metrics.recordWriteRequest();
        metrics.recordWriteSuccess(150, 1536);
        
        // Get current snapshot (should be different from last collected)
        MetricsSnapshot currentSnapshot = collector.getCurrentSnapshot();
        assertEquals(2, currentSnapshot.getWriteRequestsTotal());
        
        // Last snapshot should still be the old one until next collection
        MetricsSnapshot lastSnapshot = collector.getLastSnapshot();
        assertEquals(1, lastSnapshot.getWriteRequestsTotal());
        
        // Force collection to update last snapshot
        collector.forceCollection();
        lastSnapshot = collector.getLastSnapshot();
        assertEquals(2, lastSnapshot.getWriteRequestsTotal());
        
        LOGGER.info("Current vs last snapshot test completed successfully");
    }

    @Test
    @DisplayName("Test concurrent collection and metrics recording")
    void testConcurrentCollectionAndRecording() throws InterruptedException {
        LOGGER.info("Testing concurrent collection and metrics recording");
        
        TestMetricsListener listener = new TestMetricsListener();
        collector.addListener(listener);
        
        // Start collector
        collector.start();
        
        // Start background thread to continuously record metrics
        Thread metricsThread = new Thread(() -> {
            for (int i = 0; i < 50; i++) {
                metrics.recordWriteRequest();
                metrics.recordWriteSuccess(100 + i, 1024);
                metrics.recordReadRequest();
                metrics.recordReadSuccess(50 + i, 512);
                
                try {
                    Thread.sleep(50); // Record every 50ms
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        
        metricsThread.start();
        
        // Wait for metrics recording and collection to happen
        Thread.sleep(3000);
        
        metricsThread.join(1000);
        
        // Verify metrics were collected while being recorded
        assertTrue(listener.getCollectionCount() > 0);
        MetricsSnapshot finalSnapshot = collector.getCurrentSnapshot();
        assertTrue(finalSnapshot.getWriteRequestsTotal() > 0);
        assertTrue(finalSnapshot.getReadRequestsTotal() > 0);
        
        LOGGER.info("Final metrics: {} writes, {} reads", 
                   finalSnapshot.getWriteRequestsTotal(), finalSnapshot.getReadRequestsTotal());
        LOGGER.info("Concurrent collection and recording test completed successfully");
    }

    /**
     * Test metrics listener implementation
     */
    private static class TestMetricsListener implements MetricsCollector.MetricsListener {
        private final AtomicInteger collectionCount = new AtomicInteger(0);
        private volatile MetricsSnapshot lastSnapshot;
        private final CountDownLatch collectionLatch = new CountDownLatch(1);

        @Override
        public void onMetricsCollected(MetricsSnapshot snapshot) {
            collectionCount.incrementAndGet();
            lastSnapshot = snapshot;
            collectionLatch.countDown();
            LOGGER.debug("Received metrics collection #{}: {}", collectionCount.get(), snapshot);
        }

        public int getCollectionCount() {
            return collectionCount.get();
        }

        public MetricsSnapshot getLastSnapshot() {
            return lastSnapshot;
        }

        public boolean waitForCollections(int expectedCount, long timeoutMs) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            while (collectionCount.get() < expectedCount && 
                   (System.currentTimeMillis() - startTime) < timeoutMs) {
                Thread.sleep(100);
            }
            return collectionCount.get() >= expectedCount;
        }
    }

    /**
     * Test health-aware listener implementation
     */
    private static class TestHealthAwareListener implements MetricsCollector.MetricsListener {
        private final AtomicInteger collectionCount = new AtomicInteger(0);
        private final AtomicInteger healthChangeCount = new AtomicInteger(0);
        private final AtomicInteger criticalEventCount = new AtomicInteger(0);
        private final CountDownLatch healthChangeLatch = new CountDownLatch(1);

        @Override
        public void onMetricsCollected(MetricsSnapshot snapshot) {
            collectionCount.incrementAndGet();
            LOGGER.debug("Health-aware listener: metrics collected, health status: {}", 
                       snapshot.getHealthStatus());
        }

        @Override
        public void onHealthStatusChanged(boolean wasHealthy, boolean isHealthy, MetricsSnapshot snapshot) {
            healthChangeCount.incrementAndGet();
            healthChangeLatch.countDown();
            LOGGER.info("Health status changed: {} -> {}, snapshot: {}", 
                       wasHealthy, isHealthy, snapshot.getHealthStatus());
        }

        @Override
        public void onCriticalEvent(String event, MetricsSnapshot snapshot) {
            criticalEventCount.incrementAndGet();
            LOGGER.warn("Critical event: {}, snapshot: {}", event, snapshot.getHealthStatus());
        }

        public int getCollectionCount() {
            return collectionCount.get();
        }

        public int getHealthChangeCount() {
            return healthChangeCount.get();
        }

        public int getCriticalEventCount() {
            return criticalEventCount.get();
        }

        public boolean waitForHealthChange(long timeoutMs) throws InterruptedException {
            return healthChangeLatch.await(timeoutMs, TimeUnit.MILLISECONDS);
        }
    }
}