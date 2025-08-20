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

package com.automq.stream.s3.quorum.integration;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.quorum.S3QuorumStorage;
import com.automq.stream.s3.quorum.factory.S3QuorumStorageFactory;
import com.automq.stream.s3.quorum.metrics.MetricsSnapshot;
import com.automq.stream.s3.quorum.metrics.MetricsCollector;
import com.automq.stream.s3.quorum.metrics.ReplicaMetrics;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for metrics system with S3 Quorum Storage
 * Tests the complete metrics flow from operations to collection
 */
@DisplayName("Metrics Integration Tests")
public class MetricsIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsIntegrationTest.class);

    @TempDir
    Path tempDir;

    private File configFile;
    private Config baseConfig;
    private S3QuorumStorage quorumStorage;
    private TestMetricsListener metricsListener;

    @BeforeEach
    void setUp() throws IOException {
        LOGGER.info("Setting up Metrics Integration test environment");
        
        // Set up test credentials
        System.setProperty("AWS_ACCESS_KEY_ID", "test-access-key");
        System.setProperty("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        
        // Create base configuration
        baseConfig = new Config();
        baseConfig.nodeId(1);
        baseConfig.walCacheSize(1024 * 1024); // 1MB
        baseConfig.mockEnable(true); // Use mock storage for testing
        
        // Create test configuration file
        configFile = tempDir.resolve("metrics-integration-test.properties").toFile();
        createTestConfigFile(configFile);
        
        metricsListener = new TestMetricsListener();
        
        LOGGER.info("Metrics Integration test environment setup completed");
    }

    @AfterEach
    void tearDown() {
        if (quorumStorage != null) {
            try {
                quorumStorage.shutdown();
                LOGGER.info("Quorum storage shutdown completed");
            } catch (Exception e) {
                LOGGER.warn("Error during quorum storage shutdown: {}", e.getMessage());
            }
        }
        
        // Clean up system properties
        System.clearProperty("AWS_ACCESS_KEY_ID");
        System.clearProperty("AWS_SECRET_ACCESS_KEY");
    }

    @Test
    @DisplayName("End-to-end metrics integration test")
    void testCompleteMetricsIntegration() throws Exception {
        LOGGER.info("Starting complete metrics integration test");
        
        // Step 1: Initialize quorum storage with metrics
        setupQuorumStorageWithMetrics();
        
        // Step 2: Verify initial metrics state
        verifyInitialMetricsState();
        
        // Step 3: Perform write operations and verify metrics
        performWriteOperationsAndVerifyMetrics();
        
        // Step 4: Perform read operations and verify metrics
        performReadOperationsAndVerifyMetrics();
        
        // Step 5: Verify per-replica metrics
        verifyPerReplicaMetrics();
        
        // Step 6: Verify metrics snapshot and health status
        verifyMetricsSnapshotAndHealthStatus();
        
        LOGGER.info("Complete metrics integration test completed successfully");
    }

    @Test
    @DisplayName("Test metrics collection during operations")
    void testMetricsCollectionDuringOperations() throws Exception {
        LOGGER.info("Starting metrics collection during operations test");
        
        setupQuorumStorageWithMetrics();
        
        // Start background operations
        Thread operationsThread = new Thread(() -> {
            try {
                for (int i = 0; i < 20; i++) {
                    // Perform mixed operations
                    StreamRecordBatch recordBatch = createTestStreamRecord(2000L + i, i * 100, "operation-data-" + i);
                    AppendContext appendContext = createTestAppendContext();
                    
                    quorumStorage.append(appendContext, recordBatch).get(5, TimeUnit.SECONDS);
                    
                    FetchContext fetchContext = createTestFetchContext();
                    quorumStorage.read(fetchContext, 2000L + i, i * 100, (i + 1) * 100, 1024).get(5, TimeUnit.SECONDS);
                    
                    Thread.sleep(100); // Small delay between operations
                }
            } catch (Exception e) {
                LOGGER.error("Error during background operations", e);
            }
        });
        
        operationsThread.start();
        
        // Wait for operations to complete
        operationsThread.join(30000); // 30 second timeout
        
        // Wait for metrics collection
        Thread.sleep(2000);
        
        // Verify metrics were collected during operations
        MetricsSnapshot finalSnapshot = quorumStorage.getMetrics().getSnapshot();
        assertTrue(finalSnapshot.getWriteRequestsTotal() >= 20, "Should have recorded write operations");
        assertTrue(finalSnapshot.getReadRequestsTotal() >= 20, "Should have recorded read operations");
        assertTrue(finalSnapshot.getWriteRequestsSuccessful() > 0, "Should have successful writes");
        assertTrue(finalSnapshot.getReadRequestsSuccessful() > 0, "Should have successful reads");
        
        LOGGER.info("Metrics collection during operations test completed successfully");
        LOGGER.info("Final metrics: {} writes, {} reads, success rates: {:.1f}%/{:.1f}%",
                   finalSnapshot.getWriteRequestsTotal(), finalSnapshot.getReadRequestsTotal(),
                   finalSnapshot.getWriteSuccessRate() * 100, finalSnapshot.getReadSuccessRate() * 100);
    }

    @Test
    @DisplayName("Test metrics listener notifications")
    void testMetricsListenerNotifications() throws Exception {
        LOGGER.info("Starting metrics listener notifications test");
        
        setupQuorumStorageWithMetrics();
        
        // Perform operations that should trigger listener notifications
        for (int i = 0; i < 5; i++) {
            StreamRecordBatch recordBatch = createTestStreamRecord(3000L + i, i * 50, "listener-test-" + i);
            AppendContext appendContext = createTestAppendContext();
            
            quorumStorage.append(appendContext, recordBatch).get(5, TimeUnit.SECONDS);
        }
        
        // Wait for metrics collection
        assertTrue(metricsListener.waitForCollections(3, 10000), "Should receive metric collections");
        
        // Verify listener received notifications
        assertTrue(metricsListener.getCollectionCount() > 0, "Should have received collections");
        assertNotNull(metricsListener.getLastSnapshot(), "Should have last snapshot");
        
        MetricsSnapshot snapshot = metricsListener.getLastSnapshot();
        assertTrue(snapshot.getWriteRequestsTotal() >= 5, "Should record all write requests");
        
        LOGGER.info("Metrics listener notifications test completed successfully");
    }

    @Test
    @DisplayName("Test health status tracking in metrics")
    void testHealthStatusTrackingInMetrics() throws Exception {
        LOGGER.info("Starting health status tracking test");
        
        setupQuorumStorageWithMetrics();
        
        // Initially should be healthy
        MetricsSnapshot initialSnapshot = quorumStorage.getMetrics().getSnapshot();
        LOGGER.info("Initial health status: {}", initialSnapshot.getHealthStatus());
        
        // Perform some operations to establish baseline
        for (int i = 0; i < 3; i++) {
            StreamRecordBatch recordBatch = createTestStreamRecord(4000L + i, i * 60, "health-test-" + i);
            AppendContext appendContext = createTestAppendContext();
            
            quorumStorage.append(appendContext, recordBatch).get(5, TimeUnit.SECONDS);
        }
        
        // Get metrics after operations
        MetricsSnapshot operationalSnapshot = quorumStorage.getMetrics().getSnapshot();
        LOGGER.info("After operations health status: {}", operationalSnapshot.getHealthStatus());
        
        // Verify health-related metrics
        assertTrue(operationalSnapshot.getQuorumHealthRatio() > 0, "Should have positive health ratio");
        assertTrue(operationalSnapshot.getQuorumAvailability() > 0, "Should have positive availability");
        
        LOGGER.info("Health status tracking test completed successfully");
    }

    @Test
    @DisplayName("Test metrics reset functionality")
    void testMetricsResetFunctionality() throws Exception {
        LOGGER.info("Starting metrics reset functionality test");
        
        setupQuorumStorageWithMetrics();
        
        // Perform operations to generate metrics
        for (int i = 0; i < 3; i++) {
            StreamRecordBatch recordBatch = createTestStreamRecord(5000L + i, i * 70, "reset-test-" + i);
            AppendContext appendContext = createTestAppendContext();
            
            quorumStorage.append(appendContext, recordBatch).get(5, TimeUnit.SECONDS);
        }
        
        // Verify metrics are recorded
        MetricsSnapshot beforeReset = quorumStorage.getMetrics().getSnapshot();
        assertTrue(beforeReset.getWriteRequestsTotal() > 0, "Should have metrics before reset");
        
        // Reset metrics
        quorumStorage.getMetrics().reset();
        
        // Verify metrics are reset
        MetricsSnapshot afterReset = quorumStorage.getMetrics().getSnapshot();
        assertEquals(0, afterReset.getWriteRequestsTotal(), "Write requests should be reset");
        assertEquals(0, afterReset.getReadRequestsTotal(), "Read requests should be reset");
        assertEquals(0, afterReset.getReplicaFailuresTotal(), "Failures should be reset");
        
        LOGGER.info("Metrics reset functionality test completed successfully");
    }

    // Helper methods

    private void setupQuorumStorageWithMetrics() throws Exception {
        LOGGER.info("Setting up quorum storage with metrics");
        
        // Create quorum storage from configuration
        quorumStorage = S3QuorumStorageFactory.createFromConfig(
            configFile.getAbsolutePath(),
            baseConfig,
            null, // WriteAheadLog - null for test
            null, // StreamManager - null for test
            null, // S3BlockCache - null for test
            null  // StorageFailureHandler - null for test
        );
        
        assertNotNull(quorumStorage, "Quorum storage should be created successfully");
        assertNotNull(quorumStorage.getMetrics(), "Metrics should be initialized");
        assertNotNull(quorumStorage.getMetricsCollector(), "Metrics collector should be initialized");
        
        // Add our test listener
        quorumStorage.addMetricsListener(metricsListener);
        
        // Start the storage system
        quorumStorage.startup();
        
        // Verify quorum is available
        assertTrue(quorumStorage.getQuorumState().hasQuorum(), 
                  "Quorum should be available after startup");
        
        LOGGER.info("Quorum storage with metrics setup completed");
    }

    private void verifyInitialMetricsState() {
        LOGGER.info("Verifying initial metrics state");
        
        MetricsSnapshot snapshot = quorumStorage.getMetrics().getSnapshot();
        assertEquals(0, snapshot.getWriteRequestsTotal(), "Initial writes should be 0");
        assertEquals(0, snapshot.getReadRequestsTotal(), "Initial reads should be 0");
        assertEquals(3, snapshot.getTotalReplicas(), "Should have 3 replicas");
        
        LOGGER.info("Initial metrics state verified");
    }

    private void performWriteOperationsAndVerifyMetrics() throws Exception {
        LOGGER.info("Performing write operations and verifying metrics");
        
        int numWrites = 5;
        for (int i = 0; i < numWrites; i++) {
            StreamRecordBatch recordBatch = createTestStreamRecord(1000L + i, i * 100, "write-test-" + i);
            AppendContext appendContext = createTestAppendContext();
            
            CompletableFuture<Void> appendFuture = quorumStorage.append(appendContext, recordBatch);
            appendFuture.get(30, TimeUnit.SECONDS);
        }
        
        MetricsSnapshot snapshot = quorumStorage.getMetrics().getSnapshot();
        assertEquals(numWrites, snapshot.getWriteRequestsTotal(), "Should record all write requests");
        assertTrue(snapshot.getWriteRequestsSuccessful() > 0, "Should have successful writes");
        assertTrue(snapshot.getWriteBytesTotal() > 0, "Should record written bytes");
        
        LOGGER.info("Write operations and metrics verification completed");
    }

    private void performReadOperationsAndVerifyMetrics() throws Exception {
        LOGGER.info("Performing read operations and verifying metrics");
        
        int numReads = 3;
        for (int i = 0; i < numReads; i++) {
            FetchContext fetchContext = createTestFetchContext();
            CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
                fetchContext, 1000L + i, i * 100, (i + 1) * 100, 1024);
            readFuture.get(30, TimeUnit.SECONDS);
        }
        
        MetricsSnapshot snapshot = quorumStorage.getMetrics().getSnapshot();
        assertEquals(numReads, snapshot.getReadRequestsTotal(), "Should record all read requests");
        assertTrue(snapshot.getReadRequestsSuccessful() > 0, "Should have successful reads");
        
        LOGGER.info("Read operations and metrics verification completed");
    }

    private void verifyPerReplicaMetrics() {
        LOGGER.info("Verifying per-replica metrics");
        
        for (int i = 0; i < 3; i++) {
            ReplicaMetrics replicaMetrics = quorumStorage.getMetrics().getReplicaMetrics(i);
            assertNotNull(replicaMetrics, "Replica " + i + " metrics should exist");
            assertEquals(i, replicaMetrics.getReplicaId(), "Replica ID should match");
            
            LOGGER.debug("Replica {} metrics: {}", i, replicaMetrics.toString());
        }
        
        LOGGER.info("Per-replica metrics verification completed");
    }

    private void verifyMetricsSnapshotAndHealthStatus() {
        LOGGER.info("Verifying metrics snapshot and health status");
        
        MetricsSnapshot snapshot = quorumStorage.getMetrics().getSnapshot();
        
        assertTrue(snapshot.getTotalRequests() > 0, "Should have total requests");
        assertTrue(snapshot.getTotalSuccessfulRequests() > 0, "Should have successful requests");
        assertTrue(snapshot.getOverallSuccessRate() > 0, "Should have positive success rate");
        assertTrue(snapshot.getTimestamp() > 0, "Should have valid timestamp");
        
        String healthStatus = snapshot.getHealthStatus();
        assertNotNull(healthStatus, "Should have health status");
        
        LOGGER.info("Metrics snapshot: {}", snapshot.toString());
        LOGGER.info("Detailed metrics: {}", snapshot.toDetailedString());
        LOGGER.info("Metrics snapshot and health status verification completed");
    }

    private void createTestConfigFile(File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("# Metrics Integration Test Configuration\n");
            writer.write("automq.s3.quorum.enabled=true\n");
            writer.write("automq.s3.quorum.size=3\n");
            writer.write("automq.s3.quorum.write.quorum.size=2\n");
            writer.write("automq.s3.quorum.read.quorum.size=1\n");
            writer.write("automq.s3.quorum.write.timeout.ms=30000\n");
            writer.write("automq.s3.quorum.read.timeout.ms=10000\n");
            
            // Configure 3 replicas for integration testing
            for (int i = 0; i < 3; i++) {
                writer.write(String.format("automq.s3.quorum.replica.%d.id=%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.region=metrics-test-region-%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.bucket=metrics-test-bucket-%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.endpoint=http://localhost:920%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.role=%s\n", i, i == 0 ? "PRIMARY" : "SECONDARY"));
                writer.write(String.format("automq.s3.quorum.replica.%d.priority=%d\n", i, (3 - i) * 25));
            }
        }
        LOGGER.info("Created metrics integration test configuration file: {}", file.getAbsolutePath());
    }

    private StreamRecordBatch createTestStreamRecord(long streamId, long baseOffset, String data) {
        // Create a simple StreamRecordBatch for testing
        io.netty.buffer.ByteBuf payload = io.netty.buffer.Unpooled.wrappedBuffer(data.getBytes());
        return new StreamRecordBatch(streamId, 0L, baseOffset, 1, payload);
    }

    private AppendContext createTestAppendContext() {
        return new AppendContext() {
            @Override
            public String toString() {
                return "TestAppendContext{" + System.currentTimeMillis() + "}";
            }
        };
    }

    private FetchContext createTestFetchContext() {
        return new FetchContext() {
            @Override
            public String toString() {
                return "TestFetchContext{" + System.currentTimeMillis() + "}";
            }
        };
    }

    /**
     * Test metrics listener for integration testing
     */
    private static class TestMetricsListener implements MetricsCollector.MetricsListener {
        private final AtomicInteger collectionCount = new AtomicInteger(0);
        private volatile MetricsSnapshot lastSnapshot;

        @Override
        public void onMetricsCollected(MetricsSnapshot snapshot) {
            collectionCount.incrementAndGet();
            lastSnapshot = snapshot;
            LOGGER.debug("Metrics integration test listener received collection #{}: {}", 
                       collectionCount.get(), snapshot.getHealthStatus());
        }

        @Override
        public void onHealthStatusChanged(boolean wasHealthy, boolean isHealthy, MetricsSnapshot snapshot) {
            LOGGER.info("Metrics integration test: Health status changed {} -> {}", wasHealthy, isHealthy);
        }

        @Override
        public void onCriticalEvent(String event, MetricsSnapshot snapshot) {
            LOGGER.warn("Metrics integration test: Critical event: {}", event);
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
                Thread.sleep(200);
            }
            return collectionCount.get() >= expectedCount;
        }
    }
}