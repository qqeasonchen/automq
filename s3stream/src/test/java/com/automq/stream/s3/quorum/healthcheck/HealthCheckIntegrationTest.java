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

package com.automq.stream.s3.quorum.healthcheck;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.quorum.S3QuorumStorage;
import com.automq.stream.s3.quorum.factory.S3QuorumStorageFactory;

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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for the complete health check system with S3 Quorum Storage
 * Tests end-to-end health monitoring functionality
 */
@DisplayName("Health Check Integration Tests")
public class HealthCheckIntegrationTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckIntegrationTest.class);

    @TempDir
    Path tempDir;

    private File configFile;
    private Config baseConfig;
    private S3QuorumStorage quorumStorage;
    private TestHealthCheckListener healthCheckListener;

    @BeforeEach
    void setUp() throws IOException {
        LOGGER.info("Setting up Health Check Integration test environment");
        
        // Set up test credentials
        System.setProperty("AWS_ACCESS_KEY_ID", "test-access-key");
        System.setProperty("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        
        // Create base configuration
        baseConfig = new Config();
        baseConfig.nodeId(1);
        baseConfig.walCacheSize(1024 * 1024); // 1MB
        baseConfig.mockEnable(true); // Use mock storage for testing
        
        // Create test configuration file
        configFile = tempDir.resolve("health-check-integration-test.properties").toFile();
        createTestConfigFile(configFile);
        
        healthCheckListener = new TestHealthCheckListener();
        
        LOGGER.info("Health Check Integration test environment setup completed");
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
    @DisplayName("End-to-end health check integration test")
    void testCompleteHealthCheckIntegration() throws Exception {
        LOGGER.info("Starting complete health check integration test");
        
        // Step 1: Initialize quorum storage with health checks
        setupQuorumStorageWithHealthChecks();
        
        // Step 2: Verify initial health check state
        verifyInitialHealthCheckState();
        
        // Step 3: Perform operations and monitor health
        performOperationsAndMonitorHealth();
        
        // Step 4: Test health check responses to system changes
        testHealthCheckResponsesToChanges();
        
        // Step 5: Verify health check listener notifications
        verifyHealthCheckListenerNotifications();
        
        LOGGER.info("Complete health check integration test completed successfully");
    }

    @Test
    @DisplayName("Test health check system during normal operations")
    void testHealthCheckDuringOperations() throws Exception {
        LOGGER.info("Starting health check during operations test");
        
        setupQuorumStorageWithHealthChecks();
        
        // Start background operations
        Thread operationsThread = new Thread(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    // Perform write operations
                    StreamRecordBatch recordBatch = createTestStreamRecord(3000L + i, i * 100, "health-ops-data-" + i);
                    AppendContext appendContext = createTestAppendContext();
                    
                    quorumStorage.append(appendContext, recordBatch).get(5, TimeUnit.SECONDS);
                    
                    // Perform read operations
                    FetchContext fetchContext = createTestFetchContext();
                    quorumStorage.read(fetchContext, 3000L + i, i * 100, (i + 1) * 100, 1024).get(5, TimeUnit.SECONDS);
                    
                    Thread.sleep(200); // Pause between operations
                }
            } catch (Exception e) {
                LOGGER.error("Error during background operations", e);
            }
        });
        
        operationsThread.start();
        
        // Monitor health checks during operations
        Thread.sleep(1000); // Let operations run for a bit
        
        // Execute health checks multiple times during operations
        for (int i = 0; i < 3; i++) {
            Map<String, HealthCheckResult> results = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
            
            assertNotNull(results);
            assertTrue(results.size() >= 4); // 3 replica checks + 1 quorum check
            
            // Verify all health checks are succeeding during normal operations
            for (HealthCheckResult result : results.values()) {
                assertTrue(result.isHealthy() || result.isUnknown(), 
                          "Health check should be healthy during normal operations: " + result);
            }
            
            Thread.sleep(500);
        }
        
        operationsThread.join(10000); // Wait for operations to complete
        
        LOGGER.info("Health check during operations test completed successfully");
    }

    @Test
    @DisplayName("Test health check system responsiveness")
    void testHealthCheckSystemResponsiveness() throws Exception {
        LOGGER.info("Starting health check system responsiveness test");
        
        setupQuorumStorageWithHealthChecks();
        
        // Test immediate health check execution
        long startTime = System.currentTimeMillis();
        Map<String, HealthCheckResult> results = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;
        
        assertNotNull(results);
        assertTrue(duration < 5000); // Should complete within 5 seconds
        
        // Verify all expected health checks are present
        assertTrue(results.containsKey("QuorumHealthCheck"));
        assertTrue(results.containsKey("ReplicaHealthCheck-0"));
        assertTrue(results.containsKey("ReplicaHealthCheck-1"));
        assertTrue(results.containsKey("ReplicaHealthCheck-2"));
        
        // Test multiple rapid executions
        for (int i = 0; i < 5; i++) {
            startTime = System.currentTimeMillis();
            results = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
            duration = System.currentTimeMillis() - startTime;
            
            assertNotNull(results);
            assertTrue(duration < 3000); // Subsequent executions should be faster
            assertEquals(4, results.size()); // Consistent number of health checks
        }
        
        LOGGER.info("Health check system responsiveness test completed successfully");
    }

    @Test
    @DisplayName("Test health check listener integration")
    void testHealthCheckListenerIntegration() throws Exception {
        LOGGER.info("Starting health check listener integration test");
        
        setupQuorumStorageWithHealthChecks();
        
        // Add our test listener
        quorumStorage.addHealthCheckListener(healthCheckListener);
        
        // Execute health checks to trigger listener events
        quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        
        // Wait for listener events
        Thread.sleep(500);
        
        // Verify listener received events
        assertTrue(healthCheckListener.getResultCount() > 0, "Should have received health check results");
        
        // Execute again to potentially trigger status changes
        quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        
        // Wait for more events
        Thread.sleep(500);
        
        // Verify continued listener activity
        assertTrue(healthCheckListener.getResultCount() > 4, "Should have received more results");
        assertTrue(healthCheckListener.getExecutionCount() >= 2, "Should have completed multiple executions");
        
        LOGGER.info("Health check listener integration test completed successfully");
    }

    @Test
    @DisplayName("Test health check system with scheduler")
    void testHealthCheckWithScheduler() throws Exception {
        LOGGER.info("Starting health check with scheduler test");
        
        setupQuorumStorageWithHealthChecks();
        
        // Get scheduler stats
        HealthCheckScheduler.SchedulerStats initialStats = quorumStorage.getHealthCheckScheduler().getStats();
        assertTrue(initialStats.isRunning());
        assertEquals(4, initialStats.getHealthCheckCount()); // 3 replica + 1 quorum
        
        // Wait for scheduled executions
        Thread.sleep(2000); // Wait for at least one scheduled execution
        
        HealthCheckScheduler.SchedulerStats laterStats = quorumStorage.getHealthCheckScheduler().getStats();
        assertTrue(laterStats.getExecutionCount() > initialStats.getExecutionCount());
        assertTrue(laterStats.getLastExecutionTime() > initialStats.getLastExecutionTime());
        
        // Verify we can get last results
        Map<String, HealthCheckResult> lastResults = quorumStorage.getHealthCheckResults();
        assertNotNull(lastResults);
        assertTrue(lastResults.size() >= 4);
        
        LOGGER.info("Health check with scheduler test completed successfully");
    }

    // Helper methods

    private void setupQuorumStorageWithHealthChecks() throws Exception {
        LOGGER.info("Setting up quorum storage with health checks");
        
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
        assertNotNull(quorumStorage.getHealthCheckScheduler(), "Health check scheduler should be initialized");
        
        // Start the storage system
        quorumStorage.startup();
        
        // Verify quorum is available
        assertTrue(quorumStorage.getQuorumState().hasQuorum(), 
                  "Quorum should be available after startup");
        
        LOGGER.info("Quorum storage with health checks setup completed");
    }

    private void verifyInitialHealthCheckState() throws Exception {
        LOGGER.info("Verifying initial health check state");
        
        // Verify scheduler is running
        HealthCheckScheduler.SchedulerStats stats = quorumStorage.getHealthCheckScheduler().getStats();
        assertTrue(stats.isRunning());
        assertEquals(4, stats.getHealthCheckCount()); // 3 replica + 1 quorum check
        
        // Execute immediate health check
        Map<String, HealthCheckResult> results = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        
        assertNotNull(results);
        assertEquals(4, results.size());
        
        // All should be healthy initially
        for (HealthCheckResult result : results.values()) {
            assertTrue(result.isHealthy() || result.isUnknown(), 
                      "Initial health checks should be healthy: " + result);
        }
        
        LOGGER.info("Initial health check state verified");
    }

    private void performOperationsAndMonitorHealth() throws Exception {
        LOGGER.info("Performing operations and monitoring health");
        
        // Perform a series of operations
        for (int i = 0; i < 5; i++) {
            StreamRecordBatch recordBatch = createTestStreamRecord(2000L + i, i * 50, "monitor-data-" + i);
            AppendContext appendContext = createTestAppendContext();
            
            quorumStorage.append(appendContext, recordBatch).get(5, TimeUnit.SECONDS);
            
            // Check health after each operation
            Map<String, HealthCheckResult> results = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
            assertNotNull(results);
            assertEquals(4, results.size());
        }
        
        LOGGER.info("Operations and health monitoring completed");
    }

    private void testHealthCheckResponsesToChanges() throws Exception {
        LOGGER.info("Testing health check responses to system changes");
        
        // Get baseline health
        Map<String, HealthCheckResult> baselineResults = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        
        // Simulate some system stress with rapid operations
        for (int i = 0; i < 10; i++) {
            StreamRecordBatch recordBatch = createTestStreamRecord(4000L + i, i * 30, "stress-data-" + i);
            AppendContext appendContext = createTestAppendContext();
            
            quorumStorage.append(appendContext, recordBatch).get(2, TimeUnit.SECONDS);
        }
        
        // Check health after stress
        Map<String, HealthCheckResult> stressResults = quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        
        assertNotNull(stressResults);
        assertEquals(baselineResults.size(), stressResults.size());
        
        // Health checks should still be functioning
        for (HealthCheckResult result : stressResults.values()) {
            assertNotNull(result);
            assertTrue(result.getDurationMs() >= 0);
        }
        
        LOGGER.info("Health check responses to changes test completed");
    }

    private void verifyHealthCheckListenerNotifications() throws Exception {
        LOGGER.info("Verifying health check listener notifications");
        
        // Add listener and execute health checks
        quorumStorage.addHealthCheckListener(healthCheckListener);
        
        quorumStorage.executeHealthChecks().get(10, TimeUnit.SECONDS);
        Thread.sleep(500); // Allow listener events to process
        
        assertTrue(healthCheckListener.getResultCount() > 0);
        assertTrue(healthCheckListener.getExecutionCount() > 0);
        
        LOGGER.info("Health check listener notifications verified");
    }

    private void createTestConfigFile(File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("# Health Check Integration Test Configuration\n");
            writer.write("automq.s3.quorum.enabled=true\n");
            writer.write("automq.s3.quorum.size=3\n");
            writer.write("automq.s3.quorum.write.quorum.size=2\n");
            writer.write("automq.s3.quorum.read.quorum.size=1\n");
            writer.write("automq.s3.quorum.write.timeout.ms=30000\n");
            writer.write("automq.s3.quorum.read.timeout.ms=10000\n");
            
            // Configure 3 replicas for integration testing
            for (int i = 0; i < 3; i++) {
                writer.write(String.format("automq.s3.quorum.replica.%d.id=%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.region=health-test-region-%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.bucket=health-test-bucket-%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.endpoint=http://localhost:920%d\n", i, i));
                writer.write(String.format("automq.s3.quorum.replica.%d.role=%s\n", i, i == 0 ? "PRIMARY" : "SECONDARY"));
                writer.write(String.format("automq.s3.quorum.replica.%d.priority=%d\n", i, (3 - i) * 25));
            }
        }
        LOGGER.info("Created health check integration test configuration file: {}", file.getAbsolutePath());
    }

    private StreamRecordBatch createTestStreamRecord(long streamId, long baseOffset, String data) {
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
     * Test health check listener for integration testing
     */
    private static class TestHealthCheckListener implements HealthCheckScheduler.HealthCheckListener {
        private final AtomicInteger resultCount = new AtomicInteger(0);
        private final AtomicInteger statusChangeCount = new AtomicInteger(0);
        private final AtomicInteger executionCount = new AtomicInteger(0);

        @Override
        public void onHealthCheckResult(String healthCheckName, HealthCheckResult result) {
            resultCount.incrementAndGet();
            LOGGER.debug("Health check integration test listener received result for {}: {}", 
                       healthCheckName, result.getStatus());
        }

        @Override
        public void onHealthStatusChanged(String healthCheckName, 
                                        HealthCheckResult.Status previousStatus,
                                        HealthCheckResult.Status newStatus,
                                        HealthCheckResult result) {
            statusChangeCount.incrementAndGet();
            LOGGER.info("Health check integration test: Status changed for {} from {} to {}", 
                       healthCheckName, previousStatus, newStatus);
        }

        @Override
        public void onHealthCheckExecution(Map<String, HealthCheckResult> allResults, long durationMs) {
            executionCount.incrementAndGet();
            LOGGER.debug("Health check integration test: Execution completed with {} results ({}ms)", 
                       allResults.size(), durationMs);
        }

        public int getResultCount() {
            return resultCount.get();
        }

        public int getStatusChangeCount() {
            return statusChangeCount.get();
        }

        public int getExecutionCount() {
            return executionCount.get();
        }
    }
}