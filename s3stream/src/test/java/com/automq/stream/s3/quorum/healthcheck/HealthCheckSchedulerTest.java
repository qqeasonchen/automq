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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Test suite for HealthCheckScheduler functionality
 */
@DisplayName("Health Check Scheduler Tests")
public class HealthCheckSchedulerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckSchedulerTest.class);

    private HealthCheckScheduler scheduler;

    @BeforeEach
    void setUp() {
        LOGGER.info("Setting up HealthCheckScheduler test environment");
        scheduler = new HealthCheckScheduler(1000); // 1 second interval for testing
        LOGGER.info("HealthCheckScheduler test environment setup completed");
    }

    @AfterEach
    void tearDown() {
        if (scheduler != null) {
            scheduler.stop();
        }
        LOGGER.info("HealthCheckScheduler test environment cleaned up");
    }

    @Test
    @DisplayName("Test scheduler lifecycle")
    void testSchedulerLifecycle() {
        LOGGER.info("Testing scheduler lifecycle");

        HealthCheckScheduler.SchedulerStats stats = scheduler.getStats();
        assertFalse(stats.isRunning());
        assertEquals(0, stats.getHealthCheckCount());
        assertEquals(0, stats.getListenerCount());

        scheduler.start();
        stats = scheduler.getStats();
        assertTrue(stats.isRunning());

        scheduler.stop();
        stats = scheduler.getStats();
        assertFalse(stats.isRunning());

        LOGGER.info("Scheduler lifecycle test completed successfully");
    }

    @Test
    @DisplayName("Test adding and removing health checks")
    void testHealthCheckManagement() {
        LOGGER.info("Testing health check management");

        TestHealthCheck healthCheck1 = new TestHealthCheck("test-check-1", true);
        TestHealthCheck healthCheck2 = new TestHealthCheck("test-check-2", true);

        scheduler.addHealthCheck(healthCheck1);
        scheduler.addHealthCheck(healthCheck2);

        HealthCheckScheduler.SchedulerStats stats = scheduler.getStats();
        assertEquals(2, stats.getHealthCheckCount());

        scheduler.removeHealthCheck(healthCheck1);
        stats = scheduler.getStats();
        assertEquals(1, stats.getHealthCheckCount());

        scheduler.removeHealthCheck(healthCheck2);
        stats = scheduler.getStats();
        assertEquals(0, stats.getHealthCheckCount());

        LOGGER.info("Health check management test completed successfully");
    }

    @Test
    @DisplayName("Test adding and removing listeners")
    void testListenerManagement() {
        LOGGER.info("Testing listener management");

        TestHealthCheckListener listener1 = new TestHealthCheckListener();
        TestHealthCheckListener listener2 = new TestHealthCheckListener();

        scheduler.addListener(listener1);
        scheduler.addListener(listener2);

        HealthCheckScheduler.SchedulerStats stats = scheduler.getStats();
        assertEquals(2, stats.getListenerCount());

        scheduler.removeListener(listener1);
        stats = scheduler.getStats();
        assertEquals(1, stats.getListenerCount());

        scheduler.removeListener(listener2);
        stats = scheduler.getStats();
        assertEquals(0, stats.getListenerCount());

        LOGGER.info("Listener management test completed successfully");
    }

    @Test
    @DisplayName("Test immediate health check execution")
    void testImmediateExecution() throws Exception {
        LOGGER.info("Testing immediate health check execution");

        TestHealthCheck healthyCheck = new TestHealthCheck("healthy-check", true);
        TestHealthCheck unhealthyCheck = new TestHealthCheck("unhealthy-check", false);

        scheduler.addHealthCheck(healthyCheck);
        scheduler.addHealthCheck(unhealthyCheck);

        Map<String, HealthCheckResult> results = scheduler.executeNow().get(5, TimeUnit.SECONDS);

        assertNotNull(results);
        assertEquals(2, results.size());

        HealthCheckResult healthyResult = results.get("healthy-check");
        assertNotNull(healthyResult);
        assertTrue(healthyResult.isHealthy());

        HealthCheckResult unhealthyResult = results.get("unhealthy-check");
        assertNotNull(unhealthyResult);
        assertFalse(unhealthyResult.isHealthy());

        LOGGER.info("Immediate execution test completed successfully");
    }

    @Test
    @DisplayName("Test periodic health check execution")
    void testPeriodicExecution() throws Exception {
        LOGGER.info("Testing periodic health check execution");

        TestHealthCheck healthCheck = new TestHealthCheck("periodic-check", true);
        TestHealthCheckListener listener = new TestHealthCheckListener();

        scheduler.addHealthCheck(healthCheck);
        scheduler.addListener(listener);

        scheduler.start();

        // Wait for at least 2 executions
        assertTrue(listener.waitForExecutions(2, 5000), "Should have at least 2 executions");

        assertTrue(listener.getExecutionCount() >= 2);
        assertTrue(listener.getResultCount() >= 2);

        HealthCheckScheduler.SchedulerStats stats = scheduler.getStats();
        assertTrue(stats.getExecutionCount() >= 1);
        assertTrue(stats.getLastExecutionTime() > 0);

        LOGGER.info("Periodic execution test completed successfully");
    }

    @Test
    @DisplayName("Test health status change detection")
    void testStatusChangeDetection() throws Exception {
        LOGGER.info("Testing health status change detection");

        TestHealthCheck changingCheck = new TestHealthCheck("changing-check", true);
        TestHealthCheckListener listener = new TestHealthCheckListener();

        scheduler.addHealthCheck(changingCheck);
        scheduler.addListener(listener);

        // Execute first time (healthy)
        scheduler.executeNow().get(5, TimeUnit.SECONDS);
        assertEquals(1, listener.getResultCount());
        assertEquals(0, listener.getStatusChangeCount());

        // Change to unhealthy and execute again
        changingCheck.setHealthy(false);
        scheduler.executeNow().get(5, TimeUnit.SECONDS);
        assertEquals(2, listener.getResultCount());
        assertEquals(1, listener.getStatusChangeCount());

        // Change back to healthy and execute again
        changingCheck.setHealthy(true);
        scheduler.executeNow().get(5, TimeUnit.SECONDS);
        assertEquals(3, listener.getResultCount());
        assertEquals(2, listener.getStatusChangeCount());

        LOGGER.info("Status change detection test completed successfully");
    }

    @Test
    @DisplayName("Test health check result retrieval")
    void testResultRetrieval() throws Exception {
        LOGGER.info("Testing health check result retrieval");

        TestHealthCheck healthCheck = new TestHealthCheck("result-check", true);
        scheduler.addHealthCheck(healthCheck);

        // No results initially
        Map<String, HealthCheckResult> initialResults = scheduler.getAllLastResults();
        assertTrue(initialResults.isEmpty());

        HealthCheckResult initialResult = scheduler.getLastResult("result-check");
        assertTrue(initialResult == null);

        // Execute and check results
        scheduler.executeNow().get(5, TimeUnit.SECONDS);

        Map<String, HealthCheckResult> results = scheduler.getAllLastResults();
        assertEquals(1, results.size());

        HealthCheckResult result = scheduler.getLastResult("result-check");
        assertNotNull(result);
        assertTrue(result.isHealthy());

        LOGGER.info("Result retrieval test completed successfully");
    }

    @Test
    @DisplayName("Test disabled health check handling")
    void testDisabledHealthChecks() throws Exception {
        LOGGER.info("Testing disabled health check handling");

        TestHealthCheck enabledCheck = new TestHealthCheck("enabled-check", true, true);
        TestHealthCheck disabledCheck = new TestHealthCheck("disabled-check", true, false);

        scheduler.addHealthCheck(enabledCheck);
        scheduler.addHealthCheck(disabledCheck);

        Map<String, HealthCheckResult> results = scheduler.executeNow().get(5, TimeUnit.SECONDS);

        // Only enabled check should be executed
        assertEquals(1, results.size());
        assertTrue(results.containsKey("enabled-check"));
        assertFalse(results.containsKey("disabled-check"));

        LOGGER.info("Disabled health check handling test completed successfully");
    }

    @Test
    @DisplayName("Test health check exceptions")
    void testHealthCheckExceptions() throws Exception {
        LOGGER.info("Testing health check exceptions");

        TestHealthCheck exceptionCheck = new TestHealthCheck("exception-check", true) {
            @Override
            public CompletableFuture<HealthCheckResult> check() {
                throw new RuntimeException("Test exception");
            }
        };

        scheduler.addHealthCheck(exceptionCheck);

        Map<String, HealthCheckResult> results = scheduler.executeNow().get(5, TimeUnit.SECONDS);

        assertEquals(1, results.size());
        HealthCheckResult result = results.get("exception-check");
        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertTrue(result.getMessage().contains("execution failed"));

        LOGGER.info("Health check exceptions test completed successfully");
    }

    @Test
    @DisplayName("Test concurrent health check execution")
    void testConcurrentExecution() throws Exception {
        LOGGER.info("Testing concurrent health check execution");

        // Add multiple health checks with different execution times
        for (int i = 0; i < 5; i++) {
            final int index = i;
            TestHealthCheck check = new TestHealthCheck("concurrent-check-" + i, true) {
                @Override
                public CompletableFuture<HealthCheckResult> check() {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(100 + index * 10); // Variable delay
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return HealthCheckResult.healthy("Check " + index + " completed");
                    });
                }
            };
            scheduler.addHealthCheck(check);
        }

        long startTime = System.currentTimeMillis();
        Map<String, HealthCheckResult> results = scheduler.executeNow().get(10, TimeUnit.SECONDS);
        long duration = System.currentTimeMillis() - startTime;

        assertEquals(5, results.size());
        // Should complete faster than sequential execution (sum of delays)
        assertTrue(duration < 500); // Should be much less than 100+110+120+130+140 = 600ms

        for (HealthCheckResult result : results.values()) {
            assertTrue(result.isHealthy());
        }

        LOGGER.info("Concurrent execution test completed successfully");
    }

    // Helper classes for testing

    private static class TestHealthCheck implements HealthCheck {
        private final String name;
        private volatile boolean healthy;
        private final boolean enabled;

        public TestHealthCheck(String name, boolean healthy) {
            this(name, healthy, true);
        }

        public TestHealthCheck(String name, boolean healthy, boolean enabled) {
            this.name = name;
            this.healthy = healthy;
            this.enabled = enabled;
        }

        public void setHealthy(boolean healthy) {
            this.healthy = healthy;
        }

        @Override
        public CompletableFuture<HealthCheckResult> check() {
            return CompletableFuture.completedFuture(
                healthy ? HealthCheckResult.healthy(name + " is healthy") 
                        : HealthCheckResult.unhealthy(name + " is unhealthy")
            );
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public long getTimeoutMs() {
            return 5000;
        }

        @Override
        public boolean isEnabled() {
            return enabled;
        }
    }

    private static class TestHealthCheckListener implements HealthCheckScheduler.HealthCheckListener {
        private final AtomicInteger resultCount = new AtomicInteger(0);
        private final AtomicInteger statusChangeCount = new AtomicInteger(0);
        private final AtomicInteger executionCount = new AtomicInteger(0);
        private final CountDownLatch executionLatch = new CountDownLatch(1);

        @Override
        public void onHealthCheckResult(String healthCheckName, HealthCheckResult result) {
            resultCount.incrementAndGet();
        }

        @Override
        public void onHealthStatusChanged(String healthCheckName,
                                        HealthCheckResult.Status previousStatus,
                                        HealthCheckResult.Status newStatus,
                                        HealthCheckResult result) {
            statusChangeCount.incrementAndGet();
        }

        @Override
        public void onHealthCheckExecution(Map<String, HealthCheckResult> allResults, long durationMs) {
            executionCount.incrementAndGet();
            executionLatch.countDown();
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

        public boolean waitForExecutions(int expectedCount, long timeoutMs) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            while (executionCount.get() < expectedCount && 
                   (System.currentTimeMillis() - startTime) < timeoutMs) {
                Thread.sleep(100);
            }
            return executionCount.get() >= expectedCount;
        }
    }
}