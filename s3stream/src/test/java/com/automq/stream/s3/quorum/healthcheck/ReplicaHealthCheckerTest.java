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
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.metrics.ReplicaMetrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import org.mockito.Mockito;

/**
 * Test suite for ReplicaHealthChecker functionality
 */
@DisplayName("Replica Health Checker Tests")
public class ReplicaHealthCheckerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaHealthCheckerTest.class);

    @Mock
    private ObjectStorage mockObjectStorage;

    private ReplicaConfig replicaConfig;
    private ReplicaMetrics replicaMetrics;
    private ReplicaHealthChecker healthChecker;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        // Reset mocks to ensure clean state between tests
        Mockito.reset(mockObjectStorage);
        LOGGER.info("Setting up ReplicaHealthChecker test environment");

        Config testConfig = new Config();
        replicaConfig = ReplicaConfig.builder()
            .replicaId(0)
            .region("test-region")
            .bucket("test-bucket")
            .endpoint("http://localhost:9200")
            .accessKey("test-access-key")
            .secretKey("test-secret-key")
            .s3Config(testConfig)
            .role(ReplicaConfig.ReplicaRole.PRIMARY)
            .priority(100)
            .build();

        replicaMetrics = new ReplicaMetrics(0);
        healthChecker = new ReplicaHealthChecker(replicaConfig, mockObjectStorage, replicaMetrics);

        LOGGER.info("ReplicaHealthChecker test environment setup completed");
    }

    @Test
    @DisplayName("Test healthy replica check")
    void testHealthyReplicaCheck() throws Exception {
        LOGGER.info("Testing healthy replica check");

        // Setup healthy metrics
        replicaMetrics.recordWrite(true, 100);
        replicaMetrics.recordWrite(true, 120);
        replicaMetrics.recordRead(true, 50);
        replicaMetrics.recordRead(true, 60);

        // Mock successful connectivity
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));

        HealthCheckResult result = healthChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isHealthy());
        assertEquals(HealthCheckResult.Status.HEALTHY, result.getStatus());
        assertTrue(result.getDurationMs() >= 0);
        assertEquals(0, result.getDetail("replicaId"));
        assertEquals("test-region", result.getDetail("region"));
        assertEquals("test-bucket", result.getDetail("bucket"));
        assertTrue((Boolean) result.getDetail("performanceHealthy"));
        assertTrue((Boolean) result.getDetail("failureHealthy"));

        LOGGER.info("Healthy replica check test completed successfully");
    }

    @Test
    @DisplayName("Test unhealthy replica due to connectivity issues")
    void testUnhealthyReplicaConnectivity() throws Exception {
        LOGGER.info("Testing unhealthy replica due to connectivity issues");

        // Setup healthy metrics
        replicaMetrics.recordWrite(true, 100);
        replicaMetrics.recordRead(true, 50);

        // Mock connectivity failure
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Connection timeout")));

        HealthCheckResult result = healthChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("Connectivity check failed"));
        assertNotNull(result.getException());

        LOGGER.info("Unhealthy replica connectivity test completed successfully");
    }

    @Test
    @DisplayName("Test unhealthy replica due to poor performance metrics")
    void testUnhealthyReplicaPerformance() throws Exception {
        LOGGER.info("Testing unhealthy replica due to poor performance metrics");

        // Setup poor performance metrics
        // Low success rate (60%)
        replicaMetrics.recordWrite(true, 100);
        replicaMetrics.recordWrite(true, 120);
        replicaMetrics.recordWrite(false, 200);
        replicaMetrics.recordWrite(false, 180);
        replicaMetrics.recordWrite(false, 220);

        // High latency operations
        replicaMetrics.recordRead(true, 6000); // Above 5000ms threshold
        replicaMetrics.recordRead(true, 7000);

        // Mock successful connectivity
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));

        HealthCheckResult result = healthChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("Performance issues"));
        assertFalse((Boolean) result.getDetail("performanceHealthy"));

        LOGGER.info("Unhealthy replica performance test completed successfully");
    }

    @Test
    @DisplayName("Test unhealthy replica due to consecutive failures")
    void testUnhealthyReplicaConsecutiveFailures() throws Exception {
        LOGGER.info("Testing unhealthy replica due to consecutive failures");

        // Setup consecutive failures (more than 5)
        for (int i = 0; i < 7; i++) {
            replicaMetrics.recordWrite(false, 100);
        }

        // Mock successful connectivity
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));

        HealthCheckResult result = healthChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("consecutive failures"));
        assertFalse((Boolean) result.getDetail("failureHealthy"));
        assertEquals(7L, result.getDetail("consecutiveFailures"));

        LOGGER.info("Unhealthy replica consecutive failures test completed successfully");
    }

    @Test
    @DisplayName("Test health check timeout")
    void testHealthCheckTimeout() throws Exception {
        LOGGER.info("Testing health check timeout");

        // Create health checker with short timeout
        ReplicaHealthChecker shortTimeoutChecker = new ReplicaHealthChecker(
            replicaConfig, mockObjectStorage, replicaMetrics, 100, true); // 100ms timeout

        // Mock slow response
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.supplyAsync(() -> {
                try {
                    Thread.sleep(200); // 200ms delay, longer than timeout
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return java.util.List.of();
            }));

        HealthCheckResult result = shortTimeoutChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isTimeout());
        assertEquals(HealthCheckResult.Status.TIMEOUT, result.getStatus());
        assertTrue(result.getMessage().contains("timeout"));
        assertTrue(result.getDurationMs() >= 100); // Should be at least the timeout duration

        LOGGER.info("Health check timeout test completed successfully");
    }

    @Test
    @DisplayName("Test disabled health check")
    void testDisabledHealthCheck() throws Exception {
        LOGGER.info("Testing disabled health check");

        // Create disabled health checker
        ReplicaHealthChecker disabledChecker = new ReplicaHealthChecker(
            replicaConfig, mockObjectStorage, replicaMetrics, 5000, false); // disabled

        HealthCheckResult result = disabledChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isUnknown());
        assertEquals(HealthCheckResult.Status.UNKNOWN, result.getStatus());
        assertTrue(result.getMessage().contains("disabled"));

        LOGGER.info("Disabled health check test completed successfully");
    }

    @Test
    @DisplayName("Test health check with null metrics")
    void testHealthCheckWithNullMetrics() throws Exception {
        LOGGER.info("Testing health check with null metrics");

        // Create health checker without metrics
        ReplicaHealthChecker noMetricsChecker = new ReplicaHealthChecker(
            replicaConfig, mockObjectStorage, null);

        // Mock successful connectivity
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));

        HealthCheckResult result = noMetricsChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isHealthy()); // Should be healthy without metrics
        assertEquals(HealthCheckResult.Status.HEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("No metrics data available"));

        LOGGER.info("Health check with null metrics test completed successfully");
    }

    @Test
    @DisplayName("Test health check properties")
    void testHealthCheckProperties() {
        LOGGER.info("Testing health check properties");

        assertEquals("ReplicaHealthCheck-0", healthChecker.getName());
        assertEquals(10000, healthChecker.getTimeoutMs()); // Default timeout
        assertTrue(healthChecker.isEnabled()); // Default enabled

        // Test custom properties
        ReplicaHealthChecker customChecker = new ReplicaHealthChecker(
            replicaConfig, mockObjectStorage, replicaMetrics, 15000, false);

        assertEquals("ReplicaHealthCheck-0", customChecker.getName());
        assertEquals(15000, customChecker.getTimeoutMs());
        assertFalse(customChecker.isEnabled());

        LOGGER.info("Health check properties test completed successfully");
    }

    @Test
    @DisplayName("Test health check with mixed performance metrics")
    void testHealthCheckMixedPerformance() throws Exception {
        LOGGER.info("Testing health check with mixed performance metrics");

        // Setup mixed performance: good success rate but high latency
        // Use fewer low-latency operations to ensure overall average exceeds threshold
        replicaMetrics.recordWrite(true, 6000); // High latency write
        replicaMetrics.recordWrite(true, 7000); // High latency write  
        replicaMetrics.recordWrite(false, 5500); // One failure with high latency (still good success rate)

        // High latency reads
        replicaMetrics.recordRead(true, 8000); // Very high latency
        replicaMetrics.recordRead(true, 9000); // Very high latency

        // Mock successful connectivity
        when(mockObjectStorage.list(anyString()))
            .thenReturn(CompletableFuture.completedFuture(java.util.List.of()));

        HealthCheckResult result = healthChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy()); // Should be unhealthy due to high latency
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("High latency"));
        assertFalse((Boolean) result.getDetail("performanceHealthy"));
        assertTrue((Boolean) result.getDetail("failureHealthy")); // Failure status is still good

        Double averageLatency = (Double) result.getDetail("averageLatency");
        assertNotNull(averageLatency);
        assertTrue(averageLatency > 5000); // Should be above threshold

        LOGGER.info("Mixed performance health check test completed successfully");
    }
}