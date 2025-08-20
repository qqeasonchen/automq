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
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.metrics.QuorumMetrics;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.when;

/**
 * Test suite for QuorumHealthChecker functionality
 */
@DisplayName("Quorum Health Checker Tests")
public class QuorumHealthCheckerTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumHealthCheckerTest.class);

    @Mock
    private QuorumState mockQuorumState;

    private QuorumConfig quorumConfig;
    private QuorumMetrics quorumMetrics;
    private List<ReplicaHealthChecker> mockReplicaHealthCheckers;
    private QuorumHealthChecker quorumHealthChecker;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        LOGGER.info("Setting up QuorumHealthChecker test environment");

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

        quorumMetrics = new QuorumMetrics(quorumConfig);

        // Create mock replica health checkers
        mockReplicaHealthCheckers = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            mockReplicaHealthCheckers.add(createMockReplicaHealthChecker(i));
        }

        quorumHealthChecker = new QuorumHealthChecker(
            quorumConfig, mockQuorumState, quorumMetrics, mockReplicaHealthCheckers);

        LOGGER.info("QuorumHealthChecker test environment setup completed");
    }

    private ReplicaHealthChecker createMockReplicaHealthChecker(int replicaId) {
        return new ReplicaHealthChecker(
            quorumConfig.getReplicaConfigs().get(replicaId),
            null, // No ObjectStorage needed for mock
            quorumMetrics.getReplicaMetrics(replicaId)
        ) {
            @Override
            public CompletableFuture<HealthCheckResult> check() {
                // Default to healthy unless overridden in tests
                return CompletableFuture.completedFuture(
                    HealthCheckResult.healthy("Mock replica " + replicaId + " is healthy")
                );
            }
        };
    }

    @Test
    @DisplayName("Test healthy quorum system")
    void testHealthyQuorumSystem() throws Exception {
        LOGGER.info("Testing healthy quorum system");

        // Setup healthy system state
        when(mockQuorumState.hasQuorum()).thenReturn(true);

        // Setup healthy metrics
        quorumMetrics.recordWriteRequest();
        quorumMetrics.recordWriteSuccess(100, 1024);
        quorumMetrics.recordReadRequest();
        quorumMetrics.recordReadSuccess(50, 512);
        quorumMetrics.updateQuorumHealth(3, true);

        // All replica health checkers return healthy
        for (int i = 0; i < mockReplicaHealthCheckers.size(); i++) {
            final int replicaId = i;
            mockReplicaHealthCheckers.set(i, new ReplicaHealthChecker(
                quorumConfig.getReplicaConfigs().get(i),
                null,
                quorumMetrics.getReplicaMetrics(i)
            ) {
                @Override
                public CompletableFuture<HealthCheckResult> check() {
                    return CompletableFuture.completedFuture(
                        HealthCheckResult.healthy("Replica " + replicaId + " is healthy")
                    );
                }
            });
        }

        HealthCheckResult result = quorumHealthChecker.check().get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isHealthy());
        assertEquals(HealthCheckResult.Status.HEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("Quorum system is healthy"));
        assertTrue((Boolean) result.getDetail("hasQuorum"));
        assertTrue((Boolean) result.getDetail("stateHealthy"));
        assertTrue((Boolean) result.getDetail("metricsHealthy"));
        assertTrue((Boolean) result.getDetail("replicasHealthy"));
        assertEquals(3, result.getDetail("healthyReplicas"));
        assertEquals(0, result.getDetail("unhealthyReplicas"));

        LOGGER.info("Healthy quorum system test completed successfully");
    }

    @Test
    @DisplayName("Test unhealthy quorum due to lost quorum consensus")
    void testUnhealthyQuorumLostConsensus() throws Exception {
        LOGGER.info("Testing unhealthy quorum due to lost consensus");

        // Setup lost quorum state
        when(mockQuorumState.hasQuorum()).thenReturn(false);

        // Setup good metrics otherwise
        quorumMetrics.recordWriteRequest();
        quorumMetrics.recordWriteSuccess(100, 1024);
        quorumMetrics.updateQuorumHealth(1, false); // Only 1 healthy replica

        HealthCheckResult result = quorumHealthChecker.check().get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("Quorum consensus lost"));
        assertFalse((Boolean) result.getDetail("hasQuorum"));
        assertFalse((Boolean) result.getDetail("stateHealthy"));

        LOGGER.info("Unhealthy quorum lost consensus test completed successfully");
    }

    @Test
    @DisplayName("Test unhealthy quorum due to poor system metrics")
    void testUnhealthyQuorumPoorMetrics() throws Exception {
        LOGGER.info("Testing unhealthy quorum due to poor system metrics");

        // Setup good quorum state
        when(mockQuorumState.hasQuorum()).thenReturn(true);

        // Setup poor system metrics
        // Low success rate
        for (int i = 0; i < 5; i++) {
            quorumMetrics.recordWriteRequest();
            quorumMetrics.recordWriteFailure(200);
        }
        for (int i = 0; i < 5; i++) {
            quorumMetrics.recordReadRequest();
            quorumMetrics.recordReadFailure(150);
        }

        // Low availability
        quorumMetrics.updateQuorumHealth(1, false); // Low health ratio
        
        // Too many quorum loss events
        for (int i = 0; i < 5; i++) {
            quorumMetrics.recordQuorumLoss();
        }

        // Too many emergency recoveries
        for (int i = 0; i < 4; i++) {
            quorumMetrics.recordEmergencyRecovery();
        }

        HealthCheckResult result = quorumHealthChecker.check().get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("System metrics issues"));
        assertFalse((Boolean) result.getDetail("metricsHealthy"));

        // Check specific metric values
        Double quorumHealthRatio = (Double) result.getDetail("quorumHealthRatio");
        Double overallSuccessRate = (Double) result.getDetail("overallSuccessRate");
        assertNotNull(quorumHealthRatio);
        assertNotNull(overallSuccessRate);
        assertTrue(quorumHealthRatio < 0.67); // Below threshold
        assertTrue(overallSuccessRate < 0.95); // Below threshold

        LOGGER.info("Unhealthy quorum poor metrics test completed successfully");
    }

    @Test
    @DisplayName("Test unhealthy quorum due to unhealthy replicas")
    void testUnhealthyQuorumUnhealthyReplicas() throws Exception {
        LOGGER.info("Testing unhealthy quorum due to unhealthy replicas");

        // Setup good quorum state and metrics
        when(mockQuorumState.hasQuorum()).thenReturn(true);
        quorumMetrics.recordWriteRequest();
        quorumMetrics.recordWriteSuccess(100, 1024);
        quorumMetrics.updateQuorumHealth(3, true);

        // Setup unhealthy replicas (only 1 healthy, need 2 for write quorum)
        for (int i = 0; i < mockReplicaHealthCheckers.size(); i++) {
            final int replicaId = i;
            final boolean isHealthy = i == 0; // Only first replica is healthy
            
            mockReplicaHealthCheckers.set(i, new ReplicaHealthChecker(
                quorumConfig.getReplicaConfigs().get(i),
                null,
                quorumMetrics.getReplicaMetrics(i)
            ) {
                @Override
                public CompletableFuture<HealthCheckResult> check() {
                    if (isHealthy) {
                        return CompletableFuture.completedFuture(
                            HealthCheckResult.healthy("Replica " + replicaId + " is healthy")
                        );
                    } else {
                        return CompletableFuture.completedFuture(
                            HealthCheckResult.unhealthy("Replica " + replicaId + " is failing")
                        );
                    }
                }
            });
        }

        HealthCheckResult result = quorumHealthChecker.check().get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertFalse(result.isHealthy());
        assertEquals(HealthCheckResult.Status.UNHEALTHY, result.getStatus());
        assertTrue(result.getMessage().contains("1 healthy, 2 unhealthy"));
        assertFalse((Boolean) result.getDetail("replicasHealthy"));
        assertEquals(1, result.getDetail("healthyReplicas"));
        assertEquals(2, result.getDetail("unhealthyReplicas"));

        LOGGER.info("Unhealthy quorum unhealthy replicas test completed successfully");
    }

    @Test
    @DisplayName("Test quorum health check with timeout replicas")
    void testQuorumHealthCheckTimeouts() throws Exception {
        LOGGER.info("Testing quorum health check with timeout replicas");

        // Setup good quorum state and metrics
        when(mockQuorumState.hasQuorum()).thenReturn(true);
        quorumMetrics.recordWriteRequest();
        quorumMetrics.recordWriteSuccess(100, 1024);
        quorumMetrics.updateQuorumHealth(3, true);

        // Setup mixed replica health: 2 healthy, 1 timeout
        for (int i = 0; i < mockReplicaHealthCheckers.size(); i++) {
            final int replicaId = i;
            final boolean isTimeout = i == 2; // Last replica times out
            
            mockReplicaHealthCheckers.set(i, new ReplicaHealthChecker(
                quorumConfig.getReplicaConfigs().get(i),
                null,
                quorumMetrics.getReplicaMetrics(i)
            ) {
                @Override
                public CompletableFuture<HealthCheckResult> check() {
                    if (isTimeout) {
                        return CompletableFuture.completedFuture(
                            HealthCheckResult.timeout("Replica " + replicaId + " timed out", 5000)
                        );
                    } else {
                        return CompletableFuture.completedFuture(
                            HealthCheckResult.healthy("Replica " + replicaId + " is healthy")
                        );
                    }
                }
            });
        }

        HealthCheckResult result = quorumHealthChecker.check().get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isHealthy()); // Should still be healthy with 2/3 replicas healthy
        assertEquals(HealthCheckResult.Status.HEALTHY, result.getStatus());
        assertEquals(2, result.getDetail("healthyReplicas"));
        assertEquals(0, result.getDetail("unhealthyReplicas"));
        assertEquals(1, result.getDetail("timeoutReplicas"));

        LOGGER.info("Quorum health check timeouts test completed successfully");
    }

    @Test
    @DisplayName("Test disabled quorum health check")
    void testDisabledQuorumHealthCheck() throws Exception {
        LOGGER.info("Testing disabled quorum health check");

        // Create disabled health checker
        QuorumHealthChecker disabledChecker = new QuorumHealthChecker(
            quorumConfig, mockQuorumState, quorumMetrics, mockReplicaHealthCheckers, 10000, false);

        HealthCheckResult result = disabledChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isUnknown());
        assertEquals(HealthCheckResult.Status.UNKNOWN, result.getStatus());
        assertTrue(result.getMessage().contains("disabled"));

        LOGGER.info("Disabled quorum health check test completed successfully");
    }

    @Test
    @DisplayName("Test quorum health check timeout")
    void testQuorumHealthCheckTimeout() throws Exception {
        LOGGER.info("Testing quorum health check timeout");

        // Create health checker with short timeout
        QuorumHealthChecker shortTimeoutChecker = new QuorumHealthChecker(
            quorumConfig, mockQuorumState, quorumMetrics, mockReplicaHealthCheckers, 100, true);

        // Setup slow replica health checkers
        for (int i = 0; i < mockReplicaHealthCheckers.size(); i++) {
            final int replicaId = i;
            mockReplicaHealthCheckers.set(i, new ReplicaHealthChecker(
                quorumConfig.getReplicaConfigs().get(i),
                null,
                quorumMetrics.getReplicaMetrics(i)
            ) {
                @Override
                public CompletableFuture<HealthCheckResult> check() {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            Thread.sleep(200); // Longer than timeout
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        return HealthCheckResult.healthy("Replica " + replicaId + " is healthy");
                    });
                }
            });
        }

        HealthCheckResult result = shortTimeoutChecker.check().get(5, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isTimeout());
        assertEquals(HealthCheckResult.Status.TIMEOUT, result.getStatus());
        assertTrue(result.getMessage().contains("timeout"));

        LOGGER.info("Quorum health check timeout test completed successfully");
    }

    @Test
    @DisplayName("Test quorum health check properties")
    void testQuorumHealthCheckProperties() {
        LOGGER.info("Testing quorum health check properties");

        assertEquals("QuorumHealthCheck", quorumHealthChecker.getName());
        assertEquals(15000, quorumHealthChecker.getTimeoutMs()); // Default timeout
        assertTrue(quorumHealthChecker.isEnabled()); // Default enabled

        // Test custom properties
        QuorumHealthChecker customChecker = new QuorumHealthChecker(
            quorumConfig, mockQuorumState, quorumMetrics, mockReplicaHealthCheckers, 20000, false);

        assertEquals("QuorumHealthCheck", customChecker.getName());
        assertEquals(20000, customChecker.getTimeoutMs());
        assertFalse(customChecker.isEnabled());

        LOGGER.info("Quorum health check properties test completed successfully");
    }

    @Test
    @DisplayName("Test quorum health check with no replica health checkers")
    void testQuorumHealthCheckNoReplicaCheckers() throws Exception {
        LOGGER.info("Testing quorum health check with no replica health checkers");

        // Create quorum health checker without replica checkers
        QuorumHealthChecker noReplicasChecker = new QuorumHealthChecker(
            quorumConfig, mockQuorumState, quorumMetrics, new ArrayList<>());

        // Setup good quorum state and metrics
        when(mockQuorumState.hasQuorum()).thenReturn(true);
        quorumMetrics.recordWriteRequest();
        quorumMetrics.recordWriteSuccess(100, 1024);
        quorumMetrics.updateQuorumHealth(3, true);

        HealthCheckResult result = noReplicasChecker.check().get(10, TimeUnit.SECONDS);

        assertNotNull(result);
        assertTrue(result.isHealthy()); // Should be healthy based on state and metrics only
        assertEquals(HealthCheckResult.Status.HEALTHY, result.getStatus());
        assertEquals(0, result.getDetail("healthyReplicas"));
        assertEquals(0, result.getDetail("unhealthyReplicas"));

        LOGGER.info("Quorum health check no replica checkers test completed successfully");
    }
}