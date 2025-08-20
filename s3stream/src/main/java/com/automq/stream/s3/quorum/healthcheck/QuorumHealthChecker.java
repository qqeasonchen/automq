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

import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.metrics.QuorumMetrics;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Health checker for the overall quorum system
 * Evaluates quorum-level health including consensus capability and system stability
 */
public class QuorumHealthChecker implements HealthCheck {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumHealthChecker.class);
    
    private final QuorumConfig quorumConfig;
    private final QuorumState quorumState;
    private final QuorumMetrics quorumMetrics;
    private final List<ReplicaHealthChecker> replicaHealthCheckers;
    private final long timeoutMs;
    private final boolean enabled;
    
    // Quorum health thresholds
    private static final double MIN_QUORUM_HEALTH_RATIO = 0.67; // At least 2/3 replicas healthy
    private static final double MIN_QUORUM_AVAILABILITY = 0.99; // 99% minimum availability
    private static final double MIN_OVERALL_SUCCESS_RATE = 0.95; // 95% minimum success rate
    private static final long MAX_QUORUM_LOSS_EVENTS = 3; // Maximum tolerable quorum loss events
    private static final long MAX_EMERGENCY_RECOVERIES = 2; // Maximum emergency recoveries
    
    public QuorumHealthChecker(QuorumConfig quorumConfig,
                              QuorumState quorumState,
                              QuorumMetrics quorumMetrics,
                              List<ReplicaHealthChecker> replicaHealthCheckers) {
        this(quorumConfig, quorumState, quorumMetrics, replicaHealthCheckers, 15000, true); // 15 second timeout
    }
    
    public QuorumHealthChecker(QuorumConfig quorumConfig,
                              QuorumState quorumState,
                              QuorumMetrics quorumMetrics,
                              List<ReplicaHealthChecker> replicaHealthCheckers,
                              long timeoutMs,
                              boolean enabled) {
        this.quorumConfig = quorumConfig;
        this.quorumState = quorumState;
        this.quorumMetrics = quorumMetrics;
        this.replicaHealthCheckers = replicaHealthCheckers;
        this.timeoutMs = timeoutMs;
        this.enabled = enabled;
    }
    
    @Override
    public CompletableFuture<HealthCheckResult> check() {
        if (!enabled) {
            return CompletableFuture.completedFuture(
                HealthCheckResult.builder()
                    .status(HealthCheckResult.Status.UNKNOWN)
                    .message("Quorum health check disabled")
                    .build()
            );
        }
        
        long startTime = System.currentTimeMillis();
        
        LOGGER.debug("Starting quorum health check");
        
        return performQuorumHealthCheck(startTime)
            .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .handle((result, throwable) -> {
                long duration = System.currentTimeMillis() - startTime;
                
                if (throwable != null) {
                    LOGGER.warn("Quorum health check timed out", throwable);
                    return HealthCheckResult.timeout(
                        "Quorum health check timeout",
                        duration
                    );
                }
                
                return result.durationMs(duration).build();
            });
    }
    
    private CompletableFuture<HealthCheckResult.Builder> performQuorumHealthCheck(long startTime) {
        HealthCheckResult.Builder resultBuilder = HealthCheckResult.builder();
        
        try {
            // Check 1: Quorum state and consensus capability
            QuorumStateHealth stateHealth = analyzeQuorumState();
            
            // Check 2: System metrics analysis
            SystemMetricsHealth metricsHealth = analyzeSystemMetrics();
            
            // Check 3: Individual replica health aggregation
            CompletableFuture<ReplicaAggregateHealth> replicaHealthFuture = aggregateReplicaHealth();
            
            return replicaHealthFuture.thenApply(replicaHealth -> {
                // Combine all health assessments
                boolean isHealthy = stateHealth.isHealthy && metricsHealth.isHealthy && replicaHealth.isHealthy;
                
                resultBuilder
                    .status(isHealthy ? HealthCheckResult.Status.HEALTHY : HealthCheckResult.Status.UNHEALTHY)
                    .message(buildQuorumHealthMessage(isHealthy, stateHealth, metricsHealth, replicaHealth))
                    .detail("quorumSize", quorumConfig.getQuorumSize())
                    .detail("writeQuorumSize", quorumConfig.getWriteQuorumSize())
                    .detail("readQuorumSize", quorumConfig.getReadQuorumSize())
                    .detail("hasQuorum", stateHealth.hasQuorum)
                    .detail("quorumHealthRatio", metricsHealth.healthRatio)
                    .detail("quorumAvailability", metricsHealth.availability)
                    .detail("overallSuccessRate", metricsHealth.successRate)
                    .detail("healthyReplicas", replicaHealth.healthyCount)
                    .detail("unhealthyReplicas", replicaHealth.unhealthyCount)
                    .detail("timeoutReplicas", replicaHealth.timeoutCount)
                    .detail("unknownReplicas", replicaHealth.unknownCount)
                    .detail("stateHealthy", stateHealth.isHealthy)
                    .detail("metricsHealthy", metricsHealth.isHealthy)
                    .detail("replicasHealthy", replicaHealth.isHealthy);
                
                LOGGER.info("Quorum health check completed: {} (healthy replicas: {}/{}, has quorum: {}, availability: {:.1f}%)",
                          isHealthy ? "HEALTHY" : "UNHEALTHY",
                          replicaHealth.healthyCount, quorumConfig.getQuorumSize(),
                          stateHealth.hasQuorum, metricsHealth.availability * 100);
                
                return resultBuilder;
            });
            
        } catch (Exception e) {
            LOGGER.error("Quorum health check failed", e);
            return CompletableFuture.completedFuture(
                resultBuilder
                    .status(HealthCheckResult.Status.UNHEALTHY)
                    .message("Quorum health check exception: " + e.getMessage())
                    .exception(e)
            );
        }
    }
    
    private QuorumStateHealth analyzeQuorumState() {
        boolean hasQuorum = quorumState != null && quorumState.hasQuorum();
        boolean isHealthy = hasQuorum;
        
        String summary = hasQuorum ? 
                        "Quorum consensus available" : 
                        "Quorum consensus lost - insufficient healthy replicas";
        
        LOGGER.debug("Quorum state analysis: {} (has quorum: {})", 
                   isHealthy ? "HEALTHY" : "UNHEALTHY", hasQuorum);
        
        return new QuorumStateHealth(isHealthy, hasQuorum, summary);
    }
    
    private SystemMetricsHealth analyzeSystemMetrics() {
        if (quorumMetrics == null) {
            return new SystemMetricsHealth(true, 1.0, 1.0, 1.0, 0, 0, "No metrics data available");
        }
        
        double healthRatio = quorumMetrics.getQuorumHealthRatio();
        double availability = quorumMetrics.getQuorumAvailability();
        double successRate = calculateOverallSuccessRate();
        long quorumLossEvents = quorumMetrics.getQuorumLossEvents();
        long emergencyRecoveries = quorumMetrics.getEmergencyRecoveriesTotal();
        
        boolean metricsHealthy = true;
        StringBuilder issues = new StringBuilder();
        
        // Check quorum health ratio
        if (healthRatio < MIN_QUORUM_HEALTH_RATIO) {
            metricsHealthy = false;
            issues.append("Low quorum health ratio: ").append(String.format("%.1f%%", healthRatio * 100)).append("; ");
        }
        
        // Check availability
        if (availability < MIN_QUORUM_AVAILABILITY) {
            metricsHealthy = false;
            issues.append("Low availability: ").append(String.format("%.2f%%", availability * 100)).append("; ");
        }
        
        // Check overall success rate
        if (successRate < MIN_OVERALL_SUCCESS_RATE) {
            metricsHealthy = false;
            issues.append("Low success rate: ").append(String.format("%.1f%%", successRate * 100)).append("; ");
        }
        
        // Check quorum loss events
        if (quorumLossEvents > MAX_QUORUM_LOSS_EVENTS) {
            metricsHealthy = false;
            issues.append("Too many quorum loss events: ").append(quorumLossEvents).append("; ");
        }
        
        // Check emergency recoveries
        if (emergencyRecoveries > MAX_EMERGENCY_RECOVERIES) {
            metricsHealthy = false;
            issues.append("Too many emergency recoveries: ").append(emergencyRecoveries).append("; ");
        }
        
        String summary = metricsHealthy ? 
                        "System metrics healthy" : 
                        "System metrics issues: " + issues.toString();
        
        LOGGER.debug("System metrics analysis: {} (health={:.1f}%, availability={:.2f}%, success={:.1f}%)",
                   metricsHealthy ? "HEALTHY" : "UNHEALTHY",
                   healthRatio * 100, availability * 100, successRate * 100);
        
        return new SystemMetricsHealth(metricsHealthy, healthRatio, availability, successRate, 
                                     quorumLossEvents, emergencyRecoveries, summary);
    }
    
    private double calculateOverallSuccessRate() {
        long totalSuccessful = quorumMetrics.getWriteRequestsSuccessful() + quorumMetrics.getReadRequestsSuccessful();
        long totalRequests = quorumMetrics.getWriteRequestsTotal() + quorumMetrics.getReadRequestsTotal();
        return totalRequests > 0 ? (double) totalSuccessful / totalRequests : 1.0;
    }
    
    private CompletableFuture<ReplicaAggregateHealth> aggregateReplicaHealth() {
        if (replicaHealthCheckers == null || replicaHealthCheckers.isEmpty()) {
            LOGGER.debug("No replica health checkers available");
            return CompletableFuture.completedFuture(
                new ReplicaAggregateHealth(true, 0, 0, 0, 0, "No replica health checkers configured")
            );
        }
        
        // Run all replica health checks in parallel
        List<CompletableFuture<HealthCheckResult>> healthCheckFutures = replicaHealthCheckers.stream()
            .map(HealthCheck::check)
            .collect(Collectors.toList());
        
        return CompletableFuture.allOf(healthCheckFutures.toArray(new CompletableFuture[0]))
            .thenApply(unused -> {
                int healthyCount = 0;
                int unhealthyCount = 0;
                int timeoutCount = 0;
                int unknownCount = 0;
                
                for (CompletableFuture<HealthCheckResult> future : healthCheckFutures) {
                    try {
                        HealthCheckResult result = future.get();
                        switch (result.getStatus()) {
                            case HEALTHY:
                                healthyCount++;
                                break;
                            case UNHEALTHY:
                                unhealthyCount++;
                                break;
                            case TIMEOUT:
                                timeoutCount++;
                                break;
                            case UNKNOWN:
                                unknownCount++;
                                break;
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Failed to get replica health check result", e);
                        unknownCount++;
                    }
                }
                
                // Consider the replica aggregate healthy if we have enough healthy replicas for quorum
                boolean replicasHealthy = healthyCount >= quorumConfig.getWriteQuorumSize();
                
                String summary = String.format("Replica health: %d healthy, %d unhealthy, %d timeout, %d unknown",
                                              healthyCount, unhealthyCount, timeoutCount, unknownCount);
                
                LOGGER.debug("Replica aggregate health: {} ({})", 
                           replicasHealthy ? "HEALTHY" : "UNHEALTHY", summary);
                
                return new ReplicaAggregateHealth(replicasHealthy, healthyCount, unhealthyCount, 
                                                timeoutCount, unknownCount, summary);
            });
    }
    
    private String buildQuorumHealthMessage(boolean isHealthy, QuorumStateHealth stateHealth,
                                          SystemMetricsHealth metricsHealth, ReplicaAggregateHealth replicaHealth) {
        if (isHealthy) {
            return String.format("Quorum system is healthy - %s, %s, %s",
                                stateHealth.summary, metricsHealth.summary, replicaHealth.summary);
        } else {
            StringBuilder message = new StringBuilder();
            message.append("Quorum system is unhealthy - ");
            
            if (!stateHealth.isHealthy) {
                message.append(stateHealth.summary).append(" ");
            }
            if (!metricsHealth.isHealthy) {
                message.append(metricsHealth.summary).append(" ");
            }
            if (!replicaHealth.isHealthy) {
                message.append(replicaHealth.summary);
            }
            
            return message.toString().trim();
        }
    }
    
    @Override
    public String getName() {
        return "QuorumHealthCheck";
    }
    
    @Override
    public long getTimeoutMs() {
        return timeoutMs;
    }
    
    @Override
    public boolean isEnabled() {
        return enabled;
    }
    
    // Helper classes for organizing health status results
    
    private static class QuorumStateHealth {
        final boolean isHealthy;
        final boolean hasQuorum;
        final String summary;
        
        QuorumStateHealth(boolean isHealthy, boolean hasQuorum, String summary) {
            this.isHealthy = isHealthy;
            this.hasQuorum = hasQuorum;
            this.summary = summary;
        }
    }
    
    private static class SystemMetricsHealth {
        final boolean isHealthy;
        final double healthRatio;
        final double availability;
        final double successRate;
        final long quorumLossEvents;
        final long emergencyRecoveries;
        final String summary;
        
        SystemMetricsHealth(boolean isHealthy, double healthRatio, double availability, double successRate,
                           long quorumLossEvents, long emergencyRecoveries, String summary) {
            this.isHealthy = isHealthy;
            this.healthRatio = healthRatio;
            this.availability = availability;
            this.successRate = successRate;
            this.quorumLossEvents = quorumLossEvents;
            this.emergencyRecoveries = emergencyRecoveries;
            this.summary = summary;
        }
    }
    
    private static class ReplicaAggregateHealth {
        final boolean isHealthy;
        final int healthyCount;
        final int unhealthyCount;
        final int timeoutCount;
        final int unknownCount;
        final String summary;
        
        ReplicaAggregateHealth(boolean isHealthy, int healthyCount, int unhealthyCount,
                              int timeoutCount, int unknownCount, String summary) {
            this.isHealthy = isHealthy;
            this.healthyCount = healthyCount;
            this.unhealthyCount = unhealthyCount;
            this.timeoutCount = timeoutCount;
            this.unknownCount = unknownCount;
            this.summary = summary;
        }
    }
}