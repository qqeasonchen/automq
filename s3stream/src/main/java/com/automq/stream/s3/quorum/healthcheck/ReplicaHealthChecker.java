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

import com.automq.stream.s3.ObjectStorage;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.metrics.ReplicaMetrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Health checker for individual S3 replicas
 * Performs operational health checks on replica storage and connectivity
 */
public class ReplicaHealthChecker implements HealthCheck {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaHealthChecker.class);
    
    private final ReplicaConfig replicaConfig;
    private final ObjectStorage objectStorage;
    private final ReplicaMetrics replicaMetrics;
    private final long timeoutMs;
    private final boolean enabled;
    
    // Health check thresholds
    private static final double MIN_SUCCESS_RATE = 0.8; // 80% minimum success rate
    private static final long MAX_CONSECUTIVE_FAILURES = 5;
    private static final double MIN_UPTIME_RATIO = 0.95; // 95% minimum uptime
    private static final long MAX_AVERAGE_LATENCY_MS = 5000; // 5 seconds max average latency
    
    public ReplicaHealthChecker(ReplicaConfig replicaConfig, 
                               ObjectStorage objectStorage,
                               ReplicaMetrics replicaMetrics) {
        this(replicaConfig, objectStorage, replicaMetrics, 10000, true); // 10 second timeout
    }
    
    public ReplicaHealthChecker(ReplicaConfig replicaConfig, 
                               ObjectStorage objectStorage,
                               ReplicaMetrics replicaMetrics,
                               long timeoutMs,
                               boolean enabled) {
        this.replicaConfig = replicaConfig;
        this.objectStorage = objectStorage;
        this.replicaMetrics = replicaMetrics;
        this.timeoutMs = timeoutMs;
        this.enabled = enabled;
    }
    
    @Override
    public CompletableFuture<HealthCheckResult> check() {
        if (!enabled) {
            return CompletableFuture.completedFuture(
                HealthCheckResult.builder()
                    .status(HealthCheckResult.Status.UNKNOWN)
                    .message("Health check disabled for replica " + replicaConfig.getReplicaId())
                    .build()
            );
        }
        
        long startTime = System.currentTimeMillis();
        
        LOGGER.debug("Starting health check for replica {}", replicaConfig.getReplicaId());
        
        return performHealthCheck(startTime)
            .orTimeout(timeoutMs, TimeUnit.MILLISECONDS)
            .handle((result, throwable) -> {
                long duration = System.currentTimeMillis() - startTime;
                
                if (throwable != null) {
                    LOGGER.warn("Health check timed out for replica {}", replicaConfig.getReplicaId(), throwable);
                    return HealthCheckResult.timeout(
                        "Health check timeout for replica " + replicaConfig.getReplicaId(),
                        duration
                    );
                }
                
                return result.durationMs(duration).build();
            });
    }
    
    private CompletableFuture<HealthCheckResult.Builder> performHealthCheck(long startTime) {
        HealthCheckResult.Builder resultBuilder = HealthCheckResult.builder();
        
        try {
            // Check 1: Basic connectivity test
            CompletableFuture<Void> connectivityCheck = checkConnectivity();
            
            // Check 2: Performance metrics analysis
            PerformanceHealthStatus performanceStatus = analyzePerformanceMetrics();
            
            // Check 3: Recent failure analysis
            FailureHealthStatus failureStatus = analyzeRecentFailures();
            
            return connectivityCheck.thenApply(unused -> {
                // Combine all health check results
                boolean isHealthy = performanceStatus.isHealthy && failureStatus.isHealthy;
                
                resultBuilder
                    .status(isHealthy ? HealthCheckResult.Status.HEALTHY : HealthCheckResult.Status.UNHEALTHY)
                    .message(buildHealthMessage(isHealthy, performanceStatus, failureStatus))
                    .detail("replicaId", replicaConfig.getReplicaId())
                    .detail("endpoint", replicaConfig.getEndpoint())
                    .detail("region", replicaConfig.getRegion())
                    .detail("bucket", replicaConfig.getBucket())
                    .detail("performanceHealthy", performanceStatus.isHealthy)
                    .detail("failureHealthy", failureStatus.isHealthy)
                    .detail("successRate", performanceStatus.successRate)
                    .detail("averageLatency", performanceStatus.averageLatency)
                    .detail("consecutiveFailures", failureStatus.consecutiveFailures)
                    .detail("uptimeRatio", performanceStatus.uptimeRatio);
                
                LOGGER.debug("Health check completed for replica {}: {}", 
                           replicaConfig.getReplicaId(), isHealthy ? "HEALTHY" : "UNHEALTHY");
                
                return resultBuilder;
            });
            
        } catch (Exception e) {
            LOGGER.error("Health check failed for replica {}", replicaConfig.getReplicaId(), e);
            return CompletableFuture.completedFuture(
                resultBuilder
                    .status(HealthCheckResult.Status.UNHEALTHY)
                    .message("Health check exception: " + e.getMessage())
                    .exception(e)
                    .detail("replicaId", replicaConfig.getReplicaId())
            );
        }
    }
    
    private CompletableFuture<Void> checkConnectivity() {
        LOGGER.debug("Checking connectivity for replica {}", replicaConfig.getReplicaId());
        
        // Perform a simple head bucket operation to check connectivity
        // This is a minimal operation that tests S3 endpoint reachability
        try {
            // Use a simple list operation as connectivity test
            return objectStorage.list(replicaConfig.getBucket(), "", 1, null)
                .thenApply(listResult -> {
                    LOGGER.debug("Connectivity check passed for replica {}", replicaConfig.getReplicaId());
                    return null;
                })
                .exceptionally(throwable -> {
                    LOGGER.warn("Connectivity check failed for replica {}", replicaConfig.getReplicaId(), throwable);
                    throw new RuntimeException("Connectivity check failed: " + throwable.getMessage(), throwable);
                });
        } catch (Exception e) {
            LOGGER.warn("Connectivity check exception for replica {}", replicaConfig.getReplicaId(), e);
            return CompletableFuture.failedFuture(e);
        }
    }
    
    private PerformanceHealthStatus analyzePerformanceMetrics() {
        if (replicaMetrics == null) {
            LOGGER.debug("No metrics available for replica {}", replicaConfig.getReplicaId());
            return new PerformanceHealthStatus(true, 1.0, 0.0, 1.0, "No metrics data available");
        }
        
        double successRate = replicaMetrics.getOverallSuccessRate();
        double averageLatency = replicaMetrics.getOverallAverageLatency();
        double uptimeRatio = replicaMetrics.getUptimeRatio();
        
        boolean performanceHealthy = true;
        StringBuilder issues = new StringBuilder();
        
        // Check success rate
        if (successRate < MIN_SUCCESS_RATE) {
            performanceHealthy = false;
            issues.append("Low success rate: ").append(String.format("%.1f%%", successRate * 100)).append("; ");
        }
        
        // Check average latency
        if (averageLatency > MAX_AVERAGE_LATENCY_MS) {
            performanceHealthy = false;
            issues.append("High latency: ").append(String.format("%.0fms", averageLatency)).append("; ");
        }
        
        // Check uptime ratio
        if (uptimeRatio < MIN_UPTIME_RATIO) {
            performanceHealthy = false;
            issues.append("Low uptime: ").append(String.format("%.1f%%", uptimeRatio * 100)).append("; ");
        }
        
        String summary = performanceHealthy ? "Performance metrics healthy" : 
                        "Performance issues: " + issues.toString();
        
        LOGGER.debug("Performance analysis for replica {}: {} (success={:.1f}%, latency={:.0f}ms, uptime={:.1f}%)",
                   replicaConfig.getReplicaId(), performanceHealthy ? "HEALTHY" : "UNHEALTHY",
                   successRate * 100, averageLatency, uptimeRatio * 100);
        
        return new PerformanceHealthStatus(performanceHealthy, successRate, averageLatency, uptimeRatio, summary);
    }
    
    private FailureHealthStatus analyzeRecentFailures() {
        if (replicaMetrics == null) {
            return new FailureHealthStatus(true, 0, "No metrics data available");
        }
        
        long consecutiveFailures = replicaMetrics.getConsecutiveFailures();
        boolean failureHealthy = consecutiveFailures <= MAX_CONSECUTIVE_FAILURES;
        
        String summary = failureHealthy ? 
                        "Failure status healthy (consecutive failures: " + consecutiveFailures + ")" :
                        "Too many consecutive failures: " + consecutiveFailures;
        
        LOGGER.debug("Failure analysis for replica {}: {} (consecutive failures: {})",
                   replicaConfig.getReplicaId(), failureHealthy ? "HEALTHY" : "UNHEALTHY", consecutiveFailures);
        
        return new FailureHealthStatus(failureHealthy, consecutiveFailures, summary);
    }
    
    private String buildHealthMessage(boolean isHealthy, PerformanceHealthStatus performanceStatus, 
                                     FailureHealthStatus failureStatus) {
        if (isHealthy) {
            return String.format("Replica %d is healthy - connectivity OK, %s, %s",
                                replicaConfig.getReplicaId(),
                                performanceStatus.summary,
                                failureStatus.summary);
        } else {
            StringBuilder message = new StringBuilder();
            message.append("Replica ").append(replicaConfig.getReplicaId()).append(" is unhealthy - ");
            
            if (!performanceStatus.isHealthy) {
                message.append(performanceStatus.summary).append(" ");
            }
            if (!failureStatus.isHealthy) {
                message.append(failureStatus.summary);
            }
            
            return message.toString().trim();
        }
    }
    
    @Override
    public String getName() {
        return "ReplicaHealthCheck-" + replicaConfig.getReplicaId();
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
    
    private static class PerformanceHealthStatus {
        final boolean isHealthy;
        final double successRate;
        final double averageLatency;
        final double uptimeRatio;
        final String summary;
        
        PerformanceHealthStatus(boolean isHealthy, double successRate, double averageLatency, 
                               double uptimeRatio, String summary) {
            this.isHealthy = isHealthy;
            this.successRate = successRate;
            this.averageLatency = averageLatency;
            this.uptimeRatio = uptimeRatio;
            this.summary = summary;
        }
    }
    
    private static class FailureHealthStatus {
        final boolean isHealthy;
        final long consecutiveFailures;
        final String summary;
        
        FailureHealthStatus(boolean isHealthy, long consecutiveFailures, String summary) {
            this.isHealthy = isHealthy;
            this.consecutiveFailures = consecutiveFailures;
            this.summary = summary;
        }
    }
}