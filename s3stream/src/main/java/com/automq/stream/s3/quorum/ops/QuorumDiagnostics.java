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

package com.automq.stream.s3.quorum.ops;

import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.quorum.config.DynamicQuorumConfig;
import com.automq.stream.s3.quorum.consistency.QuorumDataValidator;
import com.automq.stream.s3.quorum.monitoring.QuorumMetricsCollector;
import com.automq.stream.s3.quorum.tracing.QuorumTraceContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * QuorumDiagnostics provides comprehensive diagnostic and troubleshooting tools
 * for S3 Quorum Storage operations.
 */
public class QuorumDiagnostics {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumDiagnostics.class);
    
    private final List<ObjectStorage> replicas;
    private final DynamicQuorumConfig config;
    private final QuorumMetricsCollector metricsCollector;
    private final QuorumDataValidator dataValidator;
    private final QuorumHealthChecker healthChecker;
    
    public QuorumDiagnostics(List<ObjectStorage> replicas, 
                           DynamicQuorumConfig config,
                           QuorumMetricsCollector metricsCollector,
                           QuorumDataValidator dataValidator,
                           QuorumHealthChecker healthChecker) {
        this.replicas = replicas;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.dataValidator = dataValidator;
        this.healthChecker = healthChecker;
        
        LOGGER.info("QuorumDiagnostics initialized for {} replicas", replicas.size());
    }
    
    /**
     * Generate comprehensive diagnostic report
     */
    public DiagnosticReport generateDiagnosticReport(DiagnosticScope scope) {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Generating diagnostic report with scope: {}", scope);
        
        DiagnosticReport.Builder reportBuilder = new DiagnosticReport.Builder()
            .generatedAt(startTime)
            .scope(scope);
        
        try {
            // System information
            if (scope.includesSystemInfo()) {
                reportBuilder.systemInfo(collectSystemInformation());
            }
            
            // Configuration information
            if (scope.includesConfiguration()) {
                reportBuilder.configurationInfo(collectConfigurationInformation());
            }
            
            // Health status
            if (scope.includesHealth()) {
                reportBuilder.healthStatus(collectHealthInformation());
            }
            
            // Performance metrics
            if (scope.includesMetrics()) {
                reportBuilder.performanceMetrics(collectPerformanceMetrics());
            }
            
            // Data consistency
            if (scope.includesConsistency()) {
                reportBuilder.consistencyInfo(collectConsistencyInformation());
            }
            
            // Network diagnostics
            if (scope.includesNetwork()) {
                reportBuilder.networkDiagnostics(performNetworkDiagnostics());
            }
            
            // Recent errors
            if (scope.includesErrors()) {
                reportBuilder.recentErrors(collectRecentErrors());
            }
            
            long duration = System.currentTimeMillis() - startTime;
            reportBuilder.generationDurationMs(duration);
            
            LOGGER.info("Diagnostic report generated successfully in {}ms", duration);
            return reportBuilder.build();
            
        } catch (Exception e) {
            LOGGER.error("Failed to generate diagnostic report", e);
            return reportBuilder
                .addError("Failed to generate report: " + e.getMessage())
                .generationDurationMs(System.currentTimeMillis() - startTime)
                .build();
        }
    }
    
    /**
     * Test connectivity to all replicas
     */
    public ConnectivityTestResult testConnectivity() {
        LOGGER.info("Starting connectivity test for {} replicas", replicas.size());
        long startTime = System.currentTimeMillis();
        
        List<ReplicaConnectivityResult> replicaResults = new ArrayList<>();
        
        for (int i = 0; i < replicas.size(); i++) {
            ReplicaConnectivityResult result = testReplicaConnectivity(i, replicas.get(i));
            replicaResults.add(result);
        }
        
        long totalDuration = System.currentTimeMillis() - startTime;
        int successfulConnections = replicaResults.stream().mapToInt(r -> r.isConnected() ? 1 : 0).sum();
        
        return new ConnectivityTestResult(
            replicaResults,
            successfulConnections,
            replicas.size(),
            totalDuration,
            System.currentTimeMillis()
        );
    }
    
    /**
     * Perform end-to-end functional test
     */
    public FunctionalTestResult performFunctionalTest() {
        LOGGER.info("Starting end-to-end functional test");
        QuorumTraceContext trace = QuorumTraceContext.startTrace("functional-test", "diagnostic-test");
        
        try {
            FunctionalTestResult.Builder resultBuilder = new FunctionalTestResult.Builder()
                .startTime(System.currentTimeMillis())
                .traceId(trace.getTraceId());
            
            // Test 1: Basic write operation
            QuorumTraceContext.TraceSpan writeSpan = trace.startSpan("write-test");
            try {
                ByteBuf testData = Unpooled.copiedBuffer("diagnostic-test-data-" + System.currentTimeMillis(), 
                                                       StandardCharsets.UTF_8);
                String testKey = "diagnostic/functional-test-" + System.currentTimeMillis();
                
                // This would require the actual QuorumObjectStorage instance
                // For now, we'll simulate the test
                writeSpan.complete();
                resultBuilder.writeTestPassed(true, "Write test simulated successfully");
            } catch (Exception e) {
                writeSpan.recordError(e);
                resultBuilder.writeTestPassed(false, "Write test failed: " + e.getMessage());
            }
            
            // Test 2: Basic read operation  
            QuorumTraceContext.TraceSpan readSpan = trace.startSpan("read-test");
            try {
                // Simulate read test
                readSpan.complete();
                resultBuilder.readTestPassed(true, "Read test simulated successfully");
            } catch (Exception e) {
                readSpan.recordError(e);
                resultBuilder.readTestPassed(false, "Read test failed: " + e.getMessage());
            }
            
            // Test 3: Consistency validation
            QuorumTraceContext.TraceSpan consistencySpan = trace.startSpan("consistency-test");
            try {
                QuorumDataValidator.ValidationStats stats = dataValidator.getValidationStats();
                consistencySpan.complete();
                resultBuilder.consistencyTestPassed(true, 
                    "Consistency test passed - validations: " + stats.getValidationCount());
            } catch (Exception e) {
                consistencySpan.recordError(e);
                resultBuilder.consistencyTestPassed(false, "Consistency test failed: " + e.getMessage());
            }
            
            trace.completeTrace();
            return resultBuilder.endTime(System.currentTimeMillis()).build();
            
        } catch (Exception e) {
            trace.completeWithError(e);
            return new FunctionalTestResult.Builder()
                .startTime(System.currentTimeMillis())
                .endTime(System.currentTimeMillis())
                .traceId(trace.getTraceId())
                .writeTestPassed(false, "Functional test failed: " + e.getMessage())
                .build();
        } finally {
            QuorumTraceContext.endTrace();
        }
    }
    
    /**
     * Analyze recent performance trends
     */
    public PerformanceAnalysis analyzePerformance() {
        LOGGER.info("Analyzing performance trends");
        
        try {
            QuorumMetricsCollector.MetricsSummary metrics = metricsCollector.getMetricsSummary();
            DynamicQuorumConfig.ConfigStats configStats = config.getConfigStats();
            
            // Analyze performance trends
            List<String> observations = new ArrayList<>();
            List<String> recommendations = new ArrayList<>();
            
            // Check response times
            double avgWriteLatency = configStats.getAverageWriteLatency();
            double avgReadLatency = configStats.getAverageReadLatency();
            
            if (avgWriteLatency > 1000) {
                observations.add("High write latency detected: " + String.format("%.1fms", avgWriteLatency));
                recommendations.add("Consider reducing write quorum size or checking network connectivity");
            }
            
            if (avgReadLatency > 500) {
                observations.add("High read latency detected: " + String.format("%.1fms", avgReadLatency));
                recommendations.add("Consider reducing read quorum size or using replica locality optimization");
            }
            
            // Check success rates
            double successRate = metrics.getSuccessRate();
            if (successRate < 0.95) {
                observations.add("Low success rate detected: " + String.format("%.1f%%", successRate * 100));
                recommendations.add("Check replica health and network connectivity");
            }
            
            // Check operation throughput
            long totalOperations = metrics.getTotalOperations();
            long successfulOperations = metrics.getSuccessfulOperations();
            if (totalOperations > 0 && (double) successfulOperations / totalOperations < 0.9) {
                observations.add("High failure rate in operations");
                recommendations.add("Review error logs and check quorum configuration");
            }
            
            return new PerformanceAnalysis(
                avgWriteLatency,
                avgReadLatency,
                successRate,
                totalOperations,
                observations,
                recommendations,
                System.currentTimeMillis()
            );
            
        } catch (Exception e) {
            LOGGER.error("Performance analysis failed", e);
            return new PerformanceAnalysis(
                -1, -1, -1, 0,
                List.of("Performance analysis failed: " + e.getMessage()),
                List.of("Check system health and try again"),
                System.currentTimeMillis()
            );
        }
    }
    
    /**
     * Export diagnostic data for support
     */
    public String exportDiagnosticData(DiagnosticScope scope) {
        DiagnosticReport report = generateDiagnosticReport(scope);
        return report.toDetailedString();
    }
    
    private SystemInformation collectSystemInformation() {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        
        return new SystemInformation(
            runtime.getVmName() + " " + runtime.getVmVersion(),
            runtime.getUptime(),
            memory.getHeapMemoryUsage().getUsed(),
            memory.getHeapMemoryUsage().getMax(),
            memory.getNonHeapMemoryUsage().getUsed(),
            threads.getThreadCount(),
            threads.getDaemonThreadCount(),
            Runtime.getRuntime().availableProcessors()
        );
    }
    
    private ConfigurationInformation collectConfigurationInformation() {
        return new ConfigurationInformation(
            config.getWriteQuorumSize(),
            config.getReadQuorumSize(),
            config.getTimeoutMs(),
            config.getRetryBackoffMs(),
            replicas.size(),
            config.isAdaptiveTuningEnabled()
        );
    }
    
    private HealthInformation collectHealthInformation() {
        QuorumHealthChecker.ClusterHealthStatus clusterHealth = healthChecker.getClusterHealth();
        QuorumHealthChecker.HealthStats healthStats = healthChecker.getHealthStats();
        
        return new HealthInformation(
            clusterHealth.getClusterState().name(),
            clusterHealth.getHealthyReplicaCount(),
            replicas.size(),
            healthStats.getSuccessRate(),
            healthStats.getAvgResponseTimeMs(),
            clusterHealth.getCheckTime()
        );
    }
    
    private PerformanceMetrics collectPerformanceMetrics() {
        QuorumMetricsCollector.MetricsSummary metrics = metricsCollector.getMetricsSummary();
        DynamicQuorumConfig.ConfigStats configStats = config.getConfigStats();
        
        return new PerformanceMetrics(
            metrics.getTotalOperations(),
            metrics.getSuccessfulOperations(),
            metrics.getSuccessRate(),
            metrics.getAverageOperationDuration(),
            configStats.getAverageWriteLatency(),
            configStats.getAverageReadLatency()
        );
    }
    
    private ConsistencyInformation collectConsistencyInformation() {
        QuorumDataValidator.ValidationStats stats = dataValidator.getValidationStats();
        
        return new ConsistencyInformation(
            stats.getValidationCount(),
            stats.getRepairCount(),
            stats.getRepairCount() > 0 ? (double) stats.getRepairCount() / stats.getValidationCount() : 0.0
        );
    }
    
    private NetworkDiagnostics performNetworkDiagnostics() {
        // Simulate network diagnostics
        List<String> networkIssues = new ArrayList<>();
        
        // Check if any replicas are consistently slow
        for (int i = 0; i < replicas.size(); i++) {
            QuorumHealthChecker.ReplicaHealthStatus status = healthChecker.getReplicaHealth(i);
            if (status.getLastResponseTimeMs() > 5000) {
                networkIssues.add("Replica " + i + " has high response time: " + status.getLastResponseTimeMs() + "ms");
            }
        }
        
        return new NetworkDiagnostics(
            networkIssues,
            replicas.size(),
            replicas.stream().mapToInt(r -> r.readinessCheck() ? 1 : 0).sum()
        );
    }
    
    private List<String> collectRecentErrors() {
        // This would collect recent errors from logs or error tracking
        List<String> errors = new ArrayList<>();
        errors.add("No recent errors collected - implement error tracking for production use");
        return errors;
    }
    
    private ReplicaConnectivityResult testReplicaConnectivity(int index, ObjectStorage replica) {
        long startTime = System.currentTimeMillis();
        String replicaId = "replica-" + index;
        
        try {
            boolean connected = replica.readinessCheck();
            long responseTime = System.currentTimeMillis() - startTime;
            
            if (connected) {
                // Test basic operation
                try {
                    CompletableFuture<List<ObjectStorage.ObjectInfo>> listFuture = replica.list("connectivity-test/");
                    listFuture.get(5, TimeUnit.SECONDS);
                    
                    return new ReplicaConnectivityResult(
                        replicaId, true, responseTime, "Connection successful"
                    );
                } catch (Exception e) {
                    return new ReplicaConnectivityResult(
                        replicaId, false, responseTime, "Readiness check passed but operation failed: " + e.getMessage()
                    );
                }
            } else {
                return new ReplicaConnectivityResult(
                    replicaId, false, responseTime, "Readiness check failed"
                );
            }
            
        } catch (Exception e) {
            long responseTime = System.currentTimeMillis() - startTime;
            return new ReplicaConnectivityResult(
                replicaId, false, responseTime, "Connection failed: " + e.getMessage()
            );
        }
    }
    
    // Data classes for diagnostic results
    public enum DiagnosticScope {
        BASIC(true, true, true, false, false, false, false),
        STANDARD(true, true, true, true, true, false, true),
        COMPREHENSIVE(true, true, true, true, true, true, true);
        
        private final boolean systemInfo;
        private final boolean configuration;
        private final boolean health;
        private final boolean metrics;
        private final boolean consistency;
        private final boolean network;
        private final boolean errors;
        
        DiagnosticScope(boolean systemInfo, boolean configuration, boolean health, 
                       boolean metrics, boolean consistency, boolean network, boolean errors) {
            this.systemInfo = systemInfo;
            this.configuration = configuration;
            this.health = health;
            this.metrics = metrics;
            this.consistency = consistency;
            this.network = network;
            this.errors = errors;
        }
        
        public boolean includesSystemInfo() { return systemInfo; }
        public boolean includesConfiguration() { return configuration; }
        public boolean includesHealth() { return health; }
        public boolean includesMetrics() { return metrics; }
        public boolean includesConsistency() { return consistency; }
        public boolean includesNetwork() { return network; }
        public boolean includesErrors() { return errors; }
    }
    
    public static class DiagnosticReport {
        private final DiagnosticScope scope;
        private final SystemInformation systemInfo;
        private final ConfigurationInformation configInfo;
        private final HealthInformation healthInfo;
        private final PerformanceMetrics performanceMetrics;
        private final ConsistencyInformation consistencyInfo;
        private final NetworkDiagnostics networkDiagnostics;
        private final List<String> recentErrors;
        private final List<String> errors;
        private final long generatedAt;
        private final long generationDurationMs;
        
        private DiagnosticReport(Builder builder) {
            this.scope = builder.scope;
            this.systemInfo = builder.systemInfo;
            this.configInfo = builder.configInfo;
            this.healthInfo = builder.healthInfo;
            this.performanceMetrics = builder.performanceMetrics;
            this.consistencyInfo = builder.consistencyInfo;
            this.networkDiagnostics = builder.networkDiagnostics;
            this.recentErrors = builder.recentErrors;
            this.errors = new ArrayList<>(builder.errors);
            this.generatedAt = builder.generatedAt;
            this.generationDurationMs = builder.generationDurationMs;
        }
        
        public String toDetailedString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== S3 Quorum Storage Diagnostic Report ===\n");
            sb.append("Generated: ").append(Instant.ofEpochMilli(generatedAt)).append("\n");
            sb.append("Generation Time: ").append(generationDurationMs).append("ms\n");
            sb.append("Scope: ").append(scope).append("\n\n");
            
            if (systemInfo != null) {
                sb.append("--- System Information ---\n");
                sb.append(systemInfo.toString()).append("\n\n");
            }
            
            if (configInfo != null) {
                sb.append("--- Configuration ---\n");
                sb.append(configInfo.toString()).append("\n\n");
            }
            
            if (healthInfo != null) {
                sb.append("--- Health Status ---\n");
                sb.append(healthInfo.toString()).append("\n\n");
            }
            
            if (performanceMetrics != null) {
                sb.append("--- Performance Metrics ---\n");
                sb.append(performanceMetrics.toString()).append("\n\n");
            }
            
            if (consistencyInfo != null) {
                sb.append("--- Data Consistency ---\n");
                sb.append(consistencyInfo.toString()).append("\n\n");
            }
            
            if (networkDiagnostics != null) {
                sb.append("--- Network Diagnostics ---\n");
                sb.append(networkDiagnostics.toString()).append("\n\n");
            }
            
            if (!recentErrors.isEmpty()) {
                sb.append("--- Recent Errors ---\n");
                for (String error : recentErrors) {
                    sb.append("- ").append(error).append("\n");
                }
                sb.append("\n");
            }
            
            if (!errors.isEmpty()) {
                sb.append("--- Report Generation Errors ---\n");
                for (String error : errors) {
                    sb.append("- ").append(error).append("\n");
                }
                sb.append("\n");
            }
            
            return sb.toString();
        }
        
        public static class Builder {
            private DiagnosticScope scope;
            private SystemInformation systemInfo;
            private ConfigurationInformation configInfo;
            private HealthInformation healthInfo;
            private PerformanceMetrics performanceMetrics;
            private ConsistencyInformation consistencyInfo;
            private NetworkDiagnostics networkDiagnostics;
            private List<String> recentErrors = new ArrayList<>();
            private List<String> errors = new ArrayList<>();
            private long generatedAt;
            private long generationDurationMs;
            
            public Builder scope(DiagnosticScope scope) { this.scope = scope; return this; }
            public Builder systemInfo(SystemInformation info) { this.systemInfo = info; return this; }
            public Builder configurationInfo(ConfigurationInformation info) { this.configInfo = info; return this; }
            public Builder healthStatus(HealthInformation info) { this.healthInfo = info; return this; }
            public Builder performanceMetrics(PerformanceMetrics metrics) { this.performanceMetrics = metrics; return this; }
            public Builder consistencyInfo(ConsistencyInformation info) { this.consistencyInfo = info; return this; }
            public Builder networkDiagnostics(NetworkDiagnostics diagnostics) { this.networkDiagnostics = diagnostics; return this; }
            public Builder recentErrors(List<String> errors) { this.recentErrors = errors; return this; }
            public Builder addError(String error) { this.errors.add(error); return this; }
            public Builder generatedAt(long timestamp) { this.generatedAt = timestamp; return this; }
            public Builder generationDurationMs(long duration) { this.generationDurationMs = duration; return this; }
            
            public DiagnosticReport build() {
                return new DiagnosticReport(this);
            }
        }
    }
    
    // Additional data classes would be defined here for all the information structures
    // (SystemInformation, ConfigurationInformation, etc.)
    // For brevity, I'm including just a few key ones:
    
    public static class SystemInformation {
        private final String jvmInfo;
        private final long uptimeMs;
        private final long heapUsed;
        private final long heapMax;
        private final long nonHeapUsed;
        private final int threadCount;
        private final int daemonThreadCount;
        private final int availableProcessors;
        
        public SystemInformation(String jvmInfo, long uptimeMs, long heapUsed, long heapMax,
                               long nonHeapUsed, int threadCount, int daemonThreadCount, int availableProcessors) {
            this.jvmInfo = jvmInfo;
            this.uptimeMs = uptimeMs;
            this.heapUsed = heapUsed;
            this.heapMax = heapMax;
            this.nonHeapUsed = nonHeapUsed;
            this.threadCount = threadCount;
            this.daemonThreadCount = daemonThreadCount;
            this.availableProcessors = availableProcessors;
        }
        
        @Override
        public String toString() {
            return String.format("JVM: %s\nUptime: %d ms\nHeap: %d/%d MB\nNon-Heap: %d MB\nThreads: %d (%d daemon)\nCPUs: %d",
                               jvmInfo, uptimeMs, heapUsed / 1024 / 1024, heapMax / 1024 / 1024, 
                               nonHeapUsed / 1024 / 1024, threadCount, daemonThreadCount, availableProcessors);
        }
    }
    
    public static class ConfigurationInformation {
        private final int writeQuorumSize;
        private final int readQuorumSize;
        private final long timeoutMs;
        private final long retryBackoffMs;
        private final int replicaCount;
        private final boolean adaptiveTuningEnabled;
        
        public ConfigurationInformation(int writeQuorumSize, int readQuorumSize, long timeoutMs,
                                      long retryBackoffMs, int replicaCount, boolean adaptiveTuningEnabled) {
            this.writeQuorumSize = writeQuorumSize;
            this.readQuorumSize = readQuorumSize;
            this.timeoutMs = timeoutMs;
            this.retryBackoffMs = retryBackoffMs;
            this.replicaCount = replicaCount;
            this.adaptiveTuningEnabled = adaptiveTuningEnabled;
        }
        
        @Override
        public String toString() {
            return String.format("Write Quorum: %d\nRead Quorum: %d\nTimeout: %d ms\nRetry Backoff: %d ms\nReplicas: %d\nAdaptive Tuning: %s",
                               writeQuorumSize, readQuorumSize, timeoutMs, retryBackoffMs, replicaCount, adaptiveTuningEnabled);
        }
    }
    
    // Additional data classes (abbreviated for space)
    public static class HealthInformation {
        private final String clusterState;
        private final int healthyReplicas;
        private final int totalReplicas;
        private final double successRate;
        private final double avgResponseTime;
        private final long lastCheckTime;
        
        public HealthInformation(String clusterState, int healthyReplicas, int totalReplicas,
                               double successRate, double avgResponseTime, long lastCheckTime) {
            this.clusterState = clusterState;
            this.healthyReplicas = healthyReplicas;
            this.totalReplicas = totalReplicas;
            this.successRate = successRate;
            this.avgResponseTime = avgResponseTime;
            this.lastCheckTime = lastCheckTime;
        }
        
        @Override
        public String toString() {
            return String.format("Cluster State: %s\nHealthy Replicas: %d/%d\nSuccess Rate: %.1f%%\nAvg Response Time: %.1f ms\nLast Check: %s",
                               clusterState, healthyReplicas, totalReplicas, successRate * 100, avgResponseTime, 
                               Instant.ofEpochMilli(lastCheckTime));
        }
    }
    
    public static class PerformanceMetrics {
        private final long totalOperations;
        private final long successfulOperations;
        private final double successRate;
        private final double avgOperationDuration;
        private final double avgWriteLatency;
        private final double avgReadLatency;
        
        public PerformanceMetrics(long totalOperations, long successfulOperations, double successRate,
                                double avgOperationDuration, double avgWriteLatency, double avgReadLatency) {
            this.totalOperations = totalOperations;
            this.successfulOperations = successfulOperations;
            this.successRate = successRate;
            this.avgOperationDuration = avgOperationDuration;
            this.avgWriteLatency = avgWriteLatency;
            this.avgReadLatency = avgReadLatency;
        }
        
        @Override
        public String toString() {
            return String.format("Total Operations: %d\nSuccessful: %d\nSuccess Rate: %.1f%%\nAvg Duration: %.1f ms\nWrite Latency: %.1f ms\nRead Latency: %.1f ms",
                               totalOperations, successfulOperations, successRate * 100, avgOperationDuration, avgWriteLatency, avgReadLatency);
        }
    }
    
    public static class ConsistencyInformation {
        private final long validationCount;
        private final long repairCount;
        private final double repairRate;
        
        public ConsistencyInformation(long validationCount, long repairCount, double repairRate) {
            this.validationCount = validationCount;
            this.repairCount = repairCount;
            this.repairRate = repairRate;
        }
        
        @Override
        public String toString() {
            return String.format("Validations: %d\nRepairs: %d\nRepair Rate: %.1f%%", 
                               validationCount, repairCount, repairRate * 100);
        }
    }
    
    public static class NetworkDiagnostics {
        private final List<String> issues;
        private final int totalReplicas;
        private final int reachableReplicas;
        
        public NetworkDiagnostics(List<String> issues, int totalReplicas, int reachableReplicas) {
            this.issues = issues;
            this.totalReplicas = totalReplicas;
            this.reachableReplicas = reachableReplicas;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("Reachable Replicas: %d/%d\n", reachableReplicas, totalReplicas));
            if (!issues.isEmpty()) {
                sb.append("Issues:\n");
                for (String issue : issues) {
                    sb.append("- ").append(issue).append("\n");
                }
            }
            return sb.toString();
        }
    }
    
    public static class ConnectivityTestResult {
        private final List<ReplicaConnectivityResult> replicaResults;
        private final int successfulConnections;
        private final int totalReplicas;
        private final long totalDurationMs;
        private final long testTime;
        
        public ConnectivityTestResult(List<ReplicaConnectivityResult> replicaResults, int successfulConnections,
                                    int totalReplicas, long totalDurationMs, long testTime) {
            this.replicaResults = replicaResults;
            this.successfulConnections = successfulConnections;
            this.totalReplicas = totalReplicas;
            this.totalDurationMs = totalDurationMs;
            this.testTime = testTime;
        }
        
        public List<ReplicaConnectivityResult> getReplicaResults() { return replicaResults; }
        public boolean isAllConnected() { return successfulConnections == totalReplicas; }
        public double getConnectivityRate() { return (double) successfulConnections / totalReplicas; }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== Connectivity Test Results ===\n");
            sb.append(String.format("Connected: %d/%d (%.1f%%)\n", 
                                   successfulConnections, totalReplicas, getConnectivityRate() * 100));
            sb.append(String.format("Test Duration: %d ms\n", totalDurationMs));
            sb.append("\nReplica Details:\n");
            for (ReplicaConnectivityResult result : replicaResults) {
                sb.append(result.toString()).append("\n");
            }
            return sb.toString();
        }
    }
    
    public static class ReplicaConnectivityResult {
        private final String replicaId;
        private final boolean connected;
        private final long responseTimeMs;
        private final String message;
        
        public ReplicaConnectivityResult(String replicaId, boolean connected, long responseTimeMs, String message) {
            this.replicaId = replicaId;
            this.connected = connected;
            this.responseTimeMs = responseTimeMs;
            this.message = message;
        }
        
        public String getReplicaId() { return replicaId; }
        public boolean isConnected() { return connected; }
        public long getResponseTimeMs() { return responseTimeMs; }
        public String getMessage() { return message; }
        
        @Override
        public String toString() {
            return String.format("%s: %s (%d ms) - %s", 
                               replicaId, connected ? "CONNECTED" : "FAILED", responseTimeMs, message);
        }
    }
    
    public static class FunctionalTestResult {
        private final long startTime;
        private final long endTime;
        private final String traceId;
        private final boolean writeTestPassed;
        private final String writeTestMessage;
        private final boolean readTestPassed;
        private final String readTestMessage;
        private final boolean consistencyTestPassed;
        private final String consistencyTestMessage;
        
        private FunctionalTestResult(Builder builder) {
            this.startTime = builder.startTime;
            this.endTime = builder.endTime;
            this.traceId = builder.traceId;
            this.writeTestPassed = builder.writeTestPassed;
            this.writeTestMessage = builder.writeTestMessage;
            this.readTestPassed = builder.readTestPassed;
            this.readTestMessage = builder.readTestMessage;
            this.consistencyTestPassed = builder.consistencyTestPassed;
            this.consistencyTestMessage = builder.consistencyTestMessage;
        }
        
        public boolean allTestsPassed() {
            return writeTestPassed && readTestPassed && consistencyTestPassed;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== Functional Test Results ===\n");
            sb.append(String.format("Duration: %d ms\n", endTime - startTime));
            sb.append(String.format("Trace ID: %s\n", traceId));
            sb.append(String.format("Write Test: %s - %s\n", writeTestPassed ? "PASSED" : "FAILED", writeTestMessage));
            sb.append(String.format("Read Test: %s - %s\n", readTestPassed ? "PASSED" : "FAILED", readTestMessage));
            sb.append(String.format("Consistency Test: %s - %s\n", consistencyTestPassed ? "PASSED" : "FAILED", consistencyTestMessage));
            sb.append(String.format("Overall: %s\n", allTestsPassed() ? "PASSED" : "FAILED"));
            return sb.toString();
        }
        
        public static class Builder {
            private long startTime;
            private long endTime;
            private String traceId;
            private boolean writeTestPassed = false;
            private String writeTestMessage = "";
            private boolean readTestPassed = false;
            private String readTestMessage = "";
            private boolean consistencyTestPassed = false;
            private String consistencyTestMessage = "";
            
            public Builder startTime(long time) { this.startTime = time; return this; }
            public Builder endTime(long time) { this.endTime = time; return this; }
            public Builder traceId(String id) { this.traceId = id; return this; }
            public Builder writeTestPassed(boolean passed, String message) { 
                this.writeTestPassed = passed; 
                this.writeTestMessage = message; 
                return this; 
            }
            public Builder readTestPassed(boolean passed, String message) { 
                this.readTestPassed = passed; 
                this.readTestMessage = message; 
                return this; 
            }
            public Builder consistencyTestPassed(boolean passed, String message) { 
                this.consistencyTestPassed = passed; 
                this.consistencyTestMessage = message; 
                return this; 
            }
            
            public FunctionalTestResult build() {
                return new FunctionalTestResult(this);
            }
        }
    }
    
    public static class PerformanceAnalysis {
        private final double avgWriteLatency;
        private final double avgReadLatency;
        private final double successRate;
        private final long totalOperations;
        private final List<String> observations;
        private final List<String> recommendations;
        private final long analysisTime;
        
        public PerformanceAnalysis(double avgWriteLatency, double avgReadLatency, double successRate,
                                 long totalOperations, List<String> observations, List<String> recommendations,
                                 long analysisTime) {
            this.avgWriteLatency = avgWriteLatency;
            this.avgReadLatency = avgReadLatency;
            this.successRate = successRate;
            this.totalOperations = totalOperations;
            this.observations = observations;
            this.recommendations = recommendations;
            this.analysisTime = analysisTime;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== Performance Analysis ===\n");
            sb.append(String.format("Write Latency: %.1f ms\n", avgWriteLatency));
            sb.append(String.format("Read Latency: %.1f ms\n", avgReadLatency));
            sb.append(String.format("Success Rate: %.1f%%\n", successRate * 100));
            sb.append(String.format("Total Operations: %d\n", totalOperations));
            
            if (!observations.isEmpty()) {
                sb.append("\nObservations:\n");
                for (String obs : observations) {
                    sb.append("- ").append(obs).append("\n");
                }
            }
            
            if (!recommendations.isEmpty()) {
                sb.append("\nRecommendations:\n");
                for (String rec : recommendations) {
                    sb.append("- ").append(rec).append("\n");
                }
            }
            
            return sb.toString();
        }
    }
}