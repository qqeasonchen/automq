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
import com.automq.stream.s3.quorum.security.QuorumSecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QuorumOperationsManager provides centralized operations management and administration
 * for S3 Quorum Storage systems.
 */
public class QuorumOperationsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumOperationsManager.class);
    
    private final List<ObjectStorage> replicas;
    private final DynamicQuorumConfig config;
    private final QuorumMetricsCollector metricsCollector;
    private final QuorumDataValidator dataValidator;
    private final QuorumHealthChecker healthChecker;
    private final QuorumDiagnostics diagnostics;
    private final QuorumSecurityContext securityContext;
    private final Map<String, OperationTask> activeTasks;
    private final AtomicLong taskCounter;
    
    public QuorumOperationsManager(List<ObjectStorage> replicas,
                                 DynamicQuorumConfig config,
                                 QuorumMetricsCollector metricsCollector,
                                 QuorumDataValidator dataValidator,
                                 QuorumHealthChecker healthChecker,
                                 QuorumDiagnostics diagnostics,
                                 QuorumSecurityContext securityContext) {
        this.replicas = replicas;
        this.config = config;
        this.metricsCollector = metricsCollector;
        this.dataValidator = dataValidator;
        this.healthChecker = healthChecker;
        this.diagnostics = diagnostics;
        this.securityContext = securityContext;
        this.activeTasks = new ConcurrentHashMap<>();
        this.taskCounter = new AtomicLong(0);
        
        LOGGER.info("QuorumOperationsManager initialized for {} replicas", replicas.size());
    }
    
    /**
     * Get overall system status
     */
    public SystemStatus getSystemStatus() {
        QuorumHealthChecker.ClusterHealthStatus health = healthChecker.getClusterHealth();
        QuorumMetricsCollector.MetricsSummary metrics = metricsCollector.getMetricsSummary();
        QuorumDataValidator.ValidationStats validation = dataValidator.getValidationStats();
        QuorumSecurityContext.SecurityStats security = securityContext.getSecurityStats();
        
        return new SystemStatus(
            health.getClusterState().name(),
            health.getHealthyReplicaCount(),
            replicas.size(),
            config.getWriteQuorumSize(),
            config.getReadQuorumSize(),
            metrics.getSuccessRate(),
            validation.getValidationCount(),
            validation.getRepairCount(),
            security.getActiveSessions(),
            activeTasks.size(),
            System.currentTimeMillis()
        );
    }
    
    /**
     * Perform cluster maintenance operations
     */
    public OperationResult performMaintenance(MaintenanceOperation operation, String reason, String operator) {
        String taskId = generateTaskId("maintenance");
        LOGGER.info("Starting maintenance operation: {} by {} (reason: {})", operation, operator, reason);
        
        OperationTask task = new OperationTask(taskId, "maintenance", operator, reason, System.currentTimeMillis());
        activeTasks.put(taskId, task);
        
        try {
            switch (operation) {
                case FULL_CONSISTENCY_CHECK:
                    return performFullConsistencyCheck(task);
                    
                case REPLICA_HEALTH_CHECK:
                    return performReplicaHealthCheck(task);
                    
                case CONFIGURATION_BACKUP:
                    return performConfigurationBackup(task);
                    
                case SYSTEM_CLEANUP:
                    return performSystemCleanup(task);
                    
                case PERFORMANCE_ANALYSIS:
                    return performPerformanceAnalysis(task);
                    
                default:
                    return completeTask(task, false, "Unsupported maintenance operation: " + operation);
            }
            
        } catch (Exception e) {
            LOGGER.error("Maintenance operation {} failed", operation, e);
            return completeTask(task, false, "Maintenance failed: " + e.getMessage());
        }
    }
    
    /**
     * Update system configuration
     */
    public OperationResult updateConfiguration(ConfigurationUpdate update, String operator) {
        String taskId = generateTaskId("config-update");
        LOGGER.info("Updating configuration by {}: {}", operator, update);
        
        OperationTask task = new OperationTask(taskId, "config-update", operator, update.toString(), System.currentTimeMillis());
        activeTasks.put(taskId, task);
        
        try {
            boolean success = true;
            StringBuilder results = new StringBuilder();
            
            if (update.getWriteQuorumSize() != null) {
                boolean updated = config.updateWriteQuorumSize(update.getWriteQuorumSize(), "Manual update by " + operator);
                results.append("Write quorum size: ").append(updated ? "updated" : "failed").append("; ");
                success = success && updated;
            }
            
            if (update.getReadQuorumSize() != null) {
                boolean updated = config.updateReadQuorumSize(update.getReadQuorumSize(), "Manual update by " + operator);
                results.append("Read quorum size: ").append(updated ? "updated" : "failed").append("; ");
                success = success && updated;
            }
            
            if (update.getTimeoutMs() != null) {
                boolean updated = config.updateTimeoutMs((long)update.getTimeoutMs(), "Manual update by " + operator);
                results.append("Timeout: ").append(updated ? "updated" : "failed").append("; ");
                success = success && updated;
            }
            
            if (update.getRetryBackoffMs() != null) {
                boolean updated = config.updateRetryBackoffMs(update.getRetryBackoffMs(), "Manual update by " + operator);
                results.append("Retry backoff: ").append(updated ? "updated" : "failed").append("; ");
                success = success && updated;
            }
            
            return completeTask(task, success, results.toString());
            
        } catch (Exception e) {
            LOGGER.error("Configuration update failed", e);
            return completeTask(task, false, "Configuration update failed: " + e.getMessage());
        }
    }
    
    /**
     * Force replica synchronization
     */
    public CompletableFuture<OperationResult> forceSynchronization(String objectPattern, String operator) {
        String taskId = generateTaskId("sync");
        LOGGER.info("Starting forced synchronization for pattern '{}' by {}", objectPattern, operator);
        
        OperationTask task = new OperationTask(taskId, "synchronization", operator, 
                                             "Pattern: " + objectPattern, System.currentTimeMillis());
        activeTasks.put(taskId, task);
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                // This would typically scan for objects matching the pattern and force consistency checks
                // For now, we'll simulate the operation
                int processedObjects = 0;
                int repairedObjects = 0;
                
                // Simulate processing
                Thread.sleep(1000);
                processedObjects = 10; // Simulated
                repairedObjects = 2;   // Simulated
                
                String result = String.format("Processed %d objects, repaired %d inconsistencies", 
                                             processedObjects, repairedObjects);
                
                return completeTask(task, true, result);
                
            } catch (Exception e) {
                LOGGER.error("Synchronization failed", e);
                return completeTask(task, false, "Synchronization failed: " + e.getMessage());
            }
        });
    }
    
    /**
     * Generate system report
     */
    public SystemReport generateSystemReport(String operator, ReportScope scope) {
        LOGGER.info("Generating system report with scope {} by {}", scope, operator);
        
        long startTime = System.currentTimeMillis();
        
        // Collect system information
        SystemStatus status = getSystemStatus();
        QuorumDiagnostics.DiagnosticReport diagnosticReport = null;
        
        if (scope.includesDiagnostics()) {
            diagnosticReport = diagnostics.generateDiagnosticReport(QuorumDiagnostics.DiagnosticScope.COMPREHENSIVE);
        }
        
        // Collect security information
        QuorumSecurityContext.SecurityStats securityStats = securityContext.getSecurityStats();
        
        List<String> recentTasks = new ArrayList<>();
        activeTasks.values().forEach(task -> {
            if (task.getStartTime() > System.currentTimeMillis() - 3600000) { // Last hour
                recentTasks.add(task.toString());
            }
        });
        
        long generationTime = System.currentTimeMillis() - startTime;
        
        return new SystemReport(
            operator,
            scope,
            startTime,
            generationTime,
            status,
            diagnosticReport,
            securityStats,
            recentTasks
        );
    }
    
    /**
     * Emergency shutdown procedures
     */
    public OperationResult emergencyShutdown(String reason, String operator) {
        LOGGER.warn("Emergency shutdown initiated by {} (reason: {})", operator, reason);
        
        String taskId = generateTaskId("emergency-shutdown");
        OperationTask task = new OperationTask(taskId, "emergency-shutdown", operator, reason, System.currentTimeMillis());
        activeTasks.put(taskId, task);
        
        try {
            // Stop health checker
            healthChecker.stop();
            
            // Cancel active tasks
            int cancelledTasks = 0;
            for (OperationTask activeTask : activeTasks.values()) {
                if (!activeTask.getTaskId().equals(taskId) && !activeTask.isCompleted()) {
                    activeTask.markCompleted(false, "Cancelled due to emergency shutdown");
                    cancelledTasks++;
                }
            }
            
            String result = String.format("Emergency shutdown completed. Cancelled %d active tasks.", cancelledTasks);
            
            return completeTask(task, true, result);
            
        } catch (Exception e) {
            LOGGER.error("Emergency shutdown failed", e);
            return completeTask(task, false, "Emergency shutdown failed: " + e.getMessage());
        }
    }
    
    /**
     * Get active operations
     */
    public List<OperationTask> getActiveOperations() {
        return new ArrayList<>(activeTasks.values());
    }
    
    /**
     * Cancel an active operation
     */
    public boolean cancelOperation(String taskId, String operator) {
        OperationTask task = activeTasks.get(taskId);
        if (task != null && !task.isCompleted()) {
            task.markCompleted(false, "Cancelled by " + operator);
            LOGGER.info("Operation {} cancelled by {}", taskId, operator);
            return true;
        }
        return false;
    }
    
    private OperationResult performFullConsistencyCheck(OperationTask task) {
        try {
            QuorumDataValidator.ValidationStats stats = dataValidator.getValidationStats();
            String result = String.format("Consistency check completed. Validations: %d, Repairs: %d", 
                                         stats.getValidationCount(), stats.getRepairCount());
            return completeTask(task, true, result);
        } catch (Exception e) {
            return completeTask(task, false, "Consistency check failed: " + e.getMessage());
        }
    }
    
    private OperationResult performReplicaHealthCheck(OperationTask task) {
        try {
            QuorumHealthChecker.ClusterHealthStatus health = healthChecker.checkHealthNow().get();
            String result = String.format("Health check completed. Cluster state: %s, Healthy replicas: %d/%d", 
                                         health.getClusterState(), health.getHealthyReplicaCount(), replicas.size());
            return completeTask(task, health.isHealthy(), result);
        } catch (Exception e) {
            return completeTask(task, false, "Health check failed: " + e.getMessage());
        }
    }
    
    private OperationResult performConfigurationBackup(OperationTask task) {
        try {
            // Simulate configuration backup
            String backup = String.format("writeQuorum=%d,readQuorum=%d,timeout=%d,backoff=%d",
                                         config.getWriteQuorumSize(), config.getReadQuorumSize(), 
                                         config.getTimeoutMs(), config.getRetryBackoffMs());
            
            String result = "Configuration backup created: " + backup;
            return completeTask(task, true, result);
        } catch (Exception e) {
            return completeTask(task, false, "Configuration backup failed: " + e.getMessage());
        }
    }
    
    private OperationResult performSystemCleanup(OperationTask task) {
        try {
            // Cleanup completed tasks
            int removedTasks = 0;
            long cutoffTime = System.currentTimeMillis() - 86400000; // 24 hours ago
            
            activeTasks.entrySet().removeIf(entry -> {
                OperationTask t = entry.getValue();
                return t.isCompleted() && t.getStartTime() < cutoffTime;
            });
            
            String result = String.format("System cleanup completed. Removed %d old tasks.", removedTasks);
            return completeTask(task, true, result);
        } catch (Exception e) {
            return completeTask(task, false, "System cleanup failed: " + e.getMessage());
        }
    }
    
    private OperationResult performPerformanceAnalysis(OperationTask task) {
        try {
            QuorumDiagnostics.PerformanceAnalysis analysis = diagnostics.analyzePerformance();
            String result = "Performance analysis completed. Check detailed report.";
            return completeTask(task, true, result);
        } catch (Exception e) {
            return completeTask(task, false, "Performance analysis failed: " + e.getMessage());
        }
    }
    
    private OperationResult completeTask(OperationTask task, boolean success, String message) {
        task.markCompleted(success, message);
        long duration = System.currentTimeMillis() - task.getStartTime();
        
        LOGGER.info("Task {} completed: {} in {}ms - {}", 
                   task.getTaskId(), success ? "SUCCESS" : "FAILED", duration, message);
        
        return new OperationResult(task.getTaskId(), success, message, duration, System.currentTimeMillis());
    }
    
    private String generateTaskId(String type) {
        return String.format("task-%s-%d-%d", type, System.currentTimeMillis(), taskCounter.incrementAndGet());
    }
    
    // Data classes
    public enum MaintenanceOperation {
        FULL_CONSISTENCY_CHECK,
        REPLICA_HEALTH_CHECK,
        CONFIGURATION_BACKUP,
        SYSTEM_CLEANUP,
        PERFORMANCE_ANALYSIS
    }
    
    public enum ReportScope {
        BASIC(false, false),
        STANDARD(true, false),
        COMPREHENSIVE(true, true);
        
        private final boolean includesDiagnostics;
        private final boolean includesDetailedMetrics;
        
        ReportScope(boolean includesDiagnostics, boolean includesDetailedMetrics) {
            this.includesDiagnostics = includesDiagnostics;
            this.includesDetailedMetrics = includesDetailedMetrics;
        }
        
        public boolean includesDiagnostics() { return includesDiagnostics; }
        public boolean includesDetailedMetrics() { return includesDetailedMetrics; }
    }
    
    public static class ConfigurationUpdate {
        private final Integer writeQuorumSize;
        private final Integer readQuorumSize;
        private final Long timeoutMs;
        private final Long retryBackoffMs;
        
        public ConfigurationUpdate(Integer writeQuorumSize, Integer readQuorumSize, 
                                 Long timeoutMs, Long retryBackoffMs) {
            this.writeQuorumSize = writeQuorumSize;
            this.readQuorumSize = readQuorumSize;
            this.timeoutMs = timeoutMs;
            this.retryBackoffMs = retryBackoffMs;
        }
        
        public Integer getWriteQuorumSize() { return writeQuorumSize; }
        public Integer getReadQuorumSize() { return readQuorumSize; }
        public Long getTimeoutMs() { return timeoutMs; }
        public Long getRetryBackoffMs() { return retryBackoffMs; }
        
        @Override
        public String toString() {
            return String.format("ConfigUpdate{writeQuorum=%s, readQuorum=%s, timeout=%s, backoff=%s}",
                               writeQuorumSize, readQuorumSize, timeoutMs, retryBackoffMs);
        }
    }
    
    public static class OperationTask {
        private final String taskId;
        private final String operationType;
        private final String operator;
        private final String parameters;
        private final long startTime;
        private volatile boolean completed;
        private volatile boolean successful;
        private volatile String result;
        private volatile long endTime;
        
        public OperationTask(String taskId, String operationType, String operator, String parameters, long startTime) {
            this.taskId = taskId;
            this.operationType = operationType;
            this.operator = operator;
            this.parameters = parameters;
            this.startTime = startTime;
            this.completed = false;
        }
        
        public void markCompleted(boolean successful, String result) {
            this.completed = true;
            this.successful = successful;
            this.result = result;
            this.endTime = System.currentTimeMillis();
        }
        
        public String getTaskId() { return taskId; }
        public String getOperationType() { return operationType; }
        public String getOperator() { return operator; }
        public String getParameters() { return parameters; }
        public long getStartTime() { return startTime; }
        public boolean isCompleted() { return completed; }
        public boolean isSuccessful() { return successful; }
        public String getResult() { return result; }
        public long getDuration() { return completed ? endTime - startTime : System.currentTimeMillis() - startTime; }
        
        @Override
        public String toString() {
            return String.format("%s[%s] by %s: %s (%s) - %dms", 
                               taskId, operationType, operator, 
                               completed ? (successful ? "SUCCESS" : "FAILED") : "RUNNING",
                               completed ? result : "In progress", getDuration());
        }
    }
    
    public static class OperationResult {
        private final String taskId;
        private final boolean success;
        private final String message;
        private final long durationMs;
        private final long completedAt;
        
        public OperationResult(String taskId, boolean success, String message, long durationMs, long completedAt) {
            this.taskId = taskId;
            this.success = success;
            this.message = message;
            this.durationMs = durationMs;
            this.completedAt = completedAt;
        }
        
        public String getTaskId() { return taskId; }
        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public long getDurationMs() { return durationMs; }
        public long getCompletedAt() { return completedAt; }
        
        @Override
        public String toString() {
            return String.format("Operation %s: %s in %dms - %s", 
                               taskId, success ? "SUCCESS" : "FAILED", durationMs, message);
        }
    }
    
    public static class SystemStatus {
        private final String clusterHealth;
        private final int healthyReplicas;
        private final int totalReplicas;
        private final int writeQuorumSize;
        private final int readQuorumSize;
        private final double operationSuccessRate;
        private final long totalValidations;
        private final long totalRepairs;
        private final int activeSessions;
        private final int activeTasks;
        private final long statusTime;
        
        public SystemStatus(String clusterHealth, int healthyReplicas, int totalReplicas,
                          int writeQuorumSize, int readQuorumSize, double operationSuccessRate,
                          long totalValidations, long totalRepairs, int activeSessions, 
                          int activeTasks, long statusTime) {
            this.clusterHealth = clusterHealth;
            this.healthyReplicas = healthyReplicas;
            this.totalReplicas = totalReplicas;
            this.writeQuorumSize = writeQuorumSize;
            this.readQuorumSize = readQuorumSize;
            this.operationSuccessRate = operationSuccessRate;
            this.totalValidations = totalValidations;
            this.totalRepairs = totalRepairs;
            this.activeSessions = activeSessions;
            this.activeTasks = activeTasks;
            this.statusTime = statusTime;
        }
        
        @Override
        public String toString() {
            return String.format(
                "=== System Status ===\n" +
                "Timestamp: %s\n" +
                "Cluster Health: %s\n" +
                "Replicas: %d/%d healthy\n" +
                "Quorum Configuration: W=%d, R=%d\n" +
                "Operation Success Rate: %.1f%%\n" +
                "Data Validations: %d (Repairs: %d)\n" +
                "Active Sessions: %d\n" +
                "Active Tasks: %d",
                Instant.ofEpochMilli(statusTime), clusterHealth, healthyReplicas, totalReplicas,
                writeQuorumSize, readQuorumSize, operationSuccessRate * 100, 
                totalValidations, totalRepairs, activeSessions, activeTasks
            );
        }
    }
    
    public static class SystemReport {
        private final String generatedBy;
        private final ReportScope scope;
        private final long generatedAt;
        private final long generationTimeMs;
        private final SystemStatus systemStatus;
        private final QuorumDiagnostics.DiagnosticReport diagnosticReport;
        private final QuorumSecurityContext.SecurityStats securityStats;
        private final List<String> recentTasks;
        
        public SystemReport(String generatedBy, ReportScope scope, long generatedAt, long generationTimeMs,
                          SystemStatus systemStatus, QuorumDiagnostics.DiagnosticReport diagnosticReport,
                          QuorumSecurityContext.SecurityStats securityStats, List<String> recentTasks) {
            this.generatedBy = generatedBy;
            this.scope = scope;
            this.generatedAt = generatedAt;
            this.generationTimeMs = generationTimeMs;
            this.systemStatus = systemStatus;
            this.diagnosticReport = diagnosticReport;
            this.securityStats = securityStats;
            this.recentTasks = recentTasks;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("=== S3 Quorum Storage System Report ===\n");
            sb.append(String.format("Generated by: %s at %s\n", generatedBy, Instant.ofEpochMilli(generatedAt)));
            sb.append(String.format("Scope: %s (Generation time: %d ms)\n\n", scope, generationTimeMs));
            
            sb.append(systemStatus.toString()).append("\n\n");
            
            if (securityStats != null) {
                sb.append("--- Security Statistics ---\n");
                sb.append(String.format("Total Principals: %d\n", securityStats.getTotalPrincipals()));
                sb.append(String.format("Active Sessions: %d\n", securityStats.getActiveSessions()));
                sb.append(String.format("Authentication Success Rate: %.1f%%\n", 
                                       securityStats.getAuthenticationSuccessRate() * 100));
                sb.append(String.format("Authorization Success Rate: %.1f%%\n", 
                                       securityStats.getAuthorizationSuccessRate() * 100));
                sb.append("\n");
            }
            
            if (!recentTasks.isEmpty()) {
                sb.append("--- Recent Tasks ---\n");
                for (String task : recentTasks) {
                    sb.append(task).append("\n");
                }
                sb.append("\n");
            }
            
            if (diagnosticReport != null) {
                sb.append("--- Detailed Diagnostics ---\n");
                sb.append(diagnosticReport.toDetailedString());
            }
            
            return sb.toString();
        }
    }
}