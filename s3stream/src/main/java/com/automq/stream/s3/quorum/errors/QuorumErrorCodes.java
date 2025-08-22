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

package com.automq.stream.s3.quorum.errors;

/**
 * Structured error codes for S3 Quorum Storage operations.
 * Provides consistent error categorization and handling strategies.
 */
public enum QuorumErrorCodes {
    
    // Write operation errors (1000-1999)
    WRITE_QUORUM_INSUFFICIENT(1001, "WRITE_QUORUM_INSUFFICIENT", 
        "Insufficient replicas available for write quorum", 
        ErrorSeverity.HIGH, ErrorCategory.AVAILABILITY),
    
    WRITE_TIMEOUT(1002, "WRITE_TIMEOUT", 
        "Write operation exceeded timeout threshold", 
        ErrorSeverity.MEDIUM, ErrorCategory.PERFORMANCE),
    
    WRITE_REPLICA_FAILURE(1003, "WRITE_REPLICA_FAILURE", 
        "One or more replicas failed during write operation", 
        ErrorSeverity.MEDIUM, ErrorCategory.AVAILABILITY),
    
    WRITE_DATA_CORRUPTION(1004, "WRITE_DATA_CORRUPTION", 
        "Data corruption detected during write operation", 
        ErrorSeverity.CRITICAL, ErrorCategory.DATA_INTEGRITY),
    
    // Read operation errors (2000-2999)
    READ_QUORUM_INSUFFICIENT(2001, "READ_QUORUM_INSUFFICIENT", 
        "Insufficient replicas available for read quorum", 
        ErrorSeverity.HIGH, ErrorCategory.AVAILABILITY),
    
    READ_TIMEOUT(2002, "READ_TIMEOUT", 
        "Read operation exceeded timeout threshold", 
        ErrorSeverity.MEDIUM, ErrorCategory.PERFORMANCE),
    
    READ_INCONSISTENCY_DETECTED(2003, "READ_INCONSISTENCY_DETECTED", 
        "Data inconsistency detected across replicas", 
        ErrorSeverity.HIGH, ErrorCategory.DATA_INTEGRITY),
    
    READ_REPAIR_FAILED(2004, "READ_REPAIR_FAILED", 
        "Automatic read repair operation failed", 
        ErrorSeverity.HIGH, ErrorCategory.DATA_INTEGRITY),
    
    // Configuration errors (3000-3999)
    CONFIG_INVALID_QUORUM_SIZE(3001, "CONFIG_INVALID_QUORUM_SIZE", 
        "Invalid quorum size configuration", 
        ErrorSeverity.CRITICAL, ErrorCategory.CONFIGURATION),
    
    CONFIG_INVALID_TIMEOUT(3002, "CONFIG_INVALID_TIMEOUT", 
        "Invalid timeout configuration", 
        ErrorSeverity.MEDIUM, ErrorCategory.CONFIGURATION),
    
    CONFIG_DYNAMIC_UPDATE_FAILED(3003, "CONFIG_DYNAMIC_UPDATE_FAILED", 
        "Dynamic configuration update failed", 
        ErrorSeverity.MEDIUM, ErrorCategory.CONFIGURATION),
    
    // Network and connectivity errors (4000-4999)
    NETWORK_CONNECTION_FAILED(4001, "NETWORK_CONNECTION_FAILED", 
        "Failed to establish connection to replica", 
        ErrorSeverity.HIGH, ErrorCategory.NETWORK),
    
    NETWORK_TIMEOUT(4002, "NETWORK_TIMEOUT", 
        "Network operation timed out", 
        ErrorSeverity.MEDIUM, ErrorCategory.NETWORK),
    
    NETWORK_PARTITION_DETECTED(4003, "NETWORK_PARTITION_DETECTED", 
        "Network partition detected between replicas", 
        ErrorSeverity.CRITICAL, ErrorCategory.NETWORK),
    
    // Storage errors (5000-5999)
    STORAGE_UNAVAILABLE(5001, "STORAGE_UNAVAILABLE", 
        "S3 storage backend unavailable", 
        ErrorSeverity.CRITICAL, ErrorCategory.STORAGE),
    
    STORAGE_QUOTA_EXCEEDED(5002, "STORAGE_QUOTA_EXCEEDED", 
        "Storage quota exceeded", 
        ErrorSeverity.HIGH, ErrorCategory.STORAGE),
    
    STORAGE_ACCESS_DENIED(5003, "STORAGE_ACCESS_DENIED", 
        "Access denied to S3 storage", 
        ErrorSeverity.CRITICAL, ErrorCategory.SECURITY),
    
    SECURITY_ACCESS_DENIED(5004, "SECURITY_ACCESS_DENIED", 
        "Access denied due to insufficient permissions", 
        ErrorSeverity.HIGH, ErrorCategory.SECURITY),
    
    // System errors (6000-6999)
    SYSTEM_RESOURCE_EXHAUSTED(6001, "SYSTEM_RESOURCE_EXHAUSTED", 
        "System resources exhausted", 
        ErrorSeverity.CRITICAL, ErrorCategory.SYSTEM),
    
    SYSTEM_MEMORY_LEAK(6002, "SYSTEM_MEMORY_LEAK", 
        "Memory leak detected in ByteBuf operations", 
        ErrorSeverity.HIGH, ErrorCategory.SYSTEM),
    
    SYSTEM_THREAD_POOL_EXHAUSTED(6003, "SYSTEM_THREAD_POOL_EXHAUSTED", 
        "Thread pool exhausted for async operations", 
        ErrorSeverity.HIGH, ErrorCategory.SYSTEM);
    
    private final int code;
    private final String name;
    private final String description;
    private final ErrorSeverity severity;
    private final ErrorCategory category;
    
    QuorumErrorCodes(int code, String name, String description, ErrorSeverity severity, ErrorCategory category) {
        this.code = code;
        this.name = name;
        this.description = description;
        this.severity = severity;
        this.category = category;
    }
    
    public int getCode() { return code; }
    public String getName() { return name; }
    public String getDescription() { return description; }
    public ErrorSeverity getSeverity() { return severity; }
    public ErrorCategory getCategory() { return category; }
    
    // Error severity levels
    public enum ErrorSeverity {
        LOW(1, "LOW", "Non-critical, monitoring only"),
        MEDIUM(2, "MEDIUM", "Affects performance but not availability"), 
        HIGH(3, "HIGH", "Affects availability or data integrity"),
        CRITICAL(4, "CRITICAL", "System failure or data loss risk");
        
        private final int level;
        private final String name;
        private final String description;
        
        ErrorSeverity(int level, String name, String description) {
            this.level = level;
            this.name = name;
            this.description = description;
        }
        
        public int getLevel() { return level; }
        public String getName() { return name; }
        public String getDescription() { return description; }
    }
    
    // Error category classification
    public enum ErrorCategory {
        AVAILABILITY("Replica availability issues"),
        PERFORMANCE("Performance degradation"),
        DATA_INTEGRITY("Data consistency and integrity"),
        CONFIGURATION("Configuration and setup"),
        NETWORK("Network connectivity"),
        STORAGE("S3 storage backend"),
        SECURITY("Security and authorization"),
        SYSTEM("System resources and memory");
        
        private final String description;
        
        ErrorCategory(String description) {
            this.description = description;
        }
        
        public String getDescription() { return description; }
    }
    
    // Error handling strategy recommendations
    public ErrorHandlingStrategy getRecommendedStrategy() {
        switch (this.severity) {
            case CRITICAL:
                return ErrorHandlingStrategy.IMMEDIATE_ALERT_AND_FAILOVER;
            case HIGH:
                return ErrorHandlingStrategy.ALERT_AND_RETRY;
            case MEDIUM:
                return ErrorHandlingStrategy.RETRY_WITH_BACKOFF;
            case LOW:
            default:
                return ErrorHandlingStrategy.LOG_AND_MONITOR;
        }
    }
    
    public enum ErrorHandlingStrategy {
        IMMEDIATE_ALERT_AND_FAILOVER("Alert immediately and trigger failover"),
        ALERT_AND_RETRY("Send alert and attempt retry"),
        RETRY_WITH_BACKOFF("Retry with exponential backoff"),
        LOG_AND_MONITOR("Log for monitoring purposes");
        
        private final String description;
        
        ErrorHandlingStrategy(String description) {
            this.description = description;
        }
        
        public String getDescription() { return description; }
    }
}