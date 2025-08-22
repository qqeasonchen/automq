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

import java.util.HashMap;
import java.util.Map;

/**
 * Structured exception class for S3 Quorum Storage operations.
 * Provides detailed context and error classification for better error handling.
 */
public class QuorumException extends RuntimeException {
    private final QuorumErrorCodes errorCode;
    private final String operation;
    private final String objectKey;
    private final Map<String, Object> context;
    private final long timestamp;
    private final String requestId;
    
    public QuorumException(QuorumErrorCodes errorCode, String operation, String objectKey) {
        this(errorCode, operation, objectKey, null, new HashMap<>());
    }
    
    public QuorumException(QuorumErrorCodes errorCode, String operation, String objectKey, Throwable cause) {
        this(errorCode, operation, objectKey, cause, new HashMap<>());
    }
    
    public QuorumException(QuorumErrorCodes errorCode, String operation, String objectKey, 
                          Throwable cause, Map<String, Object> context) {
        super(formatMessage(errorCode, operation, objectKey, context), cause);
        this.errorCode = errorCode;
        this.operation = operation;
        this.objectKey = objectKey;
        this.context = new HashMap<>(context);
        this.timestamp = System.currentTimeMillis();
        this.requestId = generateRequestId();
    }
    
    private static String formatMessage(QuorumErrorCodes errorCode, String operation, String objectKey, 
                                      Map<String, Object> context) {
        StringBuilder sb = new StringBuilder();
        sb.append("[").append(errorCode.getName()).append("] ");
        sb.append(errorCode.getDescription());
        
        if (operation != null) {
            sb.append(" (operation: ").append(operation).append(")");
        }
        
        if (objectKey != null) {
            sb.append(" (objectKey: ").append(objectKey).append(")");
        }
        
        if (context != null && !context.isEmpty()) {
            sb.append(" (context: ").append(context).append(")");
        }
        
        return sb.toString();
    }
    
    private String generateRequestId() {
        return "qrm-" + System.currentTimeMillis() + "-" + Thread.currentThread().getId();
    }
    
    // Getters
    public QuorumErrorCodes getErrorCode() { return errorCode; }
    public String getOperation() { return operation; }
    public String getObjectKey() { return objectKey; }
    public Map<String, Object> getContext() { return new HashMap<>(context); }
    public long getTimestamp() { return timestamp; }
    public String getRequestId() { return requestId; }
    
    // Context manipulation
    public QuorumException addContext(String key, Object value) {
        this.context.put(key, value);
        return this;
    }
    
    public Object getContextValue(String key) {
        return context.get(key);
    }
    
    // Convenience methods for common error scenarios
    public static QuorumException writeQuorumInsufficient(String objectKey, int availableReplicas, int requiredQuorum) {
        Map<String, Object> context = new HashMap<>();
        context.put("availableReplicas", availableReplicas);
        context.put("requiredQuorum", requiredQuorum);
        return new QuorumException(QuorumErrorCodes.WRITE_QUORUM_INSUFFICIENT, "write", objectKey, null, context);
    }
    
    public static QuorumException readQuorumInsufficient(String objectKey, int availableReplicas, int requiredQuorum) {
        Map<String, Object> context = new HashMap<>();
        context.put("availableReplicas", availableReplicas);
        context.put("requiredQuorum", requiredQuorum);
        return new QuorumException(QuorumErrorCodes.READ_QUORUM_INSUFFICIENT, "read", objectKey, null, context);
    }
    
    public static QuorumException writeTimeout(String objectKey, long timeoutMs, long actualDurationMs) {
        Map<String, Object> context = new HashMap<>();
        context.put("timeoutMs", timeoutMs);
        context.put("actualDurationMs", actualDurationMs);
        return new QuorumException(QuorumErrorCodes.WRITE_TIMEOUT, "write", objectKey, null, context);
    }
    
    public static QuorumException readTimeout(String objectKey, long timeoutMs, long actualDurationMs) {
        Map<String, Object> context = new HashMap<>();
        context.put("timeoutMs", timeoutMs);
        context.put("actualDurationMs", actualDurationMs);
        return new QuorumException(QuorumErrorCodes.READ_TIMEOUT, "read", objectKey, null, context);
    }
    
    public static QuorumException dataInconsistency(String objectKey, int replicasChecked, int inconsistentReplicas) {
        Map<String, Object> context = new HashMap<>();
        context.put("replicasChecked", replicasChecked);
        context.put("inconsistentReplicas", inconsistentReplicas);
        return new QuorumException(QuorumErrorCodes.READ_INCONSISTENCY_DETECTED, "read", objectKey, null, context);
    }
    
    public static QuorumException configInvalidQuorumSize(String parameterName, int providedValue, int maxAllowed) {
        Map<String, Object> context = new HashMap<>();
        context.put("parameterName", parameterName);
        context.put("providedValue", providedValue);
        context.put("maxAllowed", maxAllowed);
        return new QuorumException(QuorumErrorCodes.CONFIG_INVALID_QUORUM_SIZE, "config", null, null, context);
    }
    
    public static QuorumException networkConnectionFailed(String operation, String objectKey, String replicaEndpoint, Throwable cause) {
        Map<String, Object> context = new HashMap<>();
        context.put("replicaEndpoint", replicaEndpoint);
        return new QuorumException(QuorumErrorCodes.NETWORK_CONNECTION_FAILED, operation, objectKey, cause, context);
    }
    
    public static QuorumException storageUnavailable(String operation, String objectKey, String storageEndpoint, Throwable cause) {
        Map<String, Object> context = new HashMap<>();
        context.put("storageEndpoint", storageEndpoint);
        return new QuorumException(QuorumErrorCodes.STORAGE_UNAVAILABLE, operation, objectKey, cause, context);
    }
    
    public static QuorumException accessDenied(String objectKey, String operation, String principal) {
        Map<String, Object> context = new HashMap<>();
        context.put("principal", principal);
        context.put("attemptedOperation", operation);
        return new QuorumException(QuorumErrorCodes.SECURITY_ACCESS_DENIED, operation, objectKey, null, context);
    }
    
    // JSON-formatted error details for logging/monitoring
    public String toJsonString() {
        StringBuilder json = new StringBuilder();
        json.append("{");
        json.append("\"errorCode\":\"").append(errorCode.getName()).append("\",");
        json.append("\"errorMessage\":\"").append(escapeJsonString(getMessage())).append("\",");
        json.append("\"severity\":\"").append(errorCode.getSeverity().getName()).append("\",");
        json.append("\"category\":\"").append(errorCode.getCategory().name()).append("\",");
        json.append("\"operation\":\"").append(operation != null ? operation : "").append("\",");
        json.append("\"objectKey\":\"").append(objectKey != null ? objectKey : "").append("\",");
        json.append("\"requestId\":\"").append(requestId).append("\",");
        json.append("\"timestamp\":").append(timestamp);
        
        if (!context.isEmpty()) {
            json.append(",\"context\":{");
            boolean first = true;
            for (Map.Entry<String, Object> entry : context.entrySet()) {
                if (!first) json.append(",");
                json.append("\"").append(escapeJsonString(entry.getKey())).append("\":");
                if (entry.getValue() instanceof String) {
                    json.append("\"").append(escapeJsonString(entry.getValue().toString())).append("\"");
                } else {
                    json.append(entry.getValue());
                }
                first = false;
            }
            json.append("}");
        }
        
        json.append("}");
        return json.toString();
    }
    
    private String escapeJsonString(String str) {
        if (str == null) return "";
        return str.replace("\"", "\\\"").replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
    }
    
    // Error reporting summary
    public ErrorReport toErrorReport() {
        return new ErrorReport(
            errorCode,
            operation,
            objectKey,
            requestId,
            timestamp,
            getMessage(),
            context,
            getCause() != null ? getCause().getClass().getSimpleName() : null
        );
    }
    
    public static class ErrorReport {
        private final QuorumErrorCodes errorCode;
        private final String operation;
        private final String objectKey;
        private final String requestId;
        private final long timestamp;
        private final String message;
        private final Map<String, Object> context;
        private final String rootCauseType;
        
        public ErrorReport(QuorumErrorCodes errorCode, String operation, String objectKey,
                          String requestId, long timestamp, String message,
                          Map<String, Object> context, String rootCauseType) {
            this.errorCode = errorCode;
            this.operation = operation;
            this.objectKey = objectKey;
            this.requestId = requestId;
            this.timestamp = timestamp;
            this.message = message;
            this.context = context;
            this.rootCauseType = rootCauseType;
        }
        
        // Getters
        public QuorumErrorCodes getErrorCode() { return errorCode; }
        public String getOperation() { return operation; }
        public String getObjectKey() { return objectKey; }
        public String getRequestId() { return requestId; }
        public long getTimestamp() { return timestamp; }
        public String getMessage() { return message; }
        public Map<String, Object> getContext() { return context; }
        public String getRootCauseType() { return rootCauseType; }
    }
}