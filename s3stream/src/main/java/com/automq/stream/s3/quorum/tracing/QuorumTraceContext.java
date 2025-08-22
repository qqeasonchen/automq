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

package com.automq.stream.s3.quorum.tracing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * QuorumTraceContext provides distributed tracing capabilities for S3 Quorum operations.
 * Tracks request flow across multiple replicas and provides detailed execution traces.
 */
public class QuorumTraceContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumTraceContext.class);
    private static final AtomicLong TRACE_ID_COUNTER = new AtomicLong(0);
    
    private final String traceId;
    private final String operation;
    private final String objectKey;
    private final long startTime;
    private final ConcurrentHashMap<String, TraceSpan> spans;
    private volatile boolean completed = false;
    
    public QuorumTraceContext(String operation, String objectKey) {
        this.traceId = generateTraceId();
        this.operation = operation;
        this.objectKey = objectKey;
        this.startTime = System.currentTimeMillis();
        this.spans = new ConcurrentHashMap<>();
        
        // Set MDC for structured logging
        MDC.put("traceId", traceId);
        MDC.put("operation", operation);
        if (objectKey != null) {
            MDC.put("objectKey", objectKey);
        }
        
        LOGGER.debug("Started trace context for operation: {}, objectKey: {}, traceId: {}", 
                    operation, objectKey, traceId);
    }
    
    private String generateTraceId() {
        return String.format("qrm-%08x-%04x-%d",
            System.currentTimeMillis() & 0xFFFFFFFFL,
            Thread.currentThread().getId() & 0xFFFF,
            TRACE_ID_COUNTER.incrementAndGet());
    }
    
    public TraceSpan startSpan(String spanName) {
        return startSpan(spanName, null);
    }
    
    public TraceSpan startSpan(String spanName, String replicaId) {
        String fullSpanName = replicaId != null ? spanName + "-" + replicaId : spanName;
        TraceSpan span = new TraceSpan(fullSpanName, replicaId);
        spans.put(fullSpanName, span);
        
        LOGGER.debug("Started span: {} for trace: {}", fullSpanName, traceId);
        return span;
    }
    
    public void completeTrace() {
        if (completed) {
            return;
        }
        
        completed = true;
        long totalDuration = System.currentTimeMillis() - startTime;
        
        LOGGER.info("Completed trace context: traceId={}, operation={}, objectKey={}, duration={}ms, spans={}",
                   traceId, operation, objectKey, totalDuration, spans.size());
        
        // Log span details
        spans.forEach((spanName, span) -> {
            if (span.isCompleted()) {
                LOGGER.debug("Span summary: name={}, duration={}ms, success={}, traceId={}",
                           spanName, span.getDuration(), span.isSuccess(), traceId);
            } else {
                LOGGER.warn("Incomplete span detected: name={}, traceId={}", spanName, traceId);
            }
        });
        
        // Clear MDC
        MDC.clear();
    }
    
    public void completeWithError(Throwable error) {
        LOGGER.error("Trace context completed with error: traceId={}, operation={}, objectKey={}, error={}",
                    traceId, operation, objectKey, error.getMessage(), error);
        completeTrace();
    }
    
    public TraceSpan getSpan(String spanName) {
        return spans.get(spanName);
    }
    
    public TraceSummary getTraceSummary() {
        long totalDuration = completed ? 
            System.currentTimeMillis() - startTime : 
            System.currentTimeMillis() - startTime;
        
        int completedSpans = (int) spans.values().stream().mapToLong(span -> span.isCompleted() ? 1 : 0).sum();
        int successfulSpans = (int) spans.values().stream().mapToLong(span -> span.isSuccess() ? 1 : 0).sum();
        
        return new TraceSummary(
            traceId,
            operation,
            objectKey,
            startTime,
            totalDuration,
            spans.size(),
            completedSpans,
            successfulSpans,
            completed
        );
    }
    
    // Getters
    public String getTraceId() {
        return traceId;
    }
    public String getOperation() {
        return operation;
    }
    public String getObjectKey() {
        return objectKey;
    }
    public long getStartTime() {
        return startTime;
    }
    public boolean isCompleted() {
        return completed;
    }
    
    // Trace span class
    public static class TraceSpan {
        private final String name;
        private final String replicaId;
        private final long startTime;
        private volatile long endTime;
        private volatile boolean success;
        private volatile String errorMessage;
        private volatile boolean completed = false;
        
        public TraceSpan(String name, String replicaId) {
            this.name = name;
            this.replicaId = replicaId;
            this.startTime = System.currentTimeMillis();
        }
        
        public void complete() {
            complete(true, null);
        }
        
        public void complete(boolean success, String errorMessage) {
            if (completed) {
                return;
            }
            
            this.endTime = System.currentTimeMillis();
            this.success = success;
            this.errorMessage = errorMessage;
            this.completed = true;
            
            long duration = endTime - startTime;
            if (success) {
                LOGGER.debug("Span completed successfully: name={}, duration={}ms", name, duration);
            } else {
                LOGGER.warn("Span completed with error: name={}, duration={}ms, error={}", 
                           name, duration, errorMessage);
            }
        }
        
        public void recordError(String errorMessage) {
            complete(false, errorMessage);
        }
        
        public void recordError(Throwable throwable) {
            complete(false, throwable.getMessage());
        }
        
        // Getters
        public String getName() {
            return name;
        }
        public String getReplicaId() {
            return replicaId;
        }
        public long getStartTime() {
            return startTime;
        }
        public long getEndTime() {
            return endTime;
        }
        public long getDuration() {
            return completed ? endTime - startTime : System.currentTimeMillis() - startTime;
        }
        public boolean isSuccess() {
            return success;
        }
        public String getErrorMessage() {
            return errorMessage;
        }
        public boolean isCompleted() {
            return completed;
        }
    }
    
    // Trace summary for monitoring
    public static class TraceSummary {
        private final String traceId;
        private final String operation;
        private final String objectKey;
        private final long startTime;
        private final long totalDuration;
        private final int totalSpans;
        private final int completedSpans;
        private final int successfulSpans;
        private final boolean completed;
        
        public TraceSummary(String traceId, String operation, String objectKey,
                           long startTime, long totalDuration, int totalSpans,
                           int completedSpans, int successfulSpans, boolean completed) {
            this.traceId = traceId;
            this.operation = operation;
            this.objectKey = objectKey;
            this.startTime = startTime;
            this.totalDuration = totalDuration;
            this.totalSpans = totalSpans;
            this.completedSpans = completedSpans;
            this.successfulSpans = successfulSpans;
            this.completed = completed;
        }
        
        // Getters
        public String getTraceId() {
            return traceId;
        }
        public String getOperation() {
            return operation;
        }
        public String getObjectKey() {
            return objectKey;
        }
        public long getStartTime() {
            return startTime;
        }
        public long getTotalDuration() {
            return totalDuration;
        }
        public int getTotalSpans() {
            return totalSpans;
        }
        public int getCompletedSpans() {
            return completedSpans;
        }
        public int getSuccessfulSpans() {
            return successfulSpans;
        }
        public boolean isCompleted() {
            return completed;
        }
        
        public double getSuccessRate() {
            return totalSpans > 0 ? (double) successfulSpans / totalSpans : 0.0;
        }
        
        public double getCompletionRate() {
            return totalSpans > 0 ? (double) completedSpans / totalSpans : 0.0;
        }
    }
    
    // Thread-local context management
    private static final ThreadLocal<QuorumTraceContext> CURRENT_TRACE = new ThreadLocal<>();
    
    public static QuorumTraceContext current() {
        return CURRENT_TRACE.get();
    }
    
    public static void setCurrent(QuorumTraceContext context) {
        CURRENT_TRACE.set(context);
    }
    
    public static void clear() {
        CURRENT_TRACE.remove();
    }
    
    // Utility methods for common tracing patterns
    public static QuorumTraceContext startTrace(String operation, String objectKey) {
        QuorumTraceContext context = new QuorumTraceContext(operation, objectKey);
        setCurrent(context);
        return context;
    }
    
    public static void endTrace() {
        QuorumTraceContext context = current();
        if (context != null) {
            context.completeTrace();
            clear();
        }
    }
    
    public static void endTraceWithError(Throwable error) {
        QuorumTraceContext context = current();
        if (context != null) {
            context.completeWithError(error);
            clear();
        }
    }
}