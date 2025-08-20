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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Result of a health check operation
 * Contains status information and details about the check
 */
public class HealthCheckResult {
    
    public enum Status {
        HEALTHY,
        UNHEALTHY,
        UNKNOWN,
        TIMEOUT
    }
    
    private final Status status;
    private final String message;
    private final long durationMs;
    private final Throwable exception;
    private final Map<String, Object> details;
    private final long timestamp;
    
    private HealthCheckResult(Builder builder) {
        this.status = Objects.requireNonNull(builder.status, "Status cannot be null");
        this.message = builder.message != null ? builder.message : "";
        this.durationMs = builder.durationMs;
        this.exception = builder.exception;
        this.details = new HashMap<>(builder.details);
        this.timestamp = builder.timestamp > 0 ? builder.timestamp : System.currentTimeMillis();
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static HealthCheckResult healthy() {
        return builder().status(Status.HEALTHY).build();
    }
    
    public static HealthCheckResult healthy(String message) {
        return builder().status(Status.HEALTHY).message(message).build();
    }
    
    public static HealthCheckResult unhealthy(String message) {
        return builder().status(Status.UNHEALTHY).message(message).build();
    }
    
    public static HealthCheckResult unhealthy(String message, Throwable exception) {
        return builder().status(Status.UNHEALTHY).message(message).exception(exception).build();
    }
    
    public static HealthCheckResult timeout(String message, long durationMs) {
        return builder().status(Status.TIMEOUT).message(message).durationMs(durationMs).build();
    }
    
    public static HealthCheckResult unknown(String message) {
        return builder().status(Status.UNKNOWN).message(message).build();
    }
    
    // Getters
    
    public Status getStatus() {
        return status;
    }
    
    public String getMessage() {
        return message;
    }
    
    public long getDurationMs() {
        return durationMs;
    }
    
    public Throwable getException() {
        return exception;
    }
    
    public Map<String, Object> getDetails() {
        return new HashMap<>(details);
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    // Convenience methods
    
    public boolean isHealthy() {
        return status == Status.HEALTHY;
    }
    
    public boolean isUnhealthy() {
        return status == Status.UNHEALTHY;
    }
    
    public boolean isTimeout() {
        return status == Status.TIMEOUT;
    }
    
    public boolean isUnknown() {
        return status == Status.UNKNOWN;
    }
    
    public Object getDetail(String key) {
        return details.get(key);
    }
    
    public String getDetailAsString(String key) {
        Object value = details.get(key);
        return value != null ? value.toString() : null;
    }
    
    public Number getDetailAsNumber(String key) {
        Object value = details.get(key);
        return value instanceof Number ? (Number) value : null;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HealthCheckResult{")
          .append("status=").append(status)
          .append(", message='").append(message).append('\'')
          .append(", durationMs=").append(durationMs);
        
        if (exception != null) {
            sb.append(", exception=").append(exception.getClass().getSimpleName())
              .append(":").append(exception.getMessage());
        }
        
        if (!details.isEmpty()) {
            sb.append(", details=").append(details);
        }
        
        sb.append(", timestamp=").append(timestamp);
        sb.append('}');
        
        return sb.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HealthCheckResult that = (HealthCheckResult) o;
        return durationMs == that.durationMs &&
               timestamp == that.timestamp &&
               status == that.status &&
               Objects.equals(message, that.message) &&
               Objects.equals(exception, that.exception) &&
               Objects.equals(details, that.details);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(status, message, durationMs, exception, details, timestamp);
    }
    
    /**
     * Builder for HealthCheckResult
     */
    public static class Builder {
        private Status status;
        private String message;
        private long durationMs;
        private Throwable exception;
        private Map<String, Object> details = new HashMap<>();
        private long timestamp;
        
        public Builder status(Status status) {
            this.status = status;
            return this;
        }
        
        public Builder message(String message) {
            this.message = message;
            return this;
        }
        
        public Builder durationMs(long durationMs) {
            this.durationMs = durationMs;
            return this;
        }
        
        public Builder exception(Throwable exception) {
            this.exception = exception;
            return this;
        }
        
        public Builder detail(String key, Object value) {
            this.details.put(key, value);
            return this;
        }
        
        public Builder details(Map<String, Object> details) {
            this.details.putAll(details);
            return this;
        }
        
        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public HealthCheckResult build() {
            return new HealthCheckResult(this);
        }
    }
}