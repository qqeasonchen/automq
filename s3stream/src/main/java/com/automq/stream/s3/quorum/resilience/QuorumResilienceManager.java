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

package com.automq.stream.s3.quorum.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * QuorumResilienceManager provides resilience patterns for S3 Quorum Storage operations
 * including retry logic, circuit breaker patterns, and timeout management.
 */
public class QuorumResilienceManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumResilienceManager.class);
    
    private final ResilienceConfig config;
    private final ScheduledExecutorService scheduler;
    
    public QuorumResilienceManager(ResilienceConfig config) {
        this.config = config;
        this.scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "quorum-resilience-" + Thread.currentThread().getId());
            t.setDaemon(true);
            return t;
        });
        
        LOGGER.info("QuorumResilienceManager initialized with config: {}", config);
    }
    
    /**
     * Execute an operation with retry logic
     */
    public <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> operation, 
                                                    String operationName) {
        return executeWithRetry(operation, operationName, config.getMaxRetries());
    }
    
    /**
     * Execute an operation with custom retry count
     */
    public <T> CompletableFuture<T> executeWithRetry(Supplier<CompletableFuture<T>> operation, 
                                                    String operationName, 
                                                    int maxRetries) {
        CompletableFuture<T> result = new CompletableFuture<>();
        executeWithRetryInternal(operation, operationName, maxRetries, 0, result);
        return result;
    }
    
    /**
     * Execute an operation with timeout
     */
    public <T> CompletableFuture<T> executeWithTimeout(Supplier<CompletableFuture<T>> operation, 
                                                      String operationName,
                                                      Duration timeout) {
        CompletableFuture<T> operationFuture = operation.get();
        CompletableFuture<T> timeoutFuture = new CompletableFuture<>();
        
        // Schedule timeout
        scheduler.schedule(() -> {
            if (!timeoutFuture.isDone()) {
                timeoutFuture.completeExceptionally(
                    new OperationTimeoutException("Operation " + operationName + " timed out after " + timeout));
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
        
        // Race between operation completion and timeout
        operationFuture.whenComplete((result, throwable) -> {
            if (!timeoutFuture.isDone()) {
                if (throwable != null) {
                    timeoutFuture.completeExceptionally(throwable);
                } else {
                    timeoutFuture.complete(result);
                }
            }
        });
        
        return timeoutFuture;
    }
    
    /**
     * Execute an operation with both retry and timeout
     */
    public <T> CompletableFuture<T> executeWithResiliency(Supplier<CompletableFuture<T>> operation, 
                                                         String operationName) {
        return executeWithRetry(() -> executeWithTimeout(operation, operationName, config.getOperationTimeout()), 
                               operationName);
    }
    
    /**
     * Execute an operation with circuit breaker pattern
     */
    public <T> CompletableFuture<T> executeWithCircuitBreaker(Supplier<CompletableFuture<T>> operation,
                                                             String operationName,
                                                             CircuitBreakerState circuitBreaker) {
        if (circuitBreaker.isOpen()) {
            return CompletableFuture.failedFuture(
                new CircuitBreakerOpenException("Circuit breaker is open for operation: " + operationName));
        }
        
        CompletableFuture<T> operationFuture = operation.get();
        
        operationFuture.whenComplete((result, throwable) -> {
            if (throwable != null) {
                circuitBreaker.recordFailure();
            } else {
                circuitBreaker.recordSuccess();
            }
        });
        
        return operationFuture;
    }
    
    /**
     * Create an exponential backoff delay
     */
    public Duration calculateBackoffDelay(int attemptNumber) {
        long delayMs = Math.min(
            config.getBaseDelayMs() * (long) Math.pow(config.getBackoffMultiplier(), attemptNumber),
            config.getMaxDelayMs()
        );
        
        // Add jitter to prevent thundering herd
        if (config.isJitterEnabled()) {
            delayMs += (long) (Math.random() * config.getJitterMaxMs());
        }
        
        return Duration.ofMillis(delayMs);
    }
    
    /**
     * Shutdown the resilience manager
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
        LOGGER.info("QuorumResilienceManager shutdown completed");
    }
    
    private <T> void executeWithRetryInternal(Supplier<CompletableFuture<T>> operation,
                                            String operationName,
                                            int maxRetries,
                                            int currentAttempt,
                                            CompletableFuture<T> result) {
        CompletableFuture<T> attemptFuture = operation.get();
        
        attemptFuture.whenComplete((value, throwable) -> {
            if (throwable == null) {
                // Success
                if (currentAttempt > 0) {
                    LOGGER.info("Operation {} succeeded after {} retries", operationName, currentAttempt);
                }
                result.complete(value);
            } else {
                // Failure
                LOGGER.debug("Operation {} failed on attempt {}: {}", operationName, currentAttempt + 1, throwable.getMessage());
                
                if (currentAttempt >= maxRetries) {
                    // Max retries exceeded
                    LOGGER.error("Operation {} failed after {} attempts", operationName, maxRetries + 1);
                    result.completeExceptionally(new MaxRetriesExceededException(
                        "Operation " + operationName + " failed after " + (maxRetries + 1) + " attempts", throwable));
                } else if (isRetriableException(throwable)) {
                    // Schedule retry with backoff
                    Duration delay = calculateBackoffDelay(currentAttempt);
                    LOGGER.info("Retrying operation {} in {}ms (attempt {}/{})", 
                               operationName, delay.toMillis(), currentAttempt + 2, maxRetries + 1);
                    
                    scheduler.schedule(() -> {
                        executeWithRetryInternal(operation, operationName, maxRetries, currentAttempt + 1, result);
                    }, delay.toMillis(), TimeUnit.MILLISECONDS);
                } else {
                    // Non-retriable exception
                    LOGGER.error("Operation {} failed with non-retriable exception", operationName);
                    result.completeExceptionally(throwable);
                }
            }
        });
    }
    
    /**
     * Determine if an exception is retriable
     */
    private boolean isRetriableException(Throwable throwable) {
        // Add more specific exception types as needed
        String message = throwable.getMessage().toLowerCase(java.util.Locale.ROOT);
        
        // Network-related errors are typically retriable
        if (message.contains("timeout") || 
            message.contains("connection") || 
            message.contains("network") ||
            message.contains("socket")) {
            return true;
        }
        
        // S3-specific retriable errors
        if (message.contains("503") || // Service Unavailable
            message.contains("500") || // Internal Server Error
            message.contains("throttle") ||
            message.contains("slow down")) {
            return true;
        }
        
        // Authentication errors are typically not retriable
        if (message.contains("401") || message.contains("403")) {
            return false;
        }
        
        // Default to retriable for unknown errors
        return true;
    }
    
    // Configuration class
    public static class ResilienceConfig {
        private final int maxRetries;
        private final long baseDelayMs;
        private final double backoffMultiplier;
        private final long maxDelayMs;
        private final boolean jitterEnabled;
        private final long jitterMaxMs;
        private final Duration operationTimeout;
        
        public ResilienceConfig(int maxRetries, long baseDelayMs, double backoffMultiplier, 
                              long maxDelayMs, boolean jitterEnabled, long jitterMaxMs,
                              Duration operationTimeout) {
            this.maxRetries = maxRetries;
            this.baseDelayMs = baseDelayMs;
            this.backoffMultiplier = backoffMultiplier;
            this.maxDelayMs = maxDelayMs;
            this.jitterEnabled = jitterEnabled;
            this.jitterMaxMs = jitterMaxMs;
            this.operationTimeout = operationTimeout;
        }
        
        public static ResilienceConfig defaultConfig() {
            return new ResilienceConfig(
                3,                              // maxRetries
                1000,                           // baseDelayMs
                2.0,                            // backoffMultiplier
                30000,                          // maxDelayMs
                true,                           // jitterEnabled
                1000,                           // jitterMaxMs
                Duration.ofMinutes(5)           // operationTimeout
            );
        }
        
        // Getters
        public int getMaxRetries() {
            return maxRetries;
        }
        
        public long getBaseDelayMs() {
            return baseDelayMs;
        }
        
        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }
        
        public long getMaxDelayMs() {
            return maxDelayMs;
        }
        
        public boolean isJitterEnabled() {
            return jitterEnabled;
        }
        
        public long getJitterMaxMs() {
            return jitterMaxMs;
        }
        
        public Duration getOperationTimeout() {
            return operationTimeout;
        }
        
        @Override
        public String toString() {
            return String.format("ResilienceConfig{maxRetries=%d, baseDelay=%dms, backoff=%.1f, maxDelay=%dms, jitter=%s, timeout=%s}",
                               maxRetries, baseDelayMs, backoffMultiplier, maxDelayMs, jitterEnabled, operationTimeout);
        }
    }
    
    // Circuit breaker state management
    public static class CircuitBreakerState {
        private volatile int failureCount = 0;
        private volatile long lastFailureTime = 0;
        private final int failureThreshold;
        private final Duration cooldownPeriod;
        
        public CircuitBreakerState(int failureThreshold, Duration cooldownPeriod) {
            this.failureThreshold = failureThreshold;
            this.cooldownPeriod = cooldownPeriod;
        }
        
        public boolean isOpen() {
            if (failureCount >= failureThreshold) {
                long timeSinceLastFailure = System.currentTimeMillis() - lastFailureTime;
                return timeSinceLastFailure < cooldownPeriod.toMillis();
            }
            return false;
        }
        
        public void recordFailure() {
            failureCount++;
            lastFailureTime = System.currentTimeMillis();
        }
        
        public void recordSuccess() {
            failureCount = 0;
            lastFailureTime = 0;
        }
    }
    
    // Custom exceptions
    public static class OperationTimeoutException extends RuntimeException {
        public OperationTimeoutException(String message) {
            super(message);
        }
    }
    
    public static class MaxRetriesExceededException extends RuntimeException {
        public MaxRetriesExceededException(String message, Throwable cause) {
            super(message, cause);
        }
    }
    
    public static class CircuitBreakerOpenException extends RuntimeException {
        public CircuitBreakerOpenException(String message) {
            super(message);
        }
    }
}