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

package com.automq.stream.utils;

import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Utility methods for CompletableFuture operations
 */
public class FutureUtil {
    
    /**
     * Wait for majority of futures to complete successfully
     * @param futures List of futures to wait for
     * @param majorityCount Number of successful completions required
     * @return CompletableFuture that completes when majority succeeds
     */
    public static <T> CompletableFuture<List<T>> waitForMajority(
            List<CompletableFuture<T>> futures, int majorityCount) {
        
        if (majorityCount > futures.size()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Majority count cannot exceed future count"));
        }
        
        CompletableFuture<List<T>> result = new CompletableFuture<>();
        
        // Count successful and failed completions
        int[] successCount = {0};
        int[] failureCount = {0};
        Object lock = new Object();
        
        for (CompletableFuture<T> future : futures) {
            future.whenComplete((value, ex) -> {
                synchronized (lock) {
                    if (ex != null) {
                        failureCount[0]++;
                        // Check if we can't reach majority anymore
                        if (futures.size() - failureCount[0] < majorityCount) {
                            if (!result.isDone()) {
                                result.completeExceptionally(new RuntimeException(
                                    "Cannot reach majority: " + failureCount[0] + " failures"));
                            }
                        }
                    } else {
                        successCount[0]++;
                        // Check if we have reached majority
                        if (successCount[0] >= majorityCount) {
                            if (!result.isDone()) {
                                // Collect all successful results
                                List<T> results = futures.stream()
                                    .filter(f -> f.isDone() && !f.isCompletedExceptionally())
                                    .map(CompletableFuture::join)
                                    .collect(Collectors.toList());
                                result.complete(results);
                            }
                        }
                    }
                }
            });
        }
        
        return result;
    }
    
    /**
     * Wait for majority of futures to complete (void version)
     * @param futures List of void futures to wait for
     * @param majorityCount Number of successful completions required
     * @return CompletableFuture that completes when majority succeeds
     */
    public static CompletableFuture<Void> waitForMajorityVoid(
            List<CompletableFuture<Void>> futures, int majorityCount) {
        
        return waitForMajority(futures, majorityCount)
            .thenApply(results -> null);
    }
    
    /**
     * Return the first successful future result
     * @param futures List of futures to try
     * @return CompletableFuture that completes with first successful result
     */
    public static <T> CompletableFuture<T> firstSuccess(List<CompletableFuture<T>> futures) {
        if (futures.isEmpty()) {
            return CompletableFuture.failedFuture(
                new IllegalArgumentException("Future list cannot be empty"));
        }
        
        CompletableFuture<T> result = new CompletableFuture<>();
        int[] failureCount = {0};
        int totalFutures = futures.size();
        Object lock = new Object();
        
        for (CompletableFuture<T> future : futures) {
            if (future == null) {
                failureCount[0]++;
                if (failureCount[0] == totalFutures) {
                    if (!result.isDone()) {
                        result.completeExceptionally(new RuntimeException(
                            "All futures failed: " + failureCount[0] + " failures"));
                    }
                }
                continue;
            }
            
            future.whenComplete((value, ex) -> {
                synchronized (lock) {
                    if (ex != null) {
                        failureCount[0]++;
                        // If all futures failed, complete with exception
                        if (failureCount[0] == totalFutures) {
                            if (!result.isDone()) {
                                result.completeExceptionally(new RuntimeException(
                                    "All futures failed: " + failureCount[0] + " failures"));
                            }
                        }
                    } else {
                        // First success, complete the result
                        if (!result.isDone()) {
                            result.complete(value);
                        }
                    }
                }
            });
        }
        
        return result;
    }
    
    /**
     * Add timeout to a future
     * @param future The future to add timeout to
     * @param timeout Timeout duration
     * @param unit Time unit
     * @return Future with timeout
     */
    public static <T> CompletableFuture<T> withTimeout(
            CompletableFuture<T> future, long timeout, TimeUnit unit) {
        
        CompletableFuture<T> result = new CompletableFuture<>();
        
        future.whenComplete((value, ex) -> {
            if (!result.isDone()) {
                if (ex != null) {
                    result.completeExceptionally(ex);
                } else {
                    result.complete(value);
                }
            }
        });
        
        // Schedule timeout
        CompletableFuture.delayedExecutor(timeout, unit).execute(() -> {
            if (!result.isDone()) {
                result.completeExceptionally(new TimeoutException(
                    "Operation timed out after " + timeout + " " + unit));
            }
        });
        
        return result;
    }

    /**
     * Suppress exceptions from a runnable
     * @param runnable The runnable to execute
     * @param logger Logger to log exceptions
     */
    public static void suppress(Runnable runnable, Logger logger) {
        try {
            runnable.run();
        } catch (Exception e) {
            logger.warn("Suppressed exception", e);
        }
    }

    /**
     * Get the cause of an exception
     * @param ex The exception
     * @return The cause
     */
    public static Throwable cause(Throwable ex) {
        if (ex == null) {
            return null;
        }
        Throwable cause = ex;
        while (cause.getCause() != null && cause != cause.getCause()) {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * Add timeout to a future with custom return value
     * @param future The future to add timeout to
     * @param timeout Timeout duration
     * @param unit Time unit
     * @param supplier Supplier for return value on timeout
     * @return Future with timeout
     */
    public static <T> CompletableFuture<T> timeoutWithNewReturn(
            CompletableFuture<T> future, long timeout, TimeUnit unit, 
            java.util.function.Supplier<T> supplier) {
        
        CompletableFuture<T> result = new CompletableFuture<>();
        
        future.whenComplete((value, ex) -> {
            if (!result.isDone()) {
                if (ex != null) {
                    result.completeExceptionally(ex);
                } else {
                    result.complete(value);
                }
            }
        });
        
        // Schedule timeout
        CompletableFuture.delayedExecutor(timeout, unit).execute(() -> {
            if (!result.isDone()) {
                result.complete(supplier.get());
            }
        });
        
        return result;
    }

    /**
     * Complete all futures with a value
     * @param futures Iterator of futures to complete
     * @param value Value to complete with
     */
    public static <T> void complete(java.util.Iterator<CompletableFuture<T>> futures, T value) {
        while (futures.hasNext()) {
            futures.next().complete(value);
        }
    }

    /**
     * Complete all futures exceptionally
     * @param futures Iterator of futures to complete
     * @param ex Exception to complete with
     */
    public static <T> void completeExceptionally(java.util.Iterator<CompletableFuture<T>> futures, Throwable ex) {
        while (futures.hasNext()) {
            futures.next().completeExceptionally(ex);
        }
    }
    
    /**
     * Execute a task with logging
     * @param task The task to execute
     * @param logger Logger for error logging
     * @param operationName Name of the operation for logging
     * @return Future result
     */
    public static <T> CompletableFuture<T> exec(
            java.util.function.Supplier<CompletableFuture<T>> task,
            Logger logger, String operationName) {
        
        try {
            return task.get();
        } catch (Exception e) {
            logger.error("Error executing {}", operationName, e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Execute a task with logging and propagate to target future
     * @param task The task to execute
     * @param target Target future to propagate result to
     * @param logger Logger for error logging
     * @param operationName Name of the operation for logging
     */
    public static <T> void exec(
            java.util.function.Supplier<CompletableFuture<T>> task,
            CompletableFuture<T> target,
            Logger logger, String operationName) {
        
        try {
            task.get().whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("Error executing {}", operationName, ex);
                    target.completeExceptionally(ex);
                } else {
                    target.complete(result);
                }
            });
        } catch (Exception e) {
            logger.error("Error executing {}", operationName, e);
            target.completeExceptionally(e);
        }
    }
    
    /**
     * Propagate exception from one future to another
     * @param source Source future
     * @param target Target future
     */
    public static <T> void propagate(CompletableFuture<T> source, CompletableFuture<T> target) {
        source.whenComplete((value, ex) -> {
            if (!target.isDone()) {
                if (ex != null) {
                    target.completeExceptionally(ex);
                } else {
                    target.complete(value);
                }
            }
        });
    }
    
    /**
     * Create a failed future with the given exception
     * @param ex The exception
     * @return Failed future
     */
    public static <T> CompletableFuture<T> failedFuture(Throwable ex) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(ex);
        return future;
    }
}
