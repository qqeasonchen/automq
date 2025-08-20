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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scheduler for coordinating and executing health checks
 * Manages periodic health check execution and result aggregation
 */
public class HealthCheckScheduler {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckScheduler.class);
    
    private final List<HealthCheck> healthChecks = new CopyOnWriteArrayList<>();
    private final List<HealthCheckListener> listeners = new CopyOnWriteArrayList<>();
    private final Map<String, HealthCheckResult> lastResults = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService scheduler;
    private final long intervalMs;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicLong lastExecutionTime = new AtomicLong(0);
    private final AtomicLong executionCount = new AtomicLong(0);
    
    // Thread pool for parallel health check execution
    private final ScheduledExecutorService healthCheckExecutor;
    
    public HealthCheckScheduler(long intervalMs) {
        this(intervalMs, 2, 4); // 2 scheduler threads, 4 health check threads
    }
    
    public HealthCheckScheduler(long intervalMs, int schedulerThreads, int healthCheckThreads) {
        this.intervalMs = intervalMs;
        this.scheduler = Executors.newScheduledThreadPool(schedulerThreads, r -> {
            Thread t = new Thread(r, "health-check-scheduler");
            t.setDaemon(true);
            return t;
        });
        this.healthCheckExecutor = Executors.newScheduledThreadPool(healthCheckThreads, r -> {
            Thread t = new Thread(r, "health-check-executor");
            t.setDaemon(true);
            return t;
        });
    }
    
    /**
     * Add a health check to be monitored
     */
    public void addHealthCheck(HealthCheck healthCheck) {
        if (healthCheck != null) {
            healthChecks.add(healthCheck);
            LOGGER.info("Added health check: {} (enabled: {})", 
                       healthCheck.getName(), healthCheck.isEnabled());
        }
    }
    
    /**
     * Remove a health check from monitoring
     */
    public void removeHealthCheck(HealthCheck healthCheck) {
        if (healthCheck != null) {
            healthChecks.remove(healthCheck);
            lastResults.remove(healthCheck.getName());
            LOGGER.info("Removed health check: {}", healthCheck.getName());
        }
    }
    
    /**
     * Add a listener for health check results
     */
    public void addListener(HealthCheckListener listener) {
        if (listener != null) {
            listeners.add(listener);
            LOGGER.debug("Added health check listener: {}", listener.getClass().getSimpleName());
        }
    }
    
    /**
     * Remove a health check result listener
     */
    public void removeListener(HealthCheckListener listener) {
        if (listener != null) {
            listeners.remove(listener);
            LOGGER.debug("Removed health check listener: {}", listener.getClass().getSimpleName());
        }
    }
    
    /**
     * Start the health check scheduler
     */
    public void start() {
        if (started.compareAndSet(false, true)) {
            LOGGER.info("Starting health check scheduler with interval {}ms", intervalMs);
            
            scheduler.scheduleWithFixedDelay(
                this::executeHealthChecks,
                0, // Start immediately
                intervalMs,
                TimeUnit.MILLISECONDS
            );
            
            LOGGER.info("Health check scheduler started with {} health checks", healthChecks.size());
        }
    }
    
    /**
     * Stop the health check scheduler
     */
    public void stop() {
        if (started.compareAndSet(true, false)) {
            LOGGER.info("Stopping health check scheduler");
            
            scheduler.shutdown();
            healthCheckExecutor.shutdown();
            
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                if (!healthCheckExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                    healthCheckExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
                healthCheckExecutor.shutdownNow();
            }
            
            LOGGER.info("Health check scheduler stopped");
        }
    }
    
    /**
     * Execute all health checks immediately
     */
    public CompletableFuture<Map<String, HealthCheckResult>> executeNow() {
        LOGGER.debug("Executing health checks on demand");
        return executeHealthChecksAsync();
    }
    
    /**
     * Get the last result for a specific health check
     */
    public HealthCheckResult getLastResult(String healthCheckName) {
        return lastResults.get(healthCheckName);
    }
    
    /**
     * Get all last results
     */
    public Map<String, HealthCheckResult> getAllLastResults() {
        return new ConcurrentHashMap<>(lastResults);
    }
    
    /**
     * Get scheduler statistics
     */
    public SchedulerStats getStats() {
        return new SchedulerStats(
            started.get(),
            healthChecks.size(),
            listeners.size(),
            lastExecutionTime.get(),
            executionCount.get(),
            intervalMs
        );
    }
    
    private void executeHealthChecks() {
        try {
            executeHealthChecksAsync().thenAccept(results -> {
                lastExecutionTime.set(System.currentTimeMillis());
                executionCount.incrementAndGet();
            }).exceptionally(throwable -> {
                LOGGER.error("Health check execution failed", throwable);
                return null;
            });
        } catch (Exception e) {
            LOGGER.error("Error during health check execution", e);
        }
    }
    
    private CompletableFuture<Map<String, HealthCheckResult>> executeHealthChecksAsync() {
        if (healthChecks.isEmpty()) {
            LOGGER.debug("No health checks to execute");
            return CompletableFuture.completedFuture(new ConcurrentHashMap<>());
        }
        
        long startTime = System.currentTimeMillis();
        LOGGER.debug("Executing {} health checks", healthChecks.size());
        
        // Execute all health checks in parallel
        List<CompletableFuture<Void>> futures = healthChecks.stream()
            .filter(HealthCheck::isEnabled)
            .map(this::executeHealthCheckAsync)
            .collect(java.util.stream.Collectors.toList());
        
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenApply(unused -> {
                long duration = System.currentTimeMillis() - startTime;
                LOGGER.debug("Health check execution completed in {}ms", duration);
                
                // Notify listeners about completion
                notifyListenersOfExecution(getAllLastResults(), duration);
                
                return getAllLastResults();
            })
            .exceptionally(throwable -> {
                LOGGER.warn("Some health checks failed during execution", throwable);
                return getAllLastResults();
            });
    }
    
    private CompletableFuture<Void> executeHealthCheckAsync(HealthCheck healthCheck) {
        return CompletableFuture
            .supplyAsync(() -> {
                try {
                    LOGGER.debug("Executing health check: {}", healthCheck.getName());
                    return healthCheck.check().get();
                } catch (Exception e) {
                    LOGGER.warn("Health check {} failed", healthCheck.getName(), e);
                    return HealthCheckResult.unhealthy(
                        "Health check execution failed: " + e.getMessage(), e
                    );
                }
            }, healthCheckExecutor)
            .thenAccept(result -> {
                // Store result
                HealthCheckResult previousResult = lastResults.put(healthCheck.getName(), result);
                
                LOGGER.debug("Health check {} completed: {} (duration: {}ms)", 
                           healthCheck.getName(), result.getStatus(), result.getDurationMs());
                
                // Notify listeners
                notifyListenersOfResult(healthCheck.getName(), result, previousResult);
            })
            .exceptionally(throwable -> {
                LOGGER.error("Unexpected error executing health check {}", healthCheck.getName(), throwable);
                
                HealthCheckResult errorResult = HealthCheckResult.unhealthy(
                    "Unexpected execution error: " + throwable.getMessage(), throwable
                );
                lastResults.put(healthCheck.getName(), errorResult);
                
                return null;
            });
    }
    
    private void notifyListenersOfResult(String healthCheckName, HealthCheckResult result, 
                                        HealthCheckResult previousResult) {
        for (HealthCheckListener listener : listeners) {
            try {
                listener.onHealthCheckResult(healthCheckName, result);
                
                // Check for status changes
                if (previousResult != null && previousResult.getStatus() != result.getStatus()) {
                    listener.onHealthStatusChanged(
                        healthCheckName, 
                        previousResult.getStatus(), 
                        result.getStatus(), 
                        result
                    );
                }
            } catch (Exception e) {
                LOGGER.warn("Health check listener {} failed to process result for {}", 
                          listener.getClass().getSimpleName(), healthCheckName, e);
            }
        }
    }
    
    private void notifyListenersOfExecution(Map<String, HealthCheckResult> allResults, long durationMs) {
        for (HealthCheckListener listener : listeners) {
            try {
                listener.onHealthCheckExecution(allResults, durationMs);
            } catch (Exception e) {
                LOGGER.warn("Health check listener {} failed to process execution completion", 
                          listener.getClass().getSimpleName(), e);
            }
        }
    }
    
    /**
     * Listener interface for health check events
     */
    public interface HealthCheckListener {
        
        /**
         * Called when a health check produces a result
         */
        default void onHealthCheckResult(String healthCheckName, HealthCheckResult result) {
            // Default implementation does nothing
        }
        
        /**
         * Called when a health check status changes
         */
        default void onHealthStatusChanged(String healthCheckName, 
                                         HealthCheckResult.Status previousStatus,
                                         HealthCheckResult.Status newStatus,
                                         HealthCheckResult result) {
            // Default implementation does nothing
        }
        
        /**
         * Called when a full health check execution cycle completes
         */
        default void onHealthCheckExecution(Map<String, HealthCheckResult> allResults, long durationMs) {
            // Default implementation does nothing
        }
    }
    
    /**
     * Statistics about the health check scheduler
     */
    public static class SchedulerStats {
        private final boolean running;
        private final int healthCheckCount;
        private final int listenerCount;
        private final long lastExecutionTime;
        private final long executionCount;
        private final long intervalMs;
        
        public SchedulerStats(boolean running, int healthCheckCount, int listenerCount,
                             long lastExecutionTime, long executionCount, long intervalMs) {
            this.running = running;
            this.healthCheckCount = healthCheckCount;
            this.listenerCount = listenerCount;
            this.lastExecutionTime = lastExecutionTime;
            this.executionCount = executionCount;
            this.intervalMs = intervalMs;
        }
        
        public boolean isRunning() { return running; }
        public int getHealthCheckCount() { return healthCheckCount; }
        public int getListenerCount() { return listenerCount; }
        public long getLastExecutionTime() { return lastExecutionTime; }
        public long getExecutionCount() { return executionCount; }
        public long getIntervalMs() { return intervalMs; }
        
        @Override
        public String toString() {
            return String.format(
                "SchedulerStats{running=%s, healthChecks=%d, listeners=%d, executions=%d, interval=%dms, lastExecution=%d}",
                running, healthCheckCount, listenerCount, executionCount, intervalMs, lastExecutionTime
            );
        }
    }
}