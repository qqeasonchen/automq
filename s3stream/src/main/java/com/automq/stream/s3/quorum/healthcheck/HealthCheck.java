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

import java.util.concurrent.CompletableFuture;

/**
 * Base interface for health check operations
 * Defines the contract for checking system component health
 */
public interface HealthCheck {
    
    /**
     * Perform a health check operation
     * 
     * @return CompletableFuture containing the health check result
     */
    CompletableFuture<HealthCheckResult> check();
    
    /**
     * Get a human-readable name for this health check
     * 
     * @return the name of this health check
     */
    String getName();
    
    /**
     * Get the timeout for this health check in milliseconds
     * 
     * @return timeout in milliseconds
     */
    long getTimeoutMs();
    
    /**
     * Check if this health check is enabled
     * 
     * @return true if enabled, false otherwise
     */
    default boolean isEnabled() {
        return true;
    }
}