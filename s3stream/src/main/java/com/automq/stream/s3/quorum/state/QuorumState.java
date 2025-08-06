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

package com.automq.stream.s3.quorum.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Manages the state of replicas in the quorum
 */
public class QuorumState {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumState.class);
    
    private final int quorumSize;
    private final AtomicLongArray replicaFailureCounts;
    private final AtomicLongArray replicaLastSuccessTime;
    private final AtomicLongArray replicaLastFailureTime;
    private final AtomicBoolean[] replicaHealthStatus;
    private final AtomicInteger healthyReplicaCount;
    private volatile boolean started = false;

    public QuorumState(int quorumSize) {
        this.quorumSize = quorumSize;
        this.replicaFailureCounts = new AtomicLongArray(quorumSize);
        this.replicaLastSuccessTime = new AtomicLongArray(quorumSize);
        this.replicaLastFailureTime = new AtomicLongArray(quorumSize);
        this.replicaHealthStatus = new AtomicBoolean[quorumSize];
        this.healthyReplicaCount = new AtomicInteger(quorumSize);
        
        for (int i = 0; i < quorumSize; i++) {
            replicaHealthStatus[i] = new AtomicBoolean(true);
            replicaLastSuccessTime.set(i, System.currentTimeMillis());
        }
    }

    public void startup() {
        started = true;
        LOGGER.info("QuorumState started with {} replicas", quorumSize);
    }

    public void shutdown() {
        started = false;
        LOGGER.info("QuorumState shutdown");
    }

    public void markReplicaSuccess(int replicaIndex) {
        if (!started || replicaIndex < 0 || replicaIndex >= quorumSize) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        replicaLastSuccessTime.set(replicaIndex, currentTime);
        
        if (!replicaHealthStatus[replicaIndex].get()) {
            replicaHealthStatus[replicaIndex].set(true);
            int healthyCount = healthyReplicaCount.incrementAndGet();
            LOGGER.info("Replica {} recovered, healthy replicas: {}", replicaIndex, healthyCount);
        }
    }

    public void markReplicaFailed(int replicaIndex) {
        if (!started || replicaIndex < 0 || replicaIndex >= quorumSize) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        replicaLastFailureTime.set(replicaIndex, currentTime);
        replicaFailureCounts.incrementAndGet(replicaIndex);
        
        if (replicaHealthStatus[replicaIndex].get()) {
            replicaHealthStatus[replicaIndex].set(false);
            int healthyCount = healthyReplicaCount.decrementAndGet();
            LOGGER.warn("Replica {} failed, healthy replicas: {}", replicaIndex, healthyCount);
        }
    }

    public boolean isReplicaHealthy(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= quorumSize) {
            return false;
        }
        return replicaHealthStatus[replicaIndex].get();
    }

    public int getHealthyReplicaCount() {
        return healthyReplicaCount.get();
    }

    public long getReplicaFailureCount(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= quorumSize) {
            return 0;
        }
        return replicaFailureCounts.get(replicaIndex);
    }

    public long getReplicaLastSuccessTime(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= quorumSize) {
            return 0;
        }
        return replicaLastSuccessTime.get(replicaIndex);
    }

    public long getReplicaLastFailureTime(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= quorumSize) {
            return 0;
        }
        return replicaLastFailureTime.get(replicaIndex);
    }

    public boolean hasQuorum() {
        return healthyReplicaCount.get() >= (quorumSize / 2 + 1);
    }

    public int[] getHealthyReplicaIndices() {
        int[] healthyIndices = new int[healthyReplicaCount.get()];
        int index = 0;
        for (int i = 0; i < quorumSize; i++) {
            if (replicaHealthStatus[i].get()) {
                healthyIndices[index++] = i;
            }
        }
        return healthyIndices;
    }

    public int[] getUnhealthyReplicaIndices() {
        int unhealthyCount = quorumSize - healthyReplicaCount.get();
        int[] unhealthyIndices = new int[unhealthyCount];
        int index = 0;
        for (int i = 0; i < quorumSize; i++) {
            if (!replicaHealthStatus[i].get()) {
                unhealthyIndices[index++] = i;
            }
        }
        return unhealthyIndices;
    }

    public void resetReplicaFailureCount(int replicaIndex) {
        if (replicaIndex >= 0 && replicaIndex < quorumSize) {
            replicaFailureCounts.set(replicaIndex, 0);
        }
    }

    public void resetAllFailureCounts() {
        for (int i = 0; i < quorumSize; i++) {
            replicaFailureCounts.set(i, 0);
        }
    }
} 