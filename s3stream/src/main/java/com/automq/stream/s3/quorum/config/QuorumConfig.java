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

package com.automq.stream.s3.quorum.config;

import java.util.List;

/**
 * Configuration for S3 Quorum Storage
 */
public class QuorumConfig {
    private final int quorumSize;
    private final int writeQuorumSize;
    private final int readQuorumSize;
    private final long writeTimeoutMs;
    private final long readTimeoutMs;
    private final List<ReplicaConfig> replicaConfigs;
    private final boolean enableReadRepair;
    private final long readRepairTimeoutMs;

    public QuorumConfig(int quorumSize, int writeQuorumSize, int readQuorumSize, 
                       long writeTimeoutMs, long readTimeoutMs, 
                       List<ReplicaConfig> replicaConfigs,
                       boolean enableReadRepair, long readRepairTimeoutMs) {
        this.quorumSize = quorumSize;
        this.writeQuorumSize = writeQuorumSize;
        this.readQuorumSize = readQuorumSize;
        this.writeTimeoutMs = writeTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.replicaConfigs = replicaConfigs;
        this.enableReadRepair = enableReadRepair;
        this.readRepairTimeoutMs = readRepairTimeoutMs;
        
        validate();
    }

    private void validate() {
        if (quorumSize < 3) {
            throw new IllegalArgumentException("Quorum size must be at least 3");
        }
        if (writeQuorumSize > quorumSize) {
            throw new IllegalArgumentException("Write quorum size cannot exceed quorum size");
        }
        if (readQuorumSize > quorumSize) {
            throw new IllegalArgumentException("Read quorum size cannot exceed quorum size");
        }
        if (replicaConfigs.size() != quorumSize) {
            throw new IllegalArgumentException("Replica configs count must match quorum size");
        }
    }

    public int getQuorumSize() {
        return quorumSize;
    }

    public int getWriteQuorumSize() {
        return writeQuorumSize;
    }

    public int getReadQuorumSize() {
        return readQuorumSize;
    }

    public long getWriteTimeoutMs() {
        return writeTimeoutMs;
    }

    public long getReadTimeoutMs() {
        return readTimeoutMs;
    }

    public List<ReplicaConfig> getReplicaConfigs() {
        return replicaConfigs;
    }

    public boolean isEnableReadRepair() {
        return enableReadRepair;
    }

    public long getReadRepairTimeoutMs() {
        return readRepairTimeoutMs;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int quorumSize = 3;
        private int writeQuorumSize = 2; // Majority
        private int readQuorumSize = 1;  // Any replica
        private long writeTimeoutMs = 30000; // 30 seconds
        private long readTimeoutMs = 10000;  // 10 seconds
        private List<ReplicaConfig> replicaConfigs;
        private boolean enableReadRepair = true;
        private long readRepairTimeoutMs = 5000; // 5 seconds

        public Builder quorumSize(int quorumSize) {
            this.quorumSize = quorumSize;
            return this;
        }

        public Builder writeQuorumSize(int writeQuorumSize) {
            this.writeQuorumSize = writeQuorumSize;
            return this;
        }

        public Builder readQuorumSize(int readQuorumSize) {
            this.readQuorumSize = readQuorumSize;
            return this;
        }

        public Builder writeTimeoutMs(long writeTimeoutMs) {
            this.writeTimeoutMs = writeTimeoutMs;
            return this;
        }

        public Builder readTimeoutMs(long readTimeoutMs) {
            this.readTimeoutMs = readTimeoutMs;
            return this;
        }

        public Builder replicaConfigs(List<ReplicaConfig> replicaConfigs) {
            this.replicaConfigs = replicaConfigs;
            return this;
        }

        public Builder enableReadRepair(boolean enableReadRepair) {
            this.enableReadRepair = enableReadRepair;
            return this;
        }

        public Builder readRepairTimeoutMs(long readRepairTimeoutMs) {
            this.readRepairTimeoutMs = readRepairTimeoutMs;
            return this;
        }

        public QuorumConfig build() {
            return new QuorumConfig(quorumSize, writeQuorumSize, readQuorumSize,
                                  writeTimeoutMs, readTimeoutMs, replicaConfigs,
                                  enableReadRepair, readRepairTimeoutMs);
        }
    }
} 