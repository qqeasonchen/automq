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

import com.automq.stream.s3.Config;

/**
 * Configuration for a single replica in the quorum
 */
public class ReplicaConfig {
    private final int replicaId;
    private final String region;
    private final String bucket;
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final Config s3Config;
    private final ReplicaRole role;
    private final long priority;

    public ReplicaConfig(int replicaId, String region, String bucket, String endpoint,
                        String accessKey, String secretKey, Config s3Config,
                        ReplicaRole role, long priority) {
        this.replicaId = replicaId;
        this.region = region;
        this.bucket = bucket;
        this.endpoint = endpoint;
        this.accessKey = accessKey;
        this.secretKey = secretKey;
        this.s3Config = s3Config;
        this.role = role;
        this.priority = priority;
    }

    public int getReplicaId() {
        return replicaId;
    }

    public String getRegion() {
        return region;
    }

    public String getBucket() {
        return bucket;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public Config getS3Config() {
        return s3Config;
    }

    public ReplicaRole getRole() {
        return role;
    }

    public long getPriority() {
        return priority;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private int replicaId;
        private String region;
        private String bucket;
        private String endpoint;
        private String accessKey;
        private String secretKey;
        private Config s3Config;
        private ReplicaRole role = ReplicaRole.SECONDARY;
        private long priority = 0;

        public Builder replicaId(int replicaId) {
            this.replicaId = replicaId;
            return this;
        }

        public Builder region(String region) {
            this.region = region;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder accessKey(String accessKey) {
            this.accessKey = accessKey;
            return this;
        }

        public Builder secretKey(String secretKey) {
            this.secretKey = secretKey;
            return this;
        }

        public Builder s3Config(Config s3Config) {
            this.s3Config = s3Config;
            return this;
        }

        public Builder role(ReplicaRole role) {
            this.role = role;
            return this;
        }

        public Builder priority(long priority) {
            this.priority = priority;
            return this;
        }

        public ReplicaConfig build() {
            return new ReplicaConfig(replicaId, region, bucket, endpoint,
                                   accessKey, secretKey, s3Config, role, priority);
        }
    }

    /**
     * Replica role in the quorum
     */
    public enum ReplicaRole {
        PRIMARY,    // Primary replica, highest priority for reads
        SECONDARY   // Secondary replica, backup for reads and writes
    }
} 