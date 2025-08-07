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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

/**
 * Loader for Quorum configuration from properties file
 */
public class QuorumConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumConfigLoader.class);

    /**
     * Load quorum configuration from properties file
     */
    public static QuorumConfig loadFromProperties(String configPath, Config baseConfig) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(configPath)) {
            props.load(input);
        }

        // Check if quorum is enabled
        boolean enabled = Boolean.parseBoolean(props.getProperty("automq.s3.quorum.enabled", "false"));
        if (!enabled) {
            throw new IllegalStateException("Quorum storage is not enabled in configuration");
        }

        // Load quorum configuration
        int quorumSize = Integer.parseInt(props.getProperty("automq.s3.quorum.size", "3"));
        int writeQuorumSize = Integer.parseInt(props.getProperty("automq.s3.quorum.write.quorum.size", "2"));
        int readQuorumSize = Integer.parseInt(props.getProperty("automq.s3.quorum.read.quorum.size", "1"));
        long writeTimeoutMs = Long.parseLong(props.getProperty("automq.s3.quorum.write.timeout.ms", "30000"));
        long readTimeoutMs = Long.parseLong(props.getProperty("automq.s3.quorum.read.timeout.ms", "10000"));
        boolean enableReadRepair = Boolean.parseBoolean(props.getProperty("automq.s3.quorum.read.repair.enabled", "true"));
        long readRepairTimeoutMs = Long.parseLong(props.getProperty("automq.s3.quorum.read.repair.timeout.ms", "5000"));

        // Load replica configurations
        List<ReplicaConfig> replicaConfigs = new ArrayList<>();
        for (int i = 0; i < quorumSize; i++) {
            ReplicaConfig replicaConfig = loadReplicaConfig(props, i, baseConfig);
            replicaConfigs.add(replicaConfig);
            LOGGER.info("Loaded replica {} configuration: region={}, bucket={}, role={}", 
                       i, replicaConfig.getRegion(), replicaConfig.getBucket(), replicaConfig.getRole());
        }

        return QuorumConfig.builder()
            .quorumSize(quorumSize)
            .writeQuorumSize(writeQuorumSize)
            .readQuorumSize(readQuorumSize)
            .writeTimeoutMs(writeTimeoutMs)
            .readTimeoutMs(readTimeoutMs)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(enableReadRepair)
            .readRepairTimeoutMs(readRepairTimeoutMs)
            .build();
    }

    /**
     * Load replica configuration for a specific replica index
     */
    private static ReplicaConfig loadReplicaConfig(Properties props, int replicaIndex, Config baseConfig) {
        String prefix = "automq.s3.quorum.replica." + replicaIndex + ".";
        
        int replicaId = Integer.parseInt(props.getProperty(prefix + "id", String.valueOf(replicaIndex)));
        String region = props.getProperty(prefix + "region");
        String bucket = props.getProperty(prefix + "bucket");
        String endpoint = props.getProperty(prefix + "endpoint");
        String roleStr = props.getProperty(prefix + "role", "SECONDARY");
        long priority = Long.parseLong(props.getProperty(prefix + "priority", "0"));
        
        // Get AWS credentials from environment variables
        String accessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String secretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        
        if (accessKey == null || secretKey == null) {
            throw new IllegalStateException("AWS credentials not found in environment variables");
        }

        ReplicaConfig.ReplicaRole role = ReplicaConfig.ReplicaRole.valueOf(roleStr.toUpperCase(Locale.ROOT));
        
        // Create S3 config for this replica
        Config replicaS3Config = createReplicaS3Config(baseConfig, region);
        
        return ReplicaConfig.builder()
            .replicaId(replicaId)
            .region(region)
            .bucket(bucket)
            .endpoint(endpoint)
            .accessKey(accessKey)
            .secretKey(secretKey)
            .s3Config(replicaS3Config)
            .role(role)
            .priority(priority)
            .build();
    }

    /**
     * Create S3 configuration for a specific replica
     */
    private static Config createReplicaS3Config(Config baseConfig, String region) {
        Config replicaConfig = new Config();
        
        // Copy base configuration
        replicaConfig.nodeId(baseConfig.nodeId());
        replicaConfig.walCacheSize(baseConfig.walCacheSize());
        replicaConfig.walUploadThreshold(baseConfig.walUploadThreshold());
        replicaConfig.walUploadIntervalMs(baseConfig.walUploadIntervalMs());
        replicaConfig.streamSplitSize(baseConfig.streamSplitSize());
        replicaConfig.objectBlockSize(baseConfig.objectBlockSize());
        replicaConfig.objectPartSize(baseConfig.objectPartSize());
        replicaConfig.blockCacheSize(baseConfig.blockCacheSize());
        replicaConfig.streamObjectCompactionIntervalMinutes(baseConfig.streamObjectCompactionIntervalMinutes());
        replicaConfig.streamObjectCompactionMaxSizeBytes(baseConfig.streamObjectCompactionMaxSizeBytes());
        replicaConfig.controllerRequestRetryMaxCount(baseConfig.controllerRequestRetryMaxCount());
        replicaConfig.controllerRequestRetryBaseDelayMs(baseConfig.controllerRequestRetryBaseDelayMs());
        replicaConfig.nodeEpoch(baseConfig.nodeEpoch());
        replicaConfig.streamSetObjectCompactionInterval(baseConfig.streamSetObjectCompactionInterval());
        replicaConfig.streamSetObjectCompactionCacheSize(baseConfig.streamSetObjectCompactionCacheSize());
        replicaConfig.streamSetObjectCompactionUploadConcurrency(baseConfig.streamSetObjectCompactionUploadConcurrency());
        replicaConfig.streamSetObjectCompactionStreamSplitSize(baseConfig.streamSetObjectCompactionStreamSplitSize());
        replicaConfig.streamSetObjectCompactionForceSplitPeriod(baseConfig.streamSetObjectCompactionForceSplitPeriod());
        replicaConfig.streamSetObjectCompactionMaxObjectNum(baseConfig.streamSetObjectCompactionMaxObjectNum());
        replicaConfig.maxStreamNumPerStreamSetObject(baseConfig.maxStreamNumPerStreamSetObject());
        replicaConfig.maxStreamObjectNumPerCommit(baseConfig.maxStreamObjectNumPerCommit());
        replicaConfig.mockEnable(baseConfig.mockEnable());
        replicaConfig.networkBaselineBandwidth(baseConfig.networkBaselineBandwidth());
        replicaConfig.refillPeriodMs(baseConfig.refillPeriodMs());
        replicaConfig.objectRetentionTimeInSecond(baseConfig.objectRetentionTimeInSecond());
        replicaConfig.failoverEnable(baseConfig.failoverEnable());
        replicaConfig.snapshotReadEnable(baseConfig.snapshotReadEnable());
        
        // Set region-specific configuration
        replicaConfig.walConfig("0@file:///tmp/s3stream_wal_" + region);
        
        return replicaConfig;
    }

    /**
     * Check if quorum storage is enabled in configuration
     */
    public static boolean isQuorumEnabled(String configPath) {
        try {
            Properties props = new Properties();
            try (InputStream input = new FileInputStream(configPath)) {
                props.load(input);
            }
            return Boolean.parseBoolean(props.getProperty("automq.s3.quorum.enabled", "false"));
        } catch (IOException e) {
            LOGGER.warn("Failed to check quorum configuration: {}", e.getMessage());
            return false;
        }
    }
} 