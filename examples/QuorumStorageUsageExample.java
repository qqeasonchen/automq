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

package com.automq.stream.s3.quorum;

import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.factory.S3QuorumStorageFactory;
import com.automq.stream.s3.Config;

import io.netty.buffer.Unpooled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating how to use AutoMQ S3 Quorum Storage
 */
public class QuorumStorageUsageExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumStorageUsageExample.class);

    public static void main(String[] args) {
        try {
            // Create base S3 configuration
            Config baseConfig = new Config();
            baseConfig.nodeId(1);
            baseConfig.walCacheSize(200 * 1024 * 1024); // 200MB
            baseConfig.walUploadThreshold(100 * 1024 * 1024); // 100MB
            baseConfig.streamSplitSize(16 * 1024 * 1024); // 16MB
            baseConfig.objectBlockSize(1024 * 1024); // 1MB
            baseConfig.blockCacheSize(100 * 1024 * 1024); // 100MB

            // Create quorum storage
            S3QuorumStorage quorumStorage = createQuorumStorage(baseConfig);

            // Start the quorum storage
            quorumStorage.startup();
            LOGGER.info("Quorum storage started successfully");

            // Example 1: Write data with quorum
            writeDataExample(quorumStorage);

            // Example 2: Read data with fault tolerance
            readDataExample(quorumStorage);

            // Example 3: Monitor quorum state
            monitorQuorumState(quorumStorage);

            // Example 4: Force upload to all replicas
            forceUploadExample(quorumStorage);

            // Shutdown the quorum storage
            quorumStorage.shutdown();
            LOGGER.info("Quorum storage shutdown successfully");

        } catch (Exception e) {
            LOGGER.error("Error in quorum storage example", e);
        }
    }

    private static S3QuorumStorage createQuorumStorage(Config baseConfig) {
        // Create replica configurations for 3 replicas
        List<ReplicaConfig> replicaConfigs = new ArrayList<>();
        
        // Primary replica (us-east-1)
        replicaConfigs.add(ReplicaConfig.builder()
            .replicaId(0)
            .region("us-east-1")
            .bucket("automq-primary-bucket")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .s3Config(baseConfig)
            .role(ReplicaConfig.ReplicaRole.PRIMARY)
            .priority(100)
            .build());

        // Secondary replica 1 (us-west-2)
        replicaConfigs.add(ReplicaConfig.builder()
            .replicaId(1)
            .region("us-west-2")
            .bucket("automq-secondary1-bucket")
            .endpoint("https://s3.us-west-2.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .s3Config(baseConfig)
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(50)
            .build());

        // Secondary replica 2 (eu-west-1)
        replicaConfigs.add(ReplicaConfig.builder()
            .replicaId(2)
            .region("eu-west-1")
            .bucket("automq-secondary2-bucket")
            .endpoint("https://s3.eu-west-1.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .s3Config(baseConfig)
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(25)
            .build());

        // Create quorum configuration
        QuorumConfig quorumConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2) // Majority (2 out of 3)
            .readQuorumSize(1)  // Any replica
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();

        // Create quorum storage using factory
        return S3QuorumStorageFactory.createQuorumStorage(
            baseConfig, null, null, null, null);
    }

    private static void writeDataExample(S3QuorumStorage quorumStorage) 
            throws ExecutionException, InterruptedException {
        LOGGER.info("=== Write Data Example ===");

        // Create test data
        String data = "Hello AutoMQ Quorum Storage!";
        StreamRecordBatch recordBatch = new StreamRecordBatch(
            1L, // streamId
            1L, // baseOffset
            0L, // lastOffset
            data.length(),
            Unpooled.wrappedBuffer(data.getBytes())
        );

        AppendContext context = AppendContext.DEFAULT;

        // Write data with quorum (waits for majority to succeed)
        CompletableFuture<Void> writeFuture = quorumStorage.append(context, recordBatch);
        
        // Wait for completion
        writeFuture.get(30, TimeUnit.SECONDS);
        
        LOGGER.info("Data written successfully with quorum");
    }

    private static void readDataExample(S3QuorumStorage quorumStorage) 
            throws ExecutionException, InterruptedException {
        LOGGER.info("=== Read Data Example ===");

        FetchContext context = FetchContext.DEFAULT;
        long streamId = 1L;
        long startOffset = 0L;
        long endOffset = 100L;
        int maxBytes = 1024;

        // Read data with fault tolerance (tries primary, falls back to secondary)
        CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
            context, streamId, startOffset, endOffset, maxBytes);
        
        // Wait for completion
        ReadDataBlock result = readFuture.get(30, TimeUnit.SECONDS);
        
        if (result != null) {
            LOGGER.info("Data read successfully: {} records", result.getRecords().size());
        } else {
            LOGGER.info("No data found for the specified range");
        }
    }

    private static void monitorQuorumState(S3QuorumStorage quorumStorage) {
        LOGGER.info("=== Monitor Quorum State ===");

        QuorumState quorumState = quorumStorage.getQuorumState();
        
        LOGGER.info("Healthy replica count: {}", quorumState.getHealthyReplicaCount());
        LOGGER.info("Has quorum: {}", quorumState.hasQuorum());
        
        // Check individual replica health
        for (int i = 0; i < 3; i++) {
            LOGGER.info("Replica {} healthy: {}", i, quorumState.isReplicaHealthy(i));
        }
    }

    private static void forceUploadExample(S3QuorumStorage quorumStorage) 
            throws ExecutionException, InterruptedException {
        LOGGER.info("=== Force Upload Example ===");

        long streamId = 1L;
        
        // Force upload to all replicas (waits for majority to complete)
        CompletableFuture<Void> uploadFuture = quorumStorage.forceUpload(streamId);
        
        // Wait for completion
        uploadFuture.get(30, TimeUnit.SECONDS);
        
        LOGGER.info("Force upload completed successfully");
    }
} 