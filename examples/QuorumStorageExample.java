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

package examples;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.quorum.S3QuorumStorage;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.quorum.factory.S3QuorumStorageFactory;
import com.automq.stream.s3.quorum.state.QuorumState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import io.netty.buffer.Unpooled;

/**
 * Example demonstrating how to use S3QuorumStorage for high-availability data storage
 */
public class QuorumStorageExample {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuorumStorageExample.class);

    public static void main(String[] args) {
        try {
            // Create and configure the quorum storage
            S3QuorumStorage quorumStorage = createQuorumStorage();
            
            // Start the quorum storage
            quorumStorage.startup();
            LOGGER.info("Quorum storage started successfully");

            // Example 1: Write data with quorum guarantee
            writeDataExample(quorumStorage);

            // Example 2: Read data with fault tolerance
            readDataExample(quorumStorage);

            // Example 3: Monitor quorum state
            monitorQuorumState(quorumStorage);

            // Example 4: Force upload data
            forceUploadExample(quorumStorage);

            // Shutdown the quorum storage
            quorumStorage.shutdown();
            LOGGER.info("Quorum storage shutdown completed");

        } catch (Exception e) {
            LOGGER.error("Error in quorum storage example", e);
        }
    }

    /**
     * Create S3QuorumStorage with 3 replicas across different regions
     */
    private static S3QuorumStorage createQuorumStorage() {
        // Create base configuration
        Config baseConfig = new Config();
        baseConfig.nodeId(1);
        baseConfig.walCacheSize(200 * 1024 * 1024); // 200MB
        baseConfig.walUploadThreshold(100 * 1024 * 1024); // 100MB
        baseConfig.walUploadIntervalMs(1000);
        baseConfig.streamSplitSize(16 * 1024 * 1024); // 16MB
        baseConfig.objectBlockSize(1024 * 1024); // 1MB
        baseConfig.objectPartSize(16 * 1024 * 1024); // 16MB
        baseConfig.blockCacheSize(100 * 1024 * 1024); // 100MB
        baseConfig.networkBaselineBandwidth(100 * 1024 * 1024); // 100MB/s

        // Create quorum storage using factory
        // Note: In a real implementation, you would need to provide actual WAL, StreamManager, etc.
        // For this example, we'll use null values which will be handled by the factory
        return S3QuorumStorageFactory.createQuorumStorage(
            baseConfig, null, null, null, null);
    }

    /**
     * Example: Write data with quorum guarantee
     */
    private static void writeDataExample(S3QuorumStorage quorumStorage) {
        LOGGER.info("=== Write Data Example ===");

        try {
            // Create test data
            String message = "Hello, Quorum Storage!";
            ByteBuffer data = ByteBuffer.wrap(message.getBytes());
            
            StreamRecordBatch recordBatch = new StreamRecordBatch(
                1L, // streamId
                1L, // epoch
                0L, // baseOffset
                1,  // count
                Unpooled.wrappedBuffer(data)
            );

            AppendContext context = AppendContext.DEFAULT;

            // Write data with quorum guarantee
            CompletableFuture<Void> writeFuture = quorumStorage.append(context, recordBatch);
            
            // Wait for write to complete
            writeFuture.get(30, TimeUnit.SECONDS);
            
            LOGGER.info("Data written successfully with quorum guarantee");

        } catch (Exception e) {
            LOGGER.error("Failed to write data", e);
        }
    }

    /**
     * Example: Read data with fault tolerance
     */
    private static void readDataExample(S3QuorumStorage quorumStorage) {
        LOGGER.info("=== Read Data Example ===");

        try {
            FetchContext context = FetchContext.DEFAULT;
            long streamId = 1L;
            long startOffset = 0L;
            long endOffset = 100L;
            int maxBytes = 1024;

            // Read data with fault tolerance
            CompletableFuture<ReadDataBlock> readFuture = quorumStorage.read(
                context, streamId, startOffset, endOffset, maxBytes);

            // Wait for read to complete
            ReadDataBlock dataBlock = readFuture.get(10, TimeUnit.SECONDS);
            
            LOGGER.info("Data read successfully: {}", dataBlock);

        } catch (Exception e) {
            LOGGER.error("Failed to read data", e);
        }
    }

    /**
     * Example: Monitor quorum state
     */
    private static void monitorQuorumState(S3QuorumStorage quorumStorage) {
        LOGGER.info("=== Monitor Quorum State ===");

        QuorumState quorumState = quorumStorage.getQuorumState();

        // Check quorum health
        int healthyCount = quorumState.getHealthyReplicaCount();
        boolean hasQuorum = quorumState.hasQuorum();
        
        LOGGER.info("Healthy replicas: {}", healthyCount);
        LOGGER.info("Has quorum: {}", hasQuorum);

        // Get healthy and unhealthy replica indices
        int[] healthyIndices = quorumState.getHealthyReplicaIndices();
        int[] unhealthyIndices = quorumState.getUnhealthyReplicaIndices();

        LOGGER.info("Healthy replica indices: {}", java.util.Arrays.toString(healthyIndices));
        LOGGER.info("Unhealthy replica indices: {}", java.util.Arrays.toString(unhealthyIndices));

        // Check individual replica health
        for (int i = 0; i < 3; i++) {
            boolean isHealthy = quorumState.isReplicaHealthy(i);
            long failureCount = quorumState.getReplicaFailureCount(i);
            long lastSuccessTime = quorumState.getReplicaLastSuccessTime(i);
            long lastFailureTime = quorumState.getReplicaLastFailureTime(i);

            LOGGER.info("Replica {}: healthy={}, failures={}, lastSuccess={}, lastFailure={}", 
                       i, isHealthy, failureCount, lastSuccessTime, lastFailureTime);
        }
    }

    /**
     * Example: Force upload data to all replicas
     */
    private static void forceUploadExample(S3QuorumStorage quorumStorage) {
        LOGGER.info("=== Force Upload Example ===");

        try {
            long streamId = 1L;

            // Force upload data to all replicas
            CompletableFuture<Void> uploadFuture = quorumStorage.forceUpload(streamId);
            
            // Wait for upload to complete
            uploadFuture.get(60, TimeUnit.SECONDS);
            
            LOGGER.info("Force upload completed successfully");

        } catch (Exception e) {
            LOGGER.error("Failed to force upload", e);
        }
    }

    /**
     * Example: Custom quorum configuration
     */
    private static S3QuorumStorage createCustomQuorumStorage() {
        // Create replica configurations
        ReplicaConfig primaryConfig = ReplicaConfig.builder()
            .replicaId(0)
            .region("us-east-1")
            .bucket("my-primary-bucket")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .role(ReplicaConfig.ReplicaRole.PRIMARY)
            .priority(100)
            .build();

        ReplicaConfig secondary1Config = ReplicaConfig.builder()
            .replicaId(1)
            .region("us-west-2")
            .bucket("my-secondary1-bucket")
            .endpoint("https://s3.us-west-2.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(50)
            .build();

        ReplicaConfig secondary2Config = ReplicaConfig.builder()
            .replicaId(2)
            .region("eu-west-1")
            .bucket("my-secondary2-bucket")
            .endpoint("https://s3.eu-west-1.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(25)
            .build();

        // Create quorum configuration
        QuorumConfig quorumConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)  // Majority (2 out of 3)
            .readQuorumSize(1)   // Any replica
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(java.util.Arrays.asList(primaryConfig, secondary1Config, secondary2Config))
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();

        // Note: In a real implementation, you would need to create actual Storage instances
        // For this example, we'll return null to indicate this is just a configuration example
        LOGGER.info("Custom quorum configuration created");
        return null;
    }
} 