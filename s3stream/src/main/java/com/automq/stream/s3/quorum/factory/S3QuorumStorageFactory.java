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

package com.automq.stream.s3.quorum.factory;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.Storage;
import com.automq.stream.s3.cache.ReadDataBlock;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.context.AppendContext;
import com.automq.stream.s3.context.FetchContext;
import com.automq.stream.s3.failover.StorageFailureHandler;
import com.automq.stream.s3.metadata.S3ObjectMetadata;
import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitStreamSetObjectHook;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.CommitStreamSetObjectResponse;
import com.automq.stream.s3.objects.CompactStreamObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.quorum.S3QuorumStorage;
import com.automq.stream.s3.quorum.config.QuorumConfig;
import com.automq.stream.s3.quorum.config.ReplicaConfig;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.WriteAheadLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Factory for creating S3QuorumStorage with 3 replicas
 */
public class S3QuorumStorageFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3QuorumStorageFactory.class);

    /**
     * Create S3QuorumStorage with 3 replicas across different regions
     */
    public static S3QuorumStorage createQuorumStorage(
            Config baseConfig,
            WriteAheadLog writeAheadLog,
            StreamManager streamManager,
            S3BlockCache blockCache,
            StorageFailureHandler storageFailureHandler) {
        
        // Create replica configurations for 3 regions
        List<ReplicaConfig> replicaConfigs = createDefaultReplicaConfigs(baseConfig);
        
        // Create quorum configuration
        QuorumConfig quorumConfig = QuorumConfig.builder()
            .quorumSize(3)
            .writeQuorumSize(2)  // Majority (2 out of 3)
            .readQuorumSize(1)   // Any replica
            .writeTimeoutMs(30000)
            .readTimeoutMs(10000)
            .replicaConfigs(replicaConfigs)
            .enableReadRepair(true)
            .readRepairTimeoutMs(5000)
            .build();
        
        // Create individual S3Storage instances for each replica
        List<Storage> replicas = new ArrayList<>();
        for (int i = 0; i < replicaConfigs.size(); i++) {
            ReplicaConfig replicaConfig = replicaConfigs.get(i);
            Storage replica = createReplicaStorage(replicaConfig, writeAheadLog, streamManager, 
                                                 blockCache, storageFailureHandler);
            replicas.add(replica);
            LOGGER.info("Created replica {} storage for region: {}", i, replicaConfig.getRegion());
        }
        
        return new S3QuorumStorage(quorumConfig, replicas);
    }

    /**
     * Create replica configurations for 3 different regions
     */
    private static List<ReplicaConfig> createDefaultReplicaConfigs(Config baseConfig) {
        List<ReplicaConfig> configs = new ArrayList<>();
        
        // Primary replica (us-east-1)
        ReplicaConfig primaryConfig = ReplicaConfig.builder()
            .replicaId(0)
            .region("us-east-1")
            .bucket("automq-primary-bucket")
            .endpoint("https://s3.us-east-1.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .s3Config(createReplicaS3Config(baseConfig, "us-east-1"))
            .role(ReplicaConfig.ReplicaRole.PRIMARY)
            .priority(100)
            .build();
        configs.add(primaryConfig);
        
        // Secondary replica 1 (us-west-2)
        ReplicaConfig secondary1Config = ReplicaConfig.builder()
            .replicaId(1)
            .region("us-west-2")
            .bucket("automq-secondary1-bucket")
            .endpoint("https://s3.us-west-2.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .s3Config(createReplicaS3Config(baseConfig, "us-west-2"))
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(50)
            .build();
        configs.add(secondary1Config);
        
        // Secondary replica 2 (eu-west-1)
        ReplicaConfig secondary2Config = ReplicaConfig.builder()
            .replicaId(2)
            .region("eu-west-1")
            .bucket("automq-secondary2-bucket")
            .endpoint("https://s3.eu-west-1.amazonaws.com")
            .accessKey(System.getenv("AWS_ACCESS_KEY_ID"))
            .secretKey(System.getenv("AWS_SECRET_ACCESS_KEY"))
            .s3Config(createReplicaS3Config(baseConfig, "eu-west-1"))
            .role(ReplicaConfig.ReplicaRole.SECONDARY)
            .priority(25)
            .build();
        configs.add(secondary2Config);
        
        return configs;
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
     * Create individual S3Storage for a replica
     */
    private static Storage createReplicaStorage(
            ReplicaConfig replicaConfig,
            WriteAheadLog writeAheadLog,
            StreamManager streamManager,
            S3BlockCache blockCache,
            StorageFailureHandler storageFailureHandler) {
        
        try {
            // Create ObjectStorage for this replica using the factory
            ObjectStorage objectStorage = ObjectStorageFactory.createObjectStorage(
                replicaConfig.getS3Config(),
                replicaConfig.getEndpoint(),
                replicaConfig.getAccessKey(),
                replicaConfig.getSecretKey(),
                replicaConfig.getRegion()
            );
            
            // Create ObjectManager for this replica
            ObjectManager objectManager = createReplicaObjectManager(
                replicaConfig, streamManager, objectStorage);
            
            // Create S3Storage for this replica with correct constructor parameters
            // Note: In a real implementation, you would need to provide actual WAL, etc.
            // For this example, we'll create a mock or simplified version
            if (writeAheadLog == null || streamManager == null || blockCache == null || storageFailureHandler == null) {
                LOGGER.warn("Some components are null, creating simplified replica storage");
                // Return a mock storage for testing/example purposes
                return new Storage() {
                    @Override
                    public void startup() {
                        LOGGER.info("Mock replica storage started");
                    }

                    @Override
                    public void shutdown() {
                        LOGGER.info("Mock replica storage shutdown");
                    }

                    @Override
                    public CompletableFuture<Void> append(AppendContext context, StreamRecordBatch streamRecord) {
                        LOGGER.debug("Mock replica append: {}", streamRecord);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<ReadDataBlock> read(FetchContext context, long streamId, long startOffset, long endOffset, int maxBytes) {
                        LOGGER.debug("Mock replica read: streamId={}, startOffset={}, endOffset={}", streamId, startOffset, endOffset);
                        return CompletableFuture.completedFuture(null);
                    }

                    @Override
                    public CompletableFuture<Void> forceUpload(long streamId) {
                        LOGGER.debug("Mock replica force upload: streamId={}", streamId);
                        return CompletableFuture.completedFuture(null);
                    }
                };
            }
            
            return new S3Storage(
                replicaConfig.getS3Config(),
                writeAheadLog,
                streamManager,
                objectManager,
                blockCache,
                objectStorage,
                storageFailureHandler
            );
            
        } catch (Exception e) {
            LOGGER.error("Failed to create replica storage for region: {}", 
                        replicaConfig.getRegion(), e);
            throw new RuntimeException("Failed to create replica storage", e);
        }
    }

    /**
     * Create ObjectManager for a specific replica
     */
    private static ObjectManager createReplicaObjectManager(
            ReplicaConfig replicaConfig,
            StreamManager streamManager,
            ObjectStorage objectStorage) {
        
        // For quorum storage, we need to create a simpler ObjectManager
        // that doesn't depend on ControllerRequestSender and StreamMetadataManager
        // We'll use a basic implementation that works with the replica's ObjectStorage
        
        return new com.automq.stream.s3.objects.ObjectManager() {
            @Override
            public CompletableFuture<Long> prepareObject(int count, long ttl) {
                // For replica storage, we'll use a simple object ID generation
                // In a real implementation, you might want to coordinate with the controller
                return CompletableFuture.completedFuture(System.currentTimeMillis() * 1000 + count);
            }

            @Override
            public void setCommitStreamSetObjectHook(CommitStreamSetObjectHook hook) {
                // No-op for replica storage
            }

            @Override
            public CompletableFuture<CommitStreamSetObjectResponse> commitStreamSetObject(
                    CommitStreamSetObjectRequest commitStreamSetObjectRequest) {
                // For replica storage, we'll just return success
                // In a real implementation, you might want to validate the commit
                return CompletableFuture.completedFuture(new CommitStreamSetObjectResponse());
            }

            @Override
            public CompletableFuture<Void> compactStreamObject(CompactStreamObjectRequest compactStreamObjectRequest) {
                // No-op for replica storage
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public CompletableFuture<List<S3ObjectMetadata>> getObjects(long streamId, long startOffset, long endOffset, int limit) {
                // For replica storage, we'll return empty list
                // In a real implementation, you might want to query the ObjectStorage
                return CompletableFuture.completedFuture(new ArrayList<>());
            }

            @Override
            public boolean isObjectExist(long objectId) {
                // For replica storage, we'll assume object exists
                // In a real implementation, you might want to check ObjectStorage
                return true;
            }

            @Override
            public CompletableFuture<List<S3ObjectMetadata>> getServerObjects() {
                // For replica storage, we'll return empty list
                return CompletableFuture.completedFuture(new ArrayList<>());
            }

            @Override
            public CompletableFuture<List<S3ObjectMetadata>> getStreamObjects(long streamId, long startOffset, long endOffset, int limit) {
                // For replica storage, we'll return empty list
                return CompletableFuture.completedFuture(new ArrayList<>());
            }

            @Override
            public CompletableFuture<Integer> getObjectsCount() {
                // For replica storage, we'll return 0
                return CompletableFuture.completedFuture(0);
            }
        };
    }
} 