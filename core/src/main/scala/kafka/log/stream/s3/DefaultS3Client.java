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

package kafka.log.stream.s3;

import com.automq.stream.s3.operator.BucketURI;
import kafka.autobalancer.metricsreporter.metric.Derivator;
import kafka.log.stream.s3.metadata.StreamMetadataManager;
import kafka.log.stream.s3.network.ControllerRequestSender;
import kafka.log.stream.s3.node.NodeManager;
import kafka.log.stream.s3.node.NodeManagerStub;
import kafka.log.stream.s3.node.NoopNodeManager;
import kafka.log.stream.s3.objects.ControllerObjectManager;
import kafka.log.stream.s3.streams.ControllerStreamManager;
import kafka.log.stream.s3.wal.DefaultWalFactory;
import kafka.server.BrokerServer;

import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.common.automq.AutoMQVersion;

import com.automq.stream.api.Client;
import com.automq.stream.api.KVClient;
import com.automq.stream.api.StreamClient;
import com.automq.stream.s3.Config;
import com.automq.stream.s3.S3Storage;
import com.automq.stream.s3.Storage;
import com.automq.stream.s3.S3StreamClient;
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.cache.blockcache.DefaultObjectReaderFactory;
import com.automq.stream.s3.cache.blockcache.ObjectReaderFactory;
import com.automq.stream.s3.cache.blockcache.StreamReaders;
import com.automq.stream.s3.compact.CompactionManager;
import com.automq.stream.s3.failover.Failover;
import com.automq.stream.s3.failover.FailoverFactory;
import com.automq.stream.s3.failover.FailoverRequest;
import com.automq.stream.s3.failover.FailoverResponse;
import com.automq.stream.s3.failover.ForceCloseStorageFailureHandler;
import com.automq.stream.s3.failover.HaltStorageFailureHandler;
import com.automq.stream.s3.failover.StorageFailureHandlerChain;
import com.automq.stream.s3.index.LocalStreamRangeIndexCache;
import com.automq.stream.s3.network.AsyncNetworkBandwidthLimiter;
import com.automq.stream.s3.network.GlobalNetworkBandwidthLimiters;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.WalFactory;
import com.automq.stream.s3.wal.WriteAheadLog;
import com.automq.stream.utils.IdURI;
import com.automq.stream.s3.quorum.factory.S3QuorumStorageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_BACKGROUND;
import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_MAIN;

public class DefaultS3Client implements Client {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultS3Client.class);
    protected final Config config;
    protected final Derivator networkInboundRate = new Derivator();
    protected final Derivator networkOutboundRate = new Derivator();
    private StreamMetadataManager metadataManager;

    protected ControllerRequestSender requestSender;

    protected ObjectStorage mainObjectStorage;
    protected ObjectStorage backgroundObjectStorage;

    protected WriteAheadLog writeAheadLog;
    protected StorageFailureHandlerChain storageFailureHandlerChain;
    protected Storage storage;
    protected boolean enableQuorumStorage = false;

    protected ObjectReaderFactory objectReaderFactory;
    protected S3BlockCache blockCache;
    protected ObjectManager objectManager;
    protected StreamManager streamManager;
    protected NodeManager nodeManager;
    protected CompactionManager compactionManager;
    protected S3StreamClient streamClient;
    protected KVClient kvClient;
    protected Failover failover;
    protected NetworkBandwidthLimiter networkInboundLimiter;
    protected NetworkBandwidthLimiter networkOutboundLimiter;
    protected BrokerServer brokerServer;
    protected LocalStreamRangeIndexCache localIndexCache;

    public DefaultS3Client(BrokerServer brokerServer, Config config) {
        this.brokerServer = brokerServer;
        this.config = config;
    }

    @Override
    public void start() {
        LOGGER.info("Starting DefaultS3Client with quorum storage: {}", enableQuorumStorage);

        // Initialize network bandwidth limiters
        long refillToken = (long) (config.networkBaselineBandwidth() * ((double) config.refillPeriodMs() / 1000));
        if (refillToken <= 0) {
            throw new IllegalArgumentException(String.format("refillToken must be greater than 0, bandwidth: %d, refill period: %dms",
                config.networkBaselineBandwidth(), config.refillPeriodMs()));
        }

        GlobalNetworkBandwidthLimiters.instance().setup(AsyncNetworkBandwidthLimiter.Type.INBOUND,
            refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth());
        networkInboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.INBOUND);

        GlobalNetworkBandwidthLimiters.instance().setup(AsyncNetworkBandwidthLimiter.Type.OUTBOUND,
            refillToken, config.refillPeriodMs(), config.networkBaselineBandwidth() * 5);
        networkOutboundLimiter = GlobalNetworkBandwidthLimiters.instance().get(AsyncNetworkBandwidthLimiter.Type.OUTBOUND);

        // Initialize object storage
        mainObjectStorage = newMainObjectStorage();
        backgroundObjectStorage = newBackgroundObjectStorage();

        // Initialize WAL
        writeAheadLog = buildWAL();

        // Initialize object reader factory (needs object storage)
        objectReaderFactory = new DefaultObjectReaderFactory(mainObjectStorage);

        // Initialize local index cache
        localIndexCache = LocalStreamRangeIndexCache.create();

        // Initialize request sender (needed for stream manager and object manager)
        requestSender = new ControllerRequestSender(brokerServer, new ControllerRequestSender.RetryPolicyContext(
            config.controllerRequestRetryMaxCount(), config.controllerRequestRetryBaseDelayMs()));

        // Initialize metadata manager (needed for stream manager and object manager)
        metadataManager = new StreamMetadataManager(brokerServer, config.nodeId(), objectReaderFactory, localIndexCache);

        // Initialize stream manager
        streamManager = newStreamManager(config.nodeId(), config.nodeEpoch(), false);

        // Initialize object manager
        objectManager = newObjectManager(config.nodeId(), config.nodeEpoch(), false);

        // Initialize block cache (StreamReaders implementation)
        blockCache = new StreamReaders(config.blockCacheSize(), objectManager, mainObjectStorage, objectReaderFactory);

        // Initialize compaction manager
        compactionManager = new CompactionManager(config, objectManager, streamManager, mainObjectStorage);

        // Initialize stream client first (needed for failure handler)
        streamClient = new S3StreamClient(streamManager, null, objectManager,
                                        backgroundObjectStorage, config, networkInboundLimiter, networkOutboundLimiter);

        // Initialize failure handler (now that we have streamClient)
        storageFailureHandlerChain = new StorageFailureHandlerChain();
        storageFailureHandlerChain.addHandler(new HaltStorageFailureHandler());
        storageFailureHandlerChain.addHandler(new ForceCloseStorageFailureHandler(streamClient));

        // Initialize storage based on configuration
        if (enableQuorumStorage) {
            // Use quorum storage with 3 replicas
            storage = S3QuorumStorageFactory.createQuorumStorage(
                config, writeAheadLog, streamManager, blockCache, storageFailureHandlerChain);
            LOGGER.info("Using S3QuorumStorage with 3 replicas");
        } else {
            // Use single replica storage (original behavior)
            storage = newS3Storage();
            LOGGER.info("Using single replica S3Storage");
        }

        // Update stream client with the storage
        streamClient = new S3StreamClient(streamManager, storage, objectManager,
                                        backgroundObjectStorage, config, networkInboundLimiter, networkOutboundLimiter);

        // Initialize KV client
        kvClient = new ControllerKVClient(requestSender);

        // Initialize failover
        failover = failover();

        // Initialize node manager
        nodeManager = getNodeManager();

        // Start all components
        try {
            LOGGER.info("Starting S3Stream components...");
            writeAheadLog.start();
            storage.startup();
            // Note: streamManager and objectManager don't have startup methods in their interfaces
            compactionManager.start();
            // Note: metadataManager and nodeManager don't have startup methods in their interfaces
            LOGGER.info("S3Stream components started successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to start S3Stream components", e);
            throw new RuntimeException("Failed to start S3Stream components", e);
        }
    }

    @Override
    public void shutdown() {
        LOGGER.info("Shutting down DefaultS3Client");
        try {
            if (storage != null) {
                storage.shutdown();
            }
            if (compactionManager != null) {
                compactionManager.shutdown();
            }
            // Note: streamManager, objectManager, metadataManager, and nodeManager
            // don't have shutdown methods in their interfaces
            LOGGER.info("DefaultS3Client shutdown completed");
        } catch (Exception e) {
            LOGGER.error("Error during shutdown", e);
        }
    }

    @Override
    public StreamClient streamClient() {
        return streamClient;
    }

    @Override
    public KVClient kvClient() {
        return kvClient;
    }

    @Override
    public CompletableFuture<FailoverResponse> failover(FailoverRequest request) {
        return failover.failover(request);
    }

    protected WriteAheadLog buildWAL() {
        // Create WAL factory
        DefaultWalFactory walFactory = new DefaultWalFactory(
            config.nodeId(),
            config.objectTagging() != null ? config.objectTagging() : new HashMap<>(),
            networkInboundLimiter,
            networkOutboundLimiter
        );

        // Parse WAL config URI
        IdURI walUri = IdURI.parse(config.walConfig());
        List< BucketURI> bucketURIList = BucketURI.parseBuckets(config.walConfig());

        // Build options
        WalFactory.BuildOptions buildOptions = WalFactory.BuildOptions.builder()
            .nodeEpoch(config.nodeEpoch())
            .failoverMode(false)
            .build();

        return walFactory.build(bucketURIList, walUri, buildOptions);
    }

    protected ObjectStorage newMainObjectStorage() {
        System.err.println("🔧 DefaultS3Client.newMainObjectStorage() called");
        System.err.println("  Config class: " + config.getClass().getName());
        System.err.println("  Config.quorumEnabled(): " + config.quorumEnabled());
        System.err.println("  Config.writeQuorumSize(): " + config.writeQuorumSize());
        ObjectStorage result = ObjectStorageFactory.createObjectStorage(config, EXTENSION_TYPE_MAIN);
        System.err.println("  Created ObjectStorage class: " + result.getClass().getName());
        return result;
    }

    protected ObjectStorage newBackgroundObjectStorage() {
        return ObjectStorageFactory.createObjectStorage(config, EXTENSION_TYPE_BACKGROUND);
    }

    protected StreamManager newStreamManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerStreamManager(metadataManager, requestSender, nodeId, nodeEpoch,
                                        this::getAutoMQVersion, failoverMode);
    }

    protected ObjectManager newObjectManager(int nodeId, long nodeEpoch, boolean failoverMode) {
        return new ControllerObjectManager(requestSender, metadataManager, nodeId, nodeEpoch,
                                        this::getAutoMQVersion, failoverMode);
    }

    protected S3Storage newS3Storage() {
        return new S3Storage(config, writeAheadLog, streamManager, objectManager, blockCache, mainObjectStorage, storageFailureHandlerChain);
    }

    protected Failover failover() {
        FailoverFactory failoverFactory = new FailoverFactory() {
            @Override
            public StreamManager getStreamManager(int nodeId, long nodeEpoch) {
                return new ControllerStreamManager(metadataManager, requestSender, nodeId, nodeEpoch,
                                                DefaultS3Client.this::getAutoMQVersion, true);
            }

            @Override
            public ObjectManager getObjectManager(int nodeId, long nodeEpoch) {
                return new ControllerObjectManager(requestSender, metadataManager, nodeId, nodeEpoch,
                                                DefaultS3Client.this::getAutoMQVersion, true);
            }

            @Override
            public WriteAheadLog getWal(FailoverRequest request) {
                // Create WAL factory for failover
                DefaultWalFactory failoverWalFactory = new DefaultWalFactory(
                    config.nodeId(),
                    config.objectTagging() != null ? config.objectTagging() : new HashMap<>(),
                    networkInboundLimiter,
                    networkOutboundLimiter
                );

                // Parse WAL path
                IdURI walUri = IdURI.parse(request.getKraftWalConfigs());
                List< BucketURI> bucketURIList = BucketURI.parseBuckets(config.walConfig());
                // Build options for failover
                WalFactory.BuildOptions buildOptions = WalFactory.BuildOptions.builder()
                    .nodeEpoch(config.nodeEpoch())
                    .failoverMode(true)
                    .build();

                return failoverWalFactory.build(bucketURIList, walUri, buildOptions);
            }
        };
        // TODO: Add proper WALRecover implementation
        return new Failover(failoverFactory, null);
    }

    protected AutoMQVersion getAutoMQVersion() {
        // TODO: Fix AutoMQVersion retrieval from metadata
        MetadataImage metadataImage = brokerServer.metadataCache().currentImage();
        // return metadataImage.autoMQVersion(); // Method doesn't exist
        return AutoMQVersion.LATEST; // Default fallback
    }

    private NodeManager getNodeManager() {
        if (config.failoverEnable()) {
            return new NodeManagerStub(requestSender, config.nodeId(), config.nodeEpoch(), new HashMap<>());
        } else {
            return new NoopNodeManager(config.nodeId(), config.nodeEpoch());
        }
    }

    public void setEnableQuorumStorage(boolean enableQuorumStorage) {
        this.enableQuorumStorage = enableQuorumStorage;
    }

    public boolean isQuorumStorageEnabled() {
        return enableQuorumStorage;
    }
}
