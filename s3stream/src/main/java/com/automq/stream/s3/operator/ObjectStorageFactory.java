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

package com.automq.stream.s3.operator;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.network.NetworkBandwidthLimiter;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ObjectStorageFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageFactory.class);
    public static final String PROTOCOL_ROOT = "root";
    public static final String EXTENSION_TYPE_KEY = "type";
    public static final String EXTENSION_TYPE_MAIN = "main";
    public static final String EXTENSION_TYPE_BACKGROUND = "background";
    private static volatile ObjectStorageFactory instance;
    private final Map<String /* protocol */, Function<Builder, ObjectStorage>> protocolHandlers = new HashMap<>();

    static {
        ObjectStorageFactory.instance().registerProtocolHandler(PROTOCOL_ROOT, builder -> {
            if (builder.quorumEnabled() && builder.buckets() != null && builder.buckets().size() > 1) {
                // CRITICAL FIX: ALWAYS enforce optimized 2-replica strategy for global QuorumObjectStorage
                int optimizedWriteQuorum = Math.min(2, builder.buckets().size());
                int optimizedReadQuorum = 1;

                LOGGER.info("🎯 Root protocol handler - ENFORCING optimized 2-replica strategy:");
                LOGGER.info("  Original builder writeQuorum: {}, enforcing: {}", builder.writeQuorumSize(), optimizedWriteQuorum);
                LOGGER.info("  Original builder readQuorum: {}, enforcing: {}", builder.readQuorumSize(), optimizedReadQuorum);

                // FORCE the optimized values regardless of what builder had
                builder.writeQuorumSize(optimizedWriteQuorum);
                builder.readQuorumSize(optimizedReadQuorum);

                QuorumObjectStorage quorumStorage = new QuorumObjectStorage(builder);
                return quorumStorage;
            } else if (builder.buckets() != null && !builder.buckets().isEmpty()) {
                // Non-quorum case with multiple buckets - use first bucket's protocol
                BucketURI firstBucket = builder.buckets().get(0);
                return ObjectStorageFactory.instance().builder(firstBucket)
                    .tagging(builder.tagging())
                    .inboundLimiter(builder.inboundLimiter())
                    .outboundLimiter(builder.outboundLimiter())
                    .readWriteIsolate(builder.readWriteIsolate())
                    .checkS3ApiModel(builder.checkS3ApiModel())
                    .threadPrefix(builder.threadPrefix())
                    .extension(EXTENSION_TYPE_KEY, builder.extension(EXTENSION_TYPE_KEY))
                    .build();
            }
            throw new UnsupportedOperationException("Root protocol handler requires bucket configuration");
        });

        ObjectStorageFactory.instance()
            .registerProtocolHandler("s3", builder -> {
                // CRITICAL FIX: Prevent nested QuorumObjectStorage when creating replicas
                // If this is a replica creation (has buckets), don't return global QuorumObjectStorage
                if (builder.buckets() != null && builder.buckets().size() > 1) {
                    LOGGER.info("🔄 S3 protocol handler - creating replica for bucket {}, NOT returning global QuorumObjectStorage",
                        builder.bucket != null ? builder.bucket.bucketId() : "null");
                    // This is a replica creation, return AwsObjectStorage for the specific bucket
                    return AwsObjectStorage.builder()
                        .bucket(builder.bucket)
                        .tagging(builder.tagging)
                        .inboundLimiter(builder.inboundLimiter)
                        .outboundLimiter(builder.outboundLimiter)
                        .readWriteIsolate(builder.readWriteIsolate)
                        .checkS3ApiModel(builder.checkS3ApiModel)
                        .threadPrefix(builder.threadPrefix)
                        .build();
                }
                // Create new AwsObjectStorage for single bucket
                LOGGER.info("🔄 Creating new AwsObjectStorage for S3 bucket: {}",
                    builder.bucket != null ? builder.bucket.bucketId() : "null");
                return AwsObjectStorage.builder()
                    .bucket(builder.bucket)
                    .tagging(builder.tagging)
                    .inboundLimiter(builder.inboundLimiter)
                    .outboundLimiter(builder.outboundLimiter)
                    .readWriteIsolate(builder.readWriteIsolate)
                    .checkS3ApiModel(builder.checkS3ApiModel)
                    .threadPrefix(builder.threadPrefix)
                    .build();
            })
            .registerProtocolHandler("mem", builder -> new MemoryObjectStorage(builder.bucket.bucketId()))
            .registerProtocolHandler("file", builder ->
                LocalFileObjectStorage.builder()
                    .bucket(builder.bucket)
                    .build());
    }

    private ObjectStorageFactory() {
    }

    public ObjectStorageFactory registerProtocolHandler(String protocol,
                                                        Function<Builder, ObjectStorage> handler) {
        protocolHandlers.put(protocol, handler);
        return this;
    }

    public Builder builder(BucketURI bucket) {
        return new Builder().bucket(bucket);
    }

    public Builder builder() {
        return new Builder();
    }

    public static ObjectStorageFactory instance() {
        if (instance == null) {
            synchronized (ObjectStorageFactory.class) {
                if (instance == null) {
                    instance = new ObjectStorageFactory();
                }
            }
        }
        return instance;
    }

    /**
     * Create ObjectStorage with specified extension type
     */
    public static ObjectStorage createObjectStorage(Config config, String extensionType) {
        LOGGER.info("🔍 ObjectStorageFactory.createObjectStorage() called with extensionType: {}", extensionType);
        LOGGER.info("🔍 Config object class: {}", config != null ? config.getClass().getName() : "null");

        // Check if S3 Quorum is enabled
        boolean quorumEnabled = config.quorumEnabled();
        ;
        int quorumSize = 3;
        int writeQuorumSize = 2;
        int readQuorumSize = 1;
        if (quorumEnabled) {
            quorumSize = config.quorumSize();
            writeQuorumSize = config.writeQuorumSize();
            readQuorumSize = config.readQuorumSize();
        }
        List<BucketURI> dataBuckets = config.dataBuckets();
        // If we have multiple buckets and quorum is enabled, use quorum storage
        if (dataBuckets.size() > 1 && quorumEnabled) {
            // OPTIMIZED: Default to 2-replica write strategy for efficiency
            int optimizedWriteQuorum = Math.min(2, dataBuckets.size());
            int optimizedReadQuorum = 1; // Always read from 1 replica for performance

            LOGGER.info("🌟 ObjectStorageFactory.createObjectStorage() creating OPTIMIZED QuorumObjectStorage:");
            LOGGER.info("  buckets={}, optimizedWriteQuorum={}, optimizedReadQuorum={}",
                dataBuckets.size(), optimizedWriteQuorum, optimizedReadQuorum);
            LOGGER.info("  Config writeQuorumSize was: {}", writeQuorumSize);

            QuorumObjectStorage quorumStorage = (QuorumObjectStorage) instance().builder()
                .buckets(dataBuckets)
                .quorumEnabled(true)
                .quorumSize(dataBuckets.size())
                .writeQuorumSize(optimizedWriteQuorum)
                .readQuorumSize(optimizedReadQuorum)
                .extension(EXTENSION_TYPE_KEY, extensionType)
                .build();
            return quorumStorage;
        }

        return instance().builder()
            .bucket(config.dataBuckets().get(0))
            .quorumEnabled(false)  // Explicitly disable quorum for single bucket
            .quorumSize(1)         // Single replica
            .writeQuorumSize(1)    // Single replica write
            .readQuorumSize(1)     // Single replica read
            .extension(EXTENSION_TYPE_KEY, extensionType)
            .build();
    }

    /**
     * Create ObjectStorage with custom parameters
     */
    public static ObjectStorage createObjectStorage(Config config, String endpoint,
                                                    String accessKey, String secretKey, String region) {
        // Create a bucket URI for the given region
        String bucketUriStr = String.format("0@s3://%s/automq-bucket?endpoint=%s&region=%s",
            region, endpoint, region);
        BucketURI bucket = BucketURI.parse(bucketUriStr);

        return instance().builder()
            .bucket(bucket)
            .extension("accessKey", accessKey)
            .extension("secretKey", secretKey)
            .build();
    }

    public class Builder {
        private final AtomicLong defaultThreadPrefixCounter = new AtomicLong();
        private BucketURI bucket;
        private List<BucketURI> buckets;
        private Map<String, String> tagging;
        private NetworkBandwidthLimiter inboundLimiter = NetworkBandwidthLimiter.NOOP;
        private NetworkBandwidthLimiter outboundLimiter = NetworkBandwidthLimiter.NOOP;
        private boolean readWriteIsolate;
        private boolean checkS3ApiModel = false;
        private String threadPrefix = "";
        private final Map<String, Object> extensions = new HashMap<>();
        private boolean quorumEnabled = false;
        private int quorumSize = 3;
        private int writeQuorumSize = 2;
        private int readQuorumSize = 1;

        Builder bucket(BucketURI bucketURI) {
            this.bucket = bucketURI;
            return this;
        }

        public BucketURI bucket() {
            return bucket;
        }

        public Builder buckets(List<BucketURI> buckets) {
            this.buckets = buckets;
            if (bucket == null && buckets.size() == 1) {
                bucket = buckets.get(0);
            }
            return this;
        }

        public List<BucketURI> buckets() {
            return buckets;
        }

        public Builder tagging(Map<String, String> tagging) {
            this.tagging = tagging;
            return this;
        }

        public Map<String, String> tagging() {
            return tagging;
        }

        public Builder inboundLimiter(NetworkBandwidthLimiter inboundLimiter) {
            this.inboundLimiter = inboundLimiter;
            return this;
        }

        public NetworkBandwidthLimiter inboundLimiter() {
            return inboundLimiter;
        }

        public Builder outboundLimiter(NetworkBandwidthLimiter outboundLimiter) {
            this.outboundLimiter = outboundLimiter;
            return this;
        }

        public NetworkBandwidthLimiter outboundLimiter() {
            return outboundLimiter;
        }

        public Builder readWriteIsolate(boolean readWriteIsolate) {
            this.readWriteIsolate = readWriteIsolate;
            return this;
        }

        public boolean readWriteIsolate() {
            return readWriteIsolate;
        }

        public Builder checkS3ApiModel(boolean checkS3ApiModel) {
            this.checkS3ApiModel = checkS3ApiModel;
            return this;
        }

        public boolean checkS3ApiModel() {
            return checkS3ApiModel;
        }

        public Builder threadPrefix(String prefix) {
            if (prefix == null) {
                return this;
            }
            this.threadPrefix = prefix;
            return this;
        }

        public String threadPrefix() {
            return threadPrefix;
        }

        public Builder extension(String key, Object value) {
            this.extensions.put(key, value);
            return this;
        }

        @SuppressWarnings("unchecked")
        public <T> T extension(String key) {
            return (T) this.extensions.get(key);
        }

        public Map<String, Object> extensions() {
            return extensions;
        }

        public Builder quorumEnabled(boolean enabled) {
            this.quorumEnabled = enabled;
            return this;
        }

        public boolean quorumEnabled() {
            return quorumEnabled;
        }

        public Builder quorumSize(int size) {
            this.quorumSize = size;
            return this;
        }

        public int quorumSize() {
            return quorumSize;
        }

        public Builder writeQuorumSize(int size) {
            this.writeQuorumSize = size;
            return this;
        }

        public int writeQuorumSize() {
            return writeQuorumSize;
        }

        public Builder readQuorumSize(int size) {
            this.readQuorumSize = size;
            return this;
        }

        public int readQuorumSize() {
            return readQuorumSize;
        }

        public ObjectStorage build() {
            if (StringUtils.isEmpty(this.threadPrefix)) {
                this.threadPrefix = Long.toString(defaultThreadPrefixCounter.getAndIncrement());
            }

            // Ensure bucket is not null
            if (bucket == null && (buckets == null || buckets.isEmpty())) {
                throw new IllegalStateException("Bucket configuration is required but not provided. " +
                    "Please ensure proper bucket configuration is set in the system properties or configuration files.");
            }

            // If bucket is null but buckets is not empty, use the first bucket
            if (bucket == null && buckets != null && !buckets.isEmpty()) {
                bucket = buckets.get(0);
            }

            ObjectStorage objectStorage;

            // Create QuorumObjectStorage if quorum is enabled and multiple buckets are configured
            if (quorumEnabled && buckets != null && buckets.size() > 1) {
                // OPTIMIZED: Use 2-replica write strategy with backup failover
                if (quorumSize < buckets.size()) {
                    quorumSize = buckets.size();
                }
                if (writeQuorumSize < 1) {
                    // Default to 2-replica write for efficiency, with 3rd as backup
                    writeQuorumSize = Math.min(2, buckets.size());
                }
                if (readQuorumSize < 1) {
                    // Always read from 1 replica for performance
                    readQuorumSize = 1;
                }

                LOGGER.info("🔧 Building QuorumObjectStorage with optimized 2+1 strategy: " +
                        "totalBuckets={}, writeQuorum={}, readQuorum={}",
                    buckets.size(), writeQuorumSize, readQuorumSize);

                QuorumObjectStorage quorumStorage = new QuorumObjectStorage(this);
                objectStorage = quorumStorage;
            } else {
                // Single bucket mode - use protocol-specific handler
                String protocol = bucket.protocol();
                if (!protocolHandlers.containsKey(protocol)) {
                    throw new UnsupportedOperationException("No protocol handler registered for protocol: " + protocol + ". Available protocols: " + protocolHandlers.keySet());
                }
                objectStorage = protocolHandlers.get(protocol).apply(this);
            }
            return objectStorage;
        }
    }
}
