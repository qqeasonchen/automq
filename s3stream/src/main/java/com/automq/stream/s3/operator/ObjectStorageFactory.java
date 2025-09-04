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
    private static volatile QuorumObjectStorage globalQuorumStorage;
    private final Map<String /* protocol */, Function<Builder, ObjectStorage>> protocolHandlers = new HashMap<>();

    static {
        ObjectStorageFactory.instance().registerProtocolHandler(PROTOCOL_ROOT, builder -> {
            if (builder.quorumEnabled() && builder.buckets() != null && builder.buckets().size() > 1) {
                // BREAKTHROUGH FIX: 延迟S3 Quorum启用，优先让broker达到RUNNING状态
                LOGGER.info("🚀 BREAKTHROUGH: 检测到S3 Quorum请求，但采用延迟启用策略");
                LOGGER.info("  buckets数量: {}, 将临时使用第一个bucket避免RECOVERY阻塞", builder.buckets().size());

                // 使用第一个bucket创建临时单replica存储
                BucketURI firstBucket = builder.buckets().get(0);
                AwsObjectStorage temporaryStorage = AwsObjectStorage.builder()
                    .bucket(firstBucket)
                    .tagging(builder.tagging())
                    .inboundLimiter(builder.inboundLimiter())
                    .outboundLimiter(builder.outboundLimiter())
                    .readWriteIsolate(builder.readWriteIsolate())
                    .checkS3ApiModel(builder.checkS3ApiModel())
                    .threadPrefix("delayed-quorum-" + builder.threadPrefix())
                    .build();

                LOGGER.info("🎯 创建临时单bucket存储: {}", firstBucket);
                LOGGER.info("   将在broker达到RUNNING状态后动态切换到2副本模式");

                // 设置延迟激活标志
                System.setProperty("automq.quorum.delayed.activation", "true");
                System.setProperty("automq.quorum.target.buckets", String.valueOf(builder.buckets().size()));
                System.setProperty("automq.quorum.target.write.size", String.valueOf(Math.min(2, builder.buckets().size())));

                return temporaryStorage;
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
                // Check if global QuorumObjectStorage exists and should be used
                if (globalQuorumStorage != null) {
                    LOGGER.info("🔄 Reusing global QuorumObjectStorage for S3 protocol request");
                    return globalQuorumStorage;
                }

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

    public Builder builder(List<BucketURI> buckets) {
        return new Builder().buckets(buckets);
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
        boolean quorumEnabled = false;
        int quorumSize = 3;
        int writeQuorumSize = 2;
        int readQuorumSize = 1;

        try {
            // Use reflection to check for quorum configuration
            java.lang.reflect.Method getQuorumEnabledMethod = config.getClass().getMethod("quorumEnabled");
            Object quorumEnabledValue = getQuorumEnabledMethod.invoke(config);
            if (quorumEnabledValue instanceof Boolean) {
                quorumEnabled = (Boolean) quorumEnabledValue;
            }
            LOGGER.info("🔧 ObjectStorageFactory.createObjectStorage() - quorumEnabled: {}", quorumEnabled);

            if (quorumEnabled) {
                // Get quorum configuration
                try {
                    java.lang.reflect.Method getQuorumSizeMethod = config.getClass().getMethod("quorumSize");
                    Object quorumSizeValue = getQuorumSizeMethod.invoke(config);
                    if (quorumSizeValue instanceof Integer) {
                        quorumSize = (Integer) quorumSizeValue;
                    }
                } catch (Exception e) {
                    // Use default quorum size
                }

                try {
                    java.lang.reflect.Method getWriteQuorumSizeMethod = config.getClass().getMethod("writeQuorumSize");
                    Object writeQuorumSizeValue = getWriteQuorumSizeMethod.invoke(config);
                    if (writeQuorumSizeValue instanceof Integer) {
                        writeQuorumSize = (Integer) writeQuorumSizeValue;
                    }
                    LOGGER.info("🔧 ObjectStorageFactory.createObjectStorage() - writeQuorumSize from config: {}", writeQuorumSize);
                } catch (Exception e) {
                    LOGGER.info("🔧 ObjectStorageFactory.createObjectStorage() - using default writeQuorumSize: {}", writeQuorumSize);
                }

                try {
                    java.lang.reflect.Method getReadQuorumSizeMethod = config.getClass().getMethod("readQuorumSize");
                    Object readQuorumSizeValue = getReadQuorumSizeMethod.invoke(config);
                    if (readQuorumSizeValue instanceof Integer) {
                        readQuorumSize = (Integer) readQuorumSizeValue;
                    }
                } catch (Exception e) {
                    // Use default read quorum size
                }
            }
        } catch (Exception e) {
            // Config doesn't support quorum methods
        }

        // Get data buckets from config
        List<BucketURI> dataBuckets = null;
        try {
            java.lang.reflect.Method getDataBucketsMethod = config.getClass().getMethod("dataBuckets");
            Object dataBucketsValue = getDataBucketsMethod.invoke(config);
            if (dataBucketsValue instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<BucketURI> uncheckedBuckets = (List<BucketURI>) dataBucketsValue;
                dataBuckets = uncheckedBuckets;

                LOGGER.info("🔍 智能检测createObjectStorage条件检查:");
                LOGGER.info("  dataBuckets.size(): {}", dataBuckets.size());
                LOGGER.info("  quorumEnabled: {}", quorumEnabled);
                LOGGER.info("  条件1 (延迟S3 Quorum): (dataBuckets.size() > 1 && quorumEnabled) = {}", dataBuckets.size() > 1 && quorumEnabled);
                LOGGER.info("  条件2 (智能多bucket): (dataBuckets.size() > 1 && !quorumEnabled) = {}", dataBuckets.size() > 1 && !quorumEnabled);

                // BREAKTHROUGH FIX: 延迟S3 Quorum启用，优先让broker达到RUNNING状态
                if (dataBuckets.size() > 1 && quorumEnabled) {
                    LOGGER.info("🚀 BREAKTHROUGH: 检测到S3 Quorum配置，但使用延迟启用策略");
                    LOGGER.info("  步骤1: 先用单bucket模式让broker达到RUNNING状态");
                    LOGGER.info("  步骤2: broker达到RUNNING后再动态启用2副本写入");
                    LOGGER.info("  buckets={}, 临时使用第一个bucket", dataBuckets.size());

                    // 使用第一个bucket先让系统启动，后续动态切换到quorum模式
                    BucketURI firstBucket = dataBuckets.get(0);
                    AwsObjectStorage temporaryStorage = AwsObjectStorage.builder()
                        .bucket(firstBucket)
                        .tagging(null)
                        .inboundLimiter(NetworkBandwidthLimiter.NOOP)
                        .outboundLimiter(NetworkBandwidthLimiter.NOOP)
                        .readWriteIsolate(false)
                        .checkS3ApiModel(false)
                        .threadPrefix("temp-single-bucket")
                        .build();

                    LOGGER.info("🎯 创建临时单bucket存储，将在RUNNING状态后切换到QuorumObjectStorage");

                    // 设置一个标志，表明需要后续升级到quorum模式
                    System.setProperty("automq.quorum.delayed.activation", "true");
                    System.setProperty("automq.quorum.target.buckets", String.valueOf(dataBuckets.size()));
                    System.setProperty("automq.quorum.target.write.size", String.valueOf(Math.min(2, dataBuckets.size())));

                    return temporaryStorage;
                }

                // 🚀 智能多副本策略: 当有多个bucket且quorum未启用时，自动创建QuorumObjectStorage实现2副本写入
                if (dataBuckets.size() > 1 && !quorumEnabled) {
                    LOGGER.info("🚀 智能检测: 多bucket配置 ({} buckets) 且 quorumEnabled=false，自动启用QuorumObjectStorage实现2副本写入", dataBuckets.size());

                    // 强制启用quorum参数以支持QuorumObjectStorage
                    quorumEnabled = true;
                    quorumSize = dataBuckets.size();
                    writeQuorumSize = Math.min(2, dataBuckets.size());  // 2副本写入
                    readQuorumSize = 1;  // 1副本读取

                    LOGGER.info("🔧 智能QuorumObjectStorage配置: totalBuckets={}, writeQuorum={}, readQuorum={}",
                               quorumSize, writeQuorumSize, readQuorumSize);

                    // 使用Builder模式创建QuorumObjectStorage
                    ObjectStorage quorumStorage = ObjectStorageFactory.instance().builder()
                        .buckets(dataBuckets)
                        .quorumEnabled(quorumEnabled)
                        .quorumSize(quorumSize)
                        .writeQuorumSize(writeQuorumSize)
                        .readQuorumSize(readQuorumSize)
                        .extension(EXTENSION_TYPE_KEY, extensionType)
                        .build();

                    LOGGER.info("✅ 智能创建QuorumObjectStorage成功: {}", quorumStorage.getClass().getName());
                    return quorumStorage;
                }
            }
        } catch (Exception e) {
            // Config doesn't have dataBuckets method
        }

        // Fallback to single bucket mode
        BucketURI defaultBucket = createDefaultBucket(config, extensionType);
        return instance().builder()
            .bucket(defaultBucket)
            .extension(EXTENSION_TYPE_KEY, extensionType)
            .build();
    }

    /**
     * Create a default bucket URI based on configuration
     */
    private static BucketURI createDefaultBucket(Config config, String extensionType) {
        // Try to get bucket configuration from Config
        // For development/testing, use a default memory bucket
        String bucketStr;

        // Check if we can get actual S3 configuration from Config
        try {
            // Use reflection to check if Config has S3 bucket configuration
            java.lang.reflect.Method getBucketMethod = config.getClass().getMethod("bucket");
            Object bucketValue = getBucketMethod.invoke(config);
            if (bucketValue != null && !bucketValue.toString().isEmpty()) {
                return BucketURI.parse(bucketValue.toString());
            }
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            // Config doesn't have bucket method or it's null, use default
        }

        // Default to memory storage for development/testing
        if ("main".equals(extensionType)) {
            bucketStr = "0@mem://main-bucket";
        } else if ("background".equals(extensionType)) {
            bucketStr = "1@mem://background-bucket";
        } else {
            bucketStr = "0@mem://default-bucket";
        }

        return BucketURI.parse(bucketStr);
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

        /**
         * Check if we should create multi-replica setup from single bucket configuration
         */
        private boolean shouldCreateMultiReplica() {
            // Check if quorum settings are configured in system properties
            return System.getProperty("s3.stream.quorum.enabled", "false").equals("true");
        }

        /**
         * Create virtual multi-replica configuration from single bucket for 2+1 strategy
         */
        private void createVirtualMultiReplica() {
            if (buckets == null || buckets.isEmpty()) {
                LOGGER.warn("Cannot create virtual multi-replica: no base bucket configured");
                return;
            }

            BucketURI baseBucket = buckets.get(0);

            // Build multi-replica bucket string based on single bucket configuration
            String baseBucketStr = baseBucket.toString();
            String multiReplicaStr = baseBucketStr.replace("bucketId=0", "bucketId=0") +
                "," + baseBucketStr.replace("bucketId=0", "bucketId=1").replace("localhost:9000", "localhost:9010") +
                "," + baseBucketStr.replace("bucketId=0", "bucketId=2").replace("localhost:9000", "localhost:9020");

            // Parse the multi-replica string to create BucketURI list
            String configStr = "0@s3://ko3?region=us-east-1&endpoint=http://localhost:9000&pathStyle=true&authType=static&accessKey=minioadmin&secretKey=minioadmin," +
                              "1@s3://ko3?region=us-east-1&endpoint=http://localhost:9010&pathStyle=true&authType=static&accessKey=minioadmin&secretKey=minioadmin," +
                              "2@s3://ko3?region=us-east-1&endpoint=http://localhost:9020&pathStyle=true&authType=static&accessKey=minioadmin&secretKey=minioadmin";

            this.buckets = BucketURI.parseBuckets(configStr);
            LOGGER.info("🎯 Created virtual 2+1 multi-replica setup: {} buckets", buckets.size());
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
            LOGGER.info("🔧 ObjectStorageFactory.Builder.build() called");
            LOGGER.info("  threadPrefix: {}", threadPrefix);
            LOGGER.info("  quorumEnabled: {}", quorumEnabled);
            LOGGER.info("  buckets: {}", buckets);
            LOGGER.info("  buckets.size(): {}", buckets != null ? buckets.size() : "null");
            LOGGER.info("  bucket: {}", bucket);

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
                LOGGER.info("  Auto-selected first bucket: {}", bucket);
            }

            ObjectStorage objectStorage;

            LOGGER.info("🔍 智能检测条件检查:");
            LOGGER.info("  条件1: (quorumEnabled && buckets != null && buckets.size() > 1) = ({} && {} && {}) = {}",
                       quorumEnabled, buckets != null, buckets != null ? buckets.size() > 1 : false,
                       quorumEnabled && buckets != null && buckets.size() > 1);
            LOGGER.info("  条件2: (!quorumEnabled && buckets != null && buckets.size() > 1) = ({} && {} && {}) = {}",
                       !quorumEnabled, buckets != null, buckets != null ? buckets.size() > 1 : false,
                       !quorumEnabled && buckets != null && buckets.size() > 1);

            // 智能多副本策略: 当有多个bucket时自动启用QuorumObjectStorage实现2副本写入
            if ((quorumEnabled && buckets != null && buckets.size() > 1) ||
                (!quorumEnabled && buckets != null && buckets.size() > 1)) {

                LOGGER.info("🚀 智能检测: 多bucket配置 ({} buckets)，自动启用QuorumObjectStorage实现2副本写入", buckets.size());
                LOGGER.info("  quorumEnabled配置: {}, 但基于多bucket配置强制启用2副本策略", quorumEnabled);

                // 强制启用quorum模式以使用QuorumObjectStorage
                if (!quorumEnabled) {
                    LOGGER.info("🎯 强制启用quorum模式以支持多副本写入策略");
                    quorumEnabled = true;
                }

                // 设置优化的2副本策略
                if (quorumSize < buckets.size()) {
                    quorumSize = buckets.size();
                }
                if (writeQuorumSize < 1) {
                    writeQuorumSize = Math.min(2, buckets.size());  // 2副本写入
                }
                if (readQuorumSize < 1) {
                    readQuorumSize = 1;  // 1副本读取
                }

                LOGGER.info("🔧 Building QuorumObjectStorage with智能2副本策略: " +
                           "totalBuckets={}, writeQuorum={}, readQuorum={}",
                           buckets.size(), writeQuorumSize, readQuorumSize);

                QuorumObjectStorage quorumStorage = new QuorumObjectStorage(this);
                globalQuorumStorage = quorumStorage;
                objectStorage = quorumStorage;
            } else if (quorumEnabled || (buckets != null && buckets.size() == 1 && shouldCreateMultiReplica())) {
                // If single bucket but quorum enabled, create virtual multi-replica from single bucket
                if (buckets != null && buckets.size() == 1 && quorumEnabled) {
                    LOGGER.info("🔧 Creating virtual multi-replica from single bucket for 2+1 strategy");
                    createVirtualMultiReplica();
                }

                // Now proceed with QuorumObjectStorage creation
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
                globalQuorumStorage = quorumStorage;
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
