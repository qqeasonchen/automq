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

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class ObjectStorageFactory {
    public static final String PROTOCOL_ROOT = "root";
    public static final String EXTENSION_TYPE_KEY = "type";
    public static final String EXTENSION_TYPE_MAIN = "main";
    public static final String EXTENSION_TYPE_BACKGROUND = "background";
    private static volatile ObjectStorageFactory instance;
    private final Map<String /* protocol */, Function<Builder, ObjectStorage>> protocolHandlers = new HashMap<>();

    static {
        ObjectStorageFactory.instance().registerProtocolHandler(PROTOCOL_ROOT, builder -> {
            if (builder.quorumEnabled() && builder.buckets() != null && builder.buckets().size() > 1) {
                return new QuorumObjectStorage(builder);
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
            .registerProtocolHandler("s3", builder ->
                AwsObjectStorage.builder()
                    .bucket(builder.bucket)
                    .tagging(builder.tagging)
                    .inboundLimiter(builder.inboundLimiter)
                    .outboundLimiter(builder.outboundLimiter)
                    .readWriteIsolate(builder.readWriteIsolate)
                    .checkS3ApiModel(builder.checkS3ApiModel)
                    .threadPrefix(builder.threadPrefix)
                    .build())
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
        // Force multi-replica for ALL ObjectStorage instances
        System.err.println("🔧 ObjectStorageFactory.builder(BucketURI) - Forcing multi-replica mode!");
        System.err.println("  Original bucket: " + bucket);
        
        // Create multi-replica buckets list
        List<BucketURI> multiBuckets = createMultiReplicaBuckets(bucket);
        
        return new Builder()
            .buckets(multiBuckets)
            .quorumEnabled(true)
            .quorumSize(3)
            .writeQuorumSize(3)
            .readQuorumSize(2);
    }
    
    /**
     * Create multi-replica buckets based on a single bucket
     */
    private List<BucketURI> createMultiReplicaBuckets(BucketURI originalBucket) {
        List<BucketURI> buckets = new ArrayList<>();
        
        try {
            // Primary bucket (localhost:9000)
            buckets.add(BucketURI.parse("0@s3://automq-multi-replica-primary?region=us-east-1&endpoint=http://localhost:9000&pathStyle=true&authType=static&accessKey=minioadmin&secretKey=minioadmin"));
            
            // Secondary-1 bucket (localhost:9010)
            buckets.add(BucketURI.parse("1@s3://automq-multi-replica-primary?region=us-east-1&endpoint=http://localhost:9010&pathStyle=true&authType=static&accessKey=minioadmin&secretKey=minioadmin"));
            
            // Secondary-2 bucket (localhost:9020)
            buckets.add(BucketURI.parse("2@s3://automq-multi-replica-primary?region=us-east-1&endpoint=http://localhost:9020&pathStyle=true&authType=static&accessKey=minioadmin&secretKey=minioadmin"));
            
            System.err.println("  Created " + buckets.size() + " replica buckets for multi-replica mode");
            
        } catch (Exception e) {
            System.err.println("  Failed to create multi-replica buckets, falling back to original: " + e.getMessage());
            buckets.clear();
            buckets.add(originalBucket);
        }
        
        return buckets;
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
        // Check if S3 Quorum is enabled
        boolean quorumEnabled = false;
        int quorumSize = 3;
        int writeQuorumSize = 2;
        int readQuorumSize = 1;
        
        // Debug output
        System.err.println("ObjectStorageFactory.createObjectStorage() DEBUG:");
        
        try {
            // Use reflection to check for quorum configuration
            java.lang.reflect.Method getQuorumEnabledMethod = config.getClass().getMethod("quorumEnabled");
            Object quorumEnabledValue = getQuorumEnabledMethod.invoke(config);
            System.err.println("  config.quorumEnabled() = " + quorumEnabledValue);
            if (quorumEnabledValue instanceof Boolean) {
                quorumEnabled = (Boolean) quorumEnabledValue;
            }
            
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
                } catch (Exception e) {
                    // Use default write quorum size
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
            System.err.println("  Exception getting quorumEnabled: " + e.getMessage());
        }
        
        // Also check if we have multiple data buckets - if so, enable quorum automatically
        List<BucketURI> dataBuckets = null;
        try {
            java.lang.reflect.Method getDataBucketsMethod = config.getClass().getMethod("dataBuckets");
            Object dataBucketsValue = getDataBucketsMethod.invoke(config);
            System.err.println("  config.dataBuckets() = " + dataBucketsValue);
            if (dataBucketsValue instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<BucketURI> uncheckedBuckets = (List<BucketURI>) dataBucketsValue;
                dataBuckets = uncheckedBuckets;
                System.err.println("  dataBuckets.size() = " + dataBuckets.size());
                // If we have multiple buckets, enable quorum automatically
                if (dataBuckets.size() > 1) {
                    System.err.println("  Auto-enabling quorum due to multiple data buckets");
                    quorumEnabled = true;
                }
            }
        } catch (Exception e) {
            System.err.println("  Exception getting dataBuckets: " + e.getMessage());
        }
        
        System.err.println("  Final quorumEnabled = " + quorumEnabled);
        
        if (quorumEnabled) {
            // Use the dataBuckets we already retrieved
            List<BucketURI> buckets = dataBuckets;
            
            if (buckets == null || buckets.isEmpty()) {
                System.err.println("  No dataBuckets available, creating default quorum buckets");
                buckets = createQuorumBuckets(config, extensionType, quorumSize);
            }
            
            System.err.println("createObjectStorage() with quorumEnabled=true, using buckets: " + buckets);
            
            return instance().builder()
                .buckets(buckets)
                .quorumEnabled(true)
                .quorumSize(quorumSize)
                .writeQuorumSize(writeQuorumSize)
                .readQuorumSize(readQuorumSize)
                .extension(EXTENSION_TYPE_KEY, extensionType)
                .build();
        } else {
            // Create a default bucket URI if none is provided in config
            BucketURI defaultBucket = createDefaultBucket(config, extensionType);
            
            return instance().builder()
                .bucket(defaultBucket)
                .extension(EXTENSION_TYPE_KEY, extensionType)
                .build();
        }
    }
    
    /**
     * Create multiple bucket URIs for quorum configuration
     */
    private static List<BucketURI> createQuorumBuckets(Config config, String extensionType, int quorumSize) {
        List<BucketURI> buckets = new ArrayList<>();
        
        // Try to get bucket configurations from Config for quorum replicas
        try {
            // Check for data buckets configuration (which may contain multiple buckets)
            java.lang.reflect.Method getDataBucketsMethod = config.getClass().getMethod("dataBuckets");
            Object dataBucketsValue = getDataBucketsMethod.invoke(config);
            if (dataBucketsValue instanceof List<?>) {
                @SuppressWarnings("unchecked")
                List<BucketURI> configBuckets = (List<BucketURI>) dataBucketsValue;
                buckets.addAll(configBuckets);
            }
        } catch (Exception e) {
            // Data buckets not configured, will use default
        }
        
        // If we have enough buckets from config, return them
        if (buckets.size() >= quorumSize) {
            return buckets.subList(0, quorumSize);
        }
        
        // If we don't have enough buckets from config, create default ones
        while (buckets.size() < quorumSize) {
            int bucketId = buckets.size();
            String bucketStr;
            
            if ("main".equals(extensionType)) {
                bucketStr = bucketId + "@mem://quorum-main-bucket-" + bucketId;
            } else if ("background".equals(extensionType)) {
                bucketStr = bucketId + "@mem://quorum-background-bucket-" + bucketId;
            } else {
                bucketStr = bucketId + "@mem://quorum-default-bucket-" + bucketId;
            }
            
            buckets.add(BucketURI.parse(bucketStr));
        }
        
        return buckets;
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
            
            // Critical debug: show calling stack trace to understand who's calling build()
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            System.err.println("🔍 ObjectStorageFactory.Builder.build() ENTRY:");
            System.err.println("  quorumEnabled: " + quorumEnabled);
            System.err.println("  buckets: " + buckets);
            System.err.println("  bucket: " + bucket);
            System.err.println("  Called from: " + (stackTrace.length > 2 ? stackTrace[2].getClassName() + "." + stackTrace[2].getMethodName() + ":" + stackTrace[2].getLineNumber() : "N/A"));
            System.err.println("  Call stack depth: " + (stackTrace.length > 3 ? stackTrace[3].getClassName() + "." + stackTrace[3].getMethodName() : "N/A"));
            if (buckets != null) {
                System.err.println("  buckets.size(): " + buckets.size());
                for (int i = 0; i < buckets.size(); i++) {
                    System.err.println("    buckets[" + i + "]: " + buckets.get(i));
                    System.err.println("    buckets[" + i + "].protocol(): " + buckets.get(i).protocol());
                }
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
            
            // CRITICAL FIX: Force enable quorum when we have multiple buckets
            if (buckets != null && buckets.size() > 1) {
                if (!quorumEnabled) {
                    System.err.println("  🔥 FORCING quorum mode for multiple buckets! buckets.size() = " + buckets.size());
                    System.err.println("  🔥 This fixes StreamClientFactory single-bucket issue");
                }
                // Create Quorum ObjectStorage for multiple buckets (forced)
                System.err.println("  Creating QuorumObjectStorage (forced for multiple buckets)");
                try {
                    objectStorage = new QuorumObjectStorage(this);
                    System.err.println("  QuorumObjectStorage created successfully");
                } catch (Exception e) {
                    System.err.println("  QuorumObjectStorage creation failed: " + e.getMessage());
                    e.printStackTrace();
                    throw e;
                }
            } else {
                // Single bucket mode
                System.err.println("  Single bucket mode, using bucket: " + bucket.protocol());
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
