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
            throw new UnsupportedOperationException();
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
        // Create a default bucket URI if none is provided in config
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
        } catch (Exception e) {
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
            if (buckets != null && buckets.size() > 1) {
                objectStorage = protocolHandlers.get(PROTOCOL_ROOT).apply(this);
            } else {
                objectStorage = protocolHandlers.get(bucket.protocol()).apply(this);
            }
            return objectStorage;
        }
    }
}
