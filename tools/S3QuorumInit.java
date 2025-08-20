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

import com.automq.stream.s3.ByteBufAlloc;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.wal.impl.object.ObjectReservationService;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import io.netty.buffer.ByteBuf;

/**
 * AutoMQ S3 Quorum Storage Pre-initialization Tool
 * 
 * This tool creates the necessary S3 objects for S3 Quorum Storage functionality:
 * 1. Empty node range index objects for each replica
 * 2. WAL reservation objects for permission management
 * 3. Initial quorum metadata objects
 * 
 * Usage:
 * java -cp "core/build/dependant-libs-2.13.14/*:s3stream/build/libs/*:." S3QuorumInit \
 *   --config config/kraft-s3-quorum-development.properties \
 *   --cluster-id iPuq8hgRTtmRxKG47O-KYw \
 *   [--dry-run]
 */
public class S3QuorumInit {
    
    private static final String VERSION = "1.0.0";
    private static final short RANGE_INDEX_VERSION = 0;
    
    private Properties config;
    private String clusterId;
    private boolean dryRun = false;
    private ObjectStorage objectStorage;
    private String bucketName;
    
    public static void main(String[] args) {
        try {
            new S3QuorumInit().run(args);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    public void run(String[] args) throws Exception {
        parseArgs(args);
        loadConfiguration();
        validateConfiguration();
        initializeObjectStorage();
        
        System.out.println("AutoMQ S3 Quorum Storage Pre-initialization Tool v" + VERSION);
        System.out.println("==========================================================");
        System.out.println("Cluster ID: " + clusterId);
        System.out.println("Bucket: " + bucketName);
        
        if (dryRun) {
            System.out.println("DRY RUN MODE - No actual objects will be created");
        }
        
        // Initialize all necessary objects
        initializeQuorumObjects();
        
        System.out.println("✅ S3 Quorum Storage pre-initialization completed successfully!");
        System.out.println("You can now start Kafka with S3 Quorum Storage enabled.");
    }
    
    private void parseArgs(String[] args) throws Exception {
        String configFile = null;
        
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config":
                case "-c":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--config requires a file path");
                    }
                    configFile = args[++i];
                    break;
                    
                case "--cluster-id":
                case "-i":
                    if (i + 1 >= args.length) {
                        throw new IllegalArgumentException("--cluster-id requires a cluster UUID");
                    }
                    clusterId = args[++i];
                    break;
                    
                case "--dry-run":
                case "-d":
                    dryRun = true;
                    break;
                    
                case "--help":
                case "-h":
                    printHelp();
                    System.exit(0);
                    break;
                    
                default:
                    throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }
        
        if (configFile == null) {
            throw new IllegalArgumentException("Configuration file is required. Use --config option.");
        }
        
        if (clusterId == null) {
            throw new IllegalArgumentException("Cluster ID is required. Use --cluster-id option.");
        }
        
        // Load configuration from file
        config = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            config.load(fis);
        } catch (IOException e) {
            throw new Exception("Failed to load configuration file: " + configFile, e);
        }
    }
    
    private void printHelp() {
        System.out.println("AutoMQ S3 Quorum Storage Pre-initialization Tool");
        System.out.println();
        System.out.println("USAGE:");
        System.out.println("    java S3QuorumInit [OPTIONS]");
        System.out.println();
        System.out.println("OPTIONS:");
        System.out.println("    -c, --config FILE       Configuration file path (required)");
        System.out.println("    -i, --cluster-id ID     Cluster UUID (required)");
        System.out.println("    -d, --dry-run           Show what would be done without executing");
        System.out.println("    -h, --help              Show this help message");
        System.out.println();
        System.out.println("EXAMPLE:");
        System.out.println("    java -cp \"core/build/dependant-libs-2.13.14/*:s3stream/build/libs/*:.\" S3QuorumInit \\");
        System.out.println("      --config config/kraft-s3-quorum-development.properties \\");
        System.out.println("      --cluster-id iPuq8hgRTtmRxKG47O-KYw");
    }
    
    private void loadConfiguration() {
        System.out.println("Loading configuration...");
        
        // Extract key configuration values
        String quorumEnabled = config.getProperty("automq.s3.quorum.enabled", "false");
        String quorumSize = config.getProperty("automq.s3.quorum.size", "3");
        String dataBuckets = config.getProperty("s3.data.buckets", "");
        
        System.out.println("Configuration loaded:");
        System.out.println("  - S3 Quorum Enabled: " + quorumEnabled);
        System.out.println("  - Quorum Size: " + quorumSize);
        System.out.println("  - Data Buckets: " + dataBuckets);
        
        if (!"true".equals(quorumEnabled)) {
            System.out.println("⚠️  WARNING: S3 Quorum Storage is not enabled in configuration");
        }
    }
    
    private void validateConfiguration() throws Exception {
        String dataBuckets = config.getProperty("s3.data.buckets", "");
        if (dataBuckets.isEmpty()) {
            throw new Exception("s3.data.buckets configuration is required");
        }
        
        // Parse bucket URI to extract bucket name
        if (dataBuckets.contains("@s3://")) {
            String[] parts = dataBuckets.split("@s3://");
            if (parts.length > 1) {
                bucketName = parts[1].split("\\?")[0];
            }
        }
        
        if (bucketName == null || bucketName.isEmpty()) {
            throw new Exception("Could not extract bucket name from: " + dataBuckets);
        }
        
        System.out.println("Configuration validated successfully");
    }
    
    private void initializeObjectStorage() throws Exception {
        System.out.println("Initializing object storage connection...");
        
        String dataBuckets = config.getProperty("s3.data.buckets");
        BucketURI bucketURI = BucketURI.parse(dataBuckets);
        
        objectStorage = ObjectStorageFactory.instance().builder(bucketURI).build();
        
        System.out.println("Object storage initialized");
    }
    
    private void initializeQuorumObjects() throws Exception {
        String quorumSizeStr = config.getProperty("automq.s3.quorum.size", "3");
        int quorumSize = Integer.parseInt(quorumSizeStr);
        
        System.out.println("Initializing S3 objects for " + quorumSize + " replicas...");
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        // Create objects for each node/replica
        for (int nodeId = 0; nodeId < quorumSize; nodeId++) {
            futures.add(createNodeRangeIndex(nodeId));
            futures.add(createWALReservation(nodeId));
        }
        
        // Wait for all operations to complete
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(
            futures.toArray(new CompletableFuture[0])
        );
        
        try {
            allFutures.get();
            System.out.println("✅ All objects created successfully");
        } catch (Exception e) {
            System.err.println("❌ Failed to create some objects: " + e.getMessage());
            throw e;
        }
    }
    
    private CompletableFuture<Void> createNodeRangeIndex(int nodeId) {
        String objectKey = "streams-metadata/" + clusterId + "/node-" + nodeId + "/range-index";
        
        System.out.println("Creating node range index for node " + nodeId + " at " + objectKey);
        
        if (dryRun) {
            System.out.println("[DRY RUN] Would create range index: " + objectKey);
            return CompletableFuture.completedFuture(null);
        }
        
        // Create empty range index buffer
        // Format: version (2 bytes) + stream count (4 bytes) = minimal valid empty index
        ByteBuf buffer = ByteBufAlloc.byteBuffer(6);
        buffer.writeShort(RANGE_INDEX_VERSION);  // Version = 0
        buffer.writeInt(0);                      // Stream count = 0
        
        return objectStorage.write(
            ObjectStorage.WriteOptions.DEFAULT,
            objectKey,
            buffer
        ).handle((result, throwable) -> {
            if (throwable != null) {
                System.err.println("❌ Failed to create range index for node " + nodeId + ": " + throwable.getMessage());
                throw new RuntimeException(throwable);
            } else {
                System.out.println("✅ Created range index for node " + nodeId);
                return null;
            }
        });
    }
    
    private CompletableFuture<Void> createWALReservation(int nodeId) {
        if (dryRun) {
            System.out.println("[DRY RUN] Would create WAL reservation for node " + nodeId);
            return CompletableFuture.completedFuture(null);
        }
        
        System.out.println("Creating WAL reservation for node " + nodeId);
        
        // Use ObjectReservationService to create the reservation
        ObjectReservationService reservationService = new ObjectReservationService(
            clusterId, objectStorage, (short) 0
        );
        
        long nodeEpoch = System.currentTimeMillis();
        
        return reservationService.acquire(nodeId, nodeEpoch, false)
            .handle((result, throwable) -> {
                if (throwable != null) {
                    System.err.println("❌ Failed to create WAL reservation for node " + nodeId + ": " + throwable.getMessage());
                    throw new RuntimeException(throwable);
                } else {
                    System.out.println("✅ Created WAL reservation for node " + nodeId);
                    return null;
                }
            });
    }
}