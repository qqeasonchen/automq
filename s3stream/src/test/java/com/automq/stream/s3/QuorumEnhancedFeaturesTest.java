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

package com.automq.stream.s3;

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;
import com.automq.stream.s3.operator.QuorumObjectStorage;
import com.automq.stream.s3.quorum.monitoring.QuorumMetricsCollector;
import com.automq.stream.s3.quorum.resilience.QuorumResilienceManager;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for S3 Quorum Storage enhanced features
 */
public class QuorumEnhancedFeaturesTest {
    
    @Test
    public void testQuorumObjectStorageCreation() throws Exception {
        // Create test buckets
        List<BucketURI> buckets = new ArrayList<>();
        buckets.add(BucketURI.parse("0@mem://test-bucket-1"));
        buckets.add(BucketURI.parse("1@mem://test-bucket-2"));
        buckets.add(BucketURI.parse("2@mem://test-bucket-3"));
        
        // Create QuorumObjectStorage
        ObjectStorage quorumStorage = ObjectStorageFactory.instance()
            .builder()
            .buckets(buckets)
            .quorumEnabled(true)
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .build();
        
        assertNotNull(quorumStorage);
        assertEquals("QuorumObjectStorage", quorumStorage.getClass().getSimpleName());
        assertTrue(quorumStorage.readinessCheck());
        
        if (quorumStorage instanceof QuorumObjectStorage) {
            QuorumObjectStorage qos = (QuorumObjectStorage) quorumStorage;
            assertEquals(3, qos.getReplicaCount());
            assertEquals(2, qos.getWriteQuorumSize());
            assertEquals(1, qos.getReadQuorumSize());
        }
        
        quorumStorage.close();
    }
    
    @Test
    public void testMetricsCollection() throws Exception {
        // Create metrics collector
        QuorumMetricsCollector.MetricsConfig config = 
            new QuorumMetricsCollector.MetricsConfig(false, 5, true);
        
        QuorumMetricsCollector metricsCollector = new QuorumMetricsCollector(config);
        
        // Test operation timing
        try (QuorumMetricsCollector.OperationTimer timer = metricsCollector.startOperation("test-operation")) {
            Thread.sleep(10); // Simulate operation time
            timer.success();
        }
        
        // Test gauge metrics
        metricsCollector.setGauge("active-connections", 5);
        metricsCollector.incrementGauge("total-requests");
        
        // Get metrics snapshot
        QuorumMetricsCollector.MetricsSnapshot snapshot = metricsCollector.getMetricsSnapshot();
        assertEquals(1, snapshot.getOperationMetrics().size());
        assertEquals(2, snapshot.getGaugeMetrics().size());
        
        metricsCollector.stop();
    }
    
    @Test
    public void testResilienceManager() throws Exception {
        // Create resilience manager
        QuorumResilienceManager.ResilienceConfig config = 
            QuorumResilienceManager.ResilienceConfig.defaultConfig();
        
        QuorumResilienceManager resilienceManager = new QuorumResilienceManager(config);
        
        // Test successful operation
        CompletableFuture<String> successOperation = resilienceManager.executeWithTimeout(
            () -> CompletableFuture.completedFuture("success"),
            "test-success-operation",
            Duration.ofSeconds(1)
        );
        
        String result = successOperation.get();
        assertEquals("success", result);
        
        // Test retry mechanism
        final int[] attemptCount = {0};
        CompletableFuture<String> retryOperation = resilienceManager.executeWithRetry(
            () -> {
                attemptCount[0]++;
                if (attemptCount[0] < 2) {
                    return CompletableFuture.failedFuture(new RuntimeException("Temporary failure"));
                }
                return CompletableFuture.completedFuture("retry-success");
            },
            "test-retry-operation",
            2
        );
        
        String retryResult = retryOperation.get();
        assertEquals("retry-success", retryResult);
        assertEquals(2, attemptCount[0]);
        
        // Test backoff calculation
        Duration backoff = resilienceManager.calculateBackoffDelay(2);
        assertTrue(backoff.toMillis() > 0);
        
        resilienceManager.shutdown();
    }
    
    @Test
    public void testConfigurationIntegration() throws Exception {
        // Test bucket parsing for multiple buckets
        String multipleBucketsConfig = "0@mem://bucket1,1@mem://bucket2,2@mem://bucket3";
        String[] bucketEntries = multipleBucketsConfig.split(",");
        
        List<BucketURI> parsedBuckets = new ArrayList<>();
        for (String bucketEntry : bucketEntries) {
            parsedBuckets.add(BucketURI.parse(bucketEntry.trim()));
        }
        
        assertEquals(3, parsedBuckets.size());
        
        // Test ObjectStorageFactory with quorum configuration
        ObjectStorage factoryStorage = ObjectStorageFactory.instance()
            .builder()
            .buckets(parsedBuckets)
            .quorumEnabled(true)
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1)
            .build();
        
        assertEquals("QuorumObjectStorage", factoryStorage.getClass().getSimpleName());
        
        factoryStorage.close();
    }
}