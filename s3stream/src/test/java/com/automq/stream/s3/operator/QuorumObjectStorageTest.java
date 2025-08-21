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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for QuorumObjectStorage
 */
@ExtendWith(MockitoExtension.class)
public class QuorumObjectStorageTest {
    
    @Mock
    private ObjectStorage replica1;
    
    @Mock
    private ObjectStorage replica2;
    
    @Mock
    private ObjectStorage replica3;
    
    private QuorumObjectStorage quorumStorage;
    private ObjectStorageFactory.Builder builder;
    
    @BeforeEach
    void setUp() {
        // Setup mock behaviors
        when(replica1.readinessCheck()).thenReturn(true);
        when(replica2.readinessCheck()).thenReturn(true);
        when(replica3.readinessCheck()).thenReturn(true);
        when(replica1.bucketId()).thenReturn((short) 0);
        when(replica2.bucketId()).thenReturn((short) 1);
        when(replica3.bucketId()).thenReturn((short) 2);
        
        // Create builder with mock replicas
        List<BucketURI> buckets = new ArrayList<>();
        buckets.add(BucketURI.parse("0@mem://test-bucket-1"));
        buckets.add(BucketURI.parse("1@mem://test-bucket-2"));
        buckets.add(BucketURI.parse("2@mem://test-bucket-3"));
        
        builder = ObjectStorageFactory.instance().builder()
            .buckets(buckets)
            .quorumEnabled(true)
            .quorumSize(3)
            .writeQuorumSize(2)
            .readQuorumSize(1);
        
        // Create QuorumObjectStorage with manual replica injection for testing
        quorumStorage = new TestableQuorumObjectStorage(builder, List.of(replica1, replica2, replica3));
    }
    
    @Test
    void testQuorumProperties() {
        assertEquals(3, quorumStorage.getReplicaCount());
        assertEquals(2, quorumStorage.getWriteQuorumSize());
        assertEquals(1, quorumStorage.getReadQuorumSize());
    }
    
    @Test
    void testReadinessCheck() {
        // All replicas ready - should return true
        assertTrue(quorumStorage.readinessCheck());
        
        // One replica not ready - should still return true (1 >= readQuorumSize)
        when(replica1.readinessCheck()).thenReturn(false);
        assertTrue(quorumStorage.readinessCheck());
        
        // Only one replica ready - should still return true
        when(replica2.readinessCheck()).thenReturn(false);
        assertTrue(quorumStorage.readinessCheck());
        
        // No replicas ready - should return false
        when(replica3.readinessCheck()).thenReturn(false);
        assertFalse(quorumStorage.readinessCheck());
    }
    
    @Test
    void testSuccessfulQuorumWrite() throws Exception {
        // Setup successful writes for 2 out of 3 replicas
        when(replica1.write(any(), anyString(), any())).thenReturn(
            CompletableFuture.completedFuture(new ObjectStorage.WriteResult((short) 0)));
        when(replica2.write(any(), anyString(), any())).thenReturn(
            CompletableFuture.completedFuture(new ObjectStorage.WriteResult((short) 1)));
        when(replica3.write(any(), anyString(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Write failed")));
        
        ByteBuf testData = Unpooled.copiedBuffer("test data".getBytes());
        CompletableFuture<ObjectStorage.WriteResult> result = quorumStorage.write(
            new ObjectStorage.WriteOptions(), "test-key", testData);
        
        // Should succeed because 2/3 >= writeQuorumSize (2)
        ObjectStorage.WriteResult writeResult = result.get();
        assertNotNull(writeResult);
        
        testData.release();
    }
    
    @Test
    void testFailedQuorumWrite() {
        // Setup failed writes for 2 out of 3 replicas (only 1 succeeds)
        when(replica1.write(any(), anyString(), any())).thenReturn(
            CompletableFuture.completedFuture(new ObjectStorage.WriteResult((short) 0)));
        when(replica2.write(any(), anyString(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Write failed")));
        when(replica3.write(any(), anyString(), any())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Write failed")));
        
        ByteBuf testData = Unpooled.copiedBuffer("test data".getBytes());
        CompletableFuture<ObjectStorage.WriteResult> result = quorumStorage.write(
            new ObjectStorage.WriteOptions(), "test-key", testData);
        
        // Should fail because 1/3 < writeQuorumSize (2)
        assertThrows(Exception.class, result::get);
        
        testData.release();
    }
    
    @Test
    void testSuccessfulQuorumRead() throws Exception {
        // Setup successful read from first replica
        ByteBuf expectedData = Unpooled.copiedBuffer("test data".getBytes());
        when(replica1.read(any(), anyString())).thenReturn(
            CompletableFuture.completedFuture(expectedData));
        when(replica2.read(any(), anyString())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Read failed")));
        when(replica3.read(any(), anyString())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Read failed")));
        
        CompletableFuture<ByteBuf> result = quorumStorage.read(
            new ObjectStorage.ReadOptions(), "test-key");
        
        // Should succeed because 1/3 >= readQuorumSize (1)
        ByteBuf readData = result.get();
        assertNotNull(readData);
        assertEquals("test data", readData.toString(java.nio.charset.StandardCharsets.UTF_8));
        
        expectedData.release();
        readData.release();
    }
    
    @Test
    void testFailedQuorumRead() {
        // Setup failed reads from all replicas
        when(replica1.read(any(), anyString())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Read failed")));
        when(replica2.read(any(), anyString())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Read failed")));
        when(replica3.read(any(), anyString())).thenReturn(
            CompletableFuture.failedFuture(new RuntimeException("Read failed")));
        
        CompletableFuture<ByteBuf> result = quorumStorage.read(
            new ObjectStorage.ReadOptions(), "test-key");
        
        // Should fail because 0/3 < readQuorumSize (1)
        assertThrows(Exception.class, result::get);
    }
    
    @Test
    void testBucketId() {
        assertEquals(0, quorumStorage.bucketId());
    }
    
    /**
     * Testable subclass that allows injecting mock replicas
     */
    private static class TestableQuorumObjectStorage extends QuorumObjectStorage {
        private final List<ObjectStorage> testReplicas;
        
        public TestableQuorumObjectStorage(ObjectStorageFactory.Builder builder, List<ObjectStorage> testReplicas) {
            super(builder);
            this.testReplicas = testReplicas;
        }
        
        @Override
        protected List<ObjectStorage> createReplicas(ObjectStorageFactory.Builder builder) {
            return testReplicas;
        }
    }
}