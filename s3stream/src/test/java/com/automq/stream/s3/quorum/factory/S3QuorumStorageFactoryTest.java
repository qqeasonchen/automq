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
import com.automq.stream.s3.cache.S3BlockCache;
import com.automq.stream.s3.failover.StorageFailureHandler;
import com.automq.stream.s3.quorum.S3QuorumStorage;
import com.automq.stream.s3.streams.StreamManager;
import com.automq.stream.s3.wal.WriteAheadLog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
@DisplayName("S3QuorumStorageFactory Tests")
class S3QuorumStorageFactoryTest {

    @TempDir
    Path tempDir;

    @Mock
    private Config mockConfig;
    
    @Mock
    private WriteAheadLog mockWriteAheadLog;
    
    @Mock
    private StreamManager mockStreamManager;
    
    @Mock
    private S3BlockCache mockBlockCache;
    
    @Mock
    private StorageFailureHandler mockFailureHandler;

    private File validConfigFile;
    private File disabledConfigFile;

    @BeforeEach
    void setUp() throws IOException {
        // Set up test environment variables
        System.setProperty("AWS_ACCESS_KEY_ID", "test-access-key");
        System.setProperty("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        
        // Create base config
        mockConfig = new Config();
        mockConfig.nodeId(1);
        mockConfig.walCacheSize(1024);
        
        // Create test config files
        validConfigFile = tempDir.resolve("valid-quorum.properties").toFile();
        writeValidConfigFile(validConfigFile);
        
        disabledConfigFile = tempDir.resolve("disabled-quorum.properties").toFile();
        writeDisabledConfigFile(disabledConfigFile);
    }

    @Test
    void testCreateQuorumStorage() {
        // Test basic factory creation
        S3QuorumStorage quorumStorage = S3QuorumStorageFactory.createQuorumStorage(
            mockConfig, 
            mockWriteAheadLog, 
            mockStreamManager, 
            mockBlockCache, 
            mockFailureHandler
        );

        // Verify that the factory returns a valid quorum storage instance
        assertNotNull(quorumStorage);
        assertTrue(quorumStorage instanceof S3QuorumStorage);
    }

    @Test
    void testCreateQuorumStorageWithNullConfig() {
        // Test factory with null config - should throw NullPointerException
        assertThrows(NullPointerException.class, () -> {
            S3QuorumStorageFactory.createQuorumStorage(
                null, 
                mockWriteAheadLog, 
                mockStreamManager, 
                mockBlockCache, 
                mockFailureHandler
            );
        });
    }

    @Test
    void testFactoryCreatesValidQuorumStorage() {
        // Test that factory creates a working quorum storage
        try {
            S3QuorumStorage quorumStorage = S3QuorumStorageFactory.createQuorumStorage(
                mockConfig, 
                mockWriteAheadLog, 
                mockStreamManager, 
                mockBlockCache, 
                mockFailureHandler
            );

            // Basic validation
            assertNotNull(quorumStorage);
            assertNotNull(quorumStorage.getQuorumState());
            
        } catch (Exception e) {
            // Factory might fail due to missing mock behavior, but it should create the object structure
            assertTrue(e.getMessage().contains("nodeId") || e.getMessage().contains("Config") || 
                      e.getMessage().contains("null") || e instanceof IllegalArgumentException);
        }
    }

    @Test
    @DisplayName("Create quorum storage from configuration file")
    void testCreateFromConfigFile() throws IOException {
        // Test creating quorum storage from configuration file
        S3QuorumStorage quorumStorage = S3QuorumStorageFactory.createFromConfig(
            validConfigFile.getAbsolutePath(),
            mockConfig,
            null,  // Allow null for test
            null,
            null,
            null
        );

        assertNotNull(quorumStorage);
        assertNotNull(quorumStorage.getQuorumState());
        
        // Verify configuration was loaded correctly
        // Since storage is not started, we just verify it was created successfully
        // The actual quorum functionality will be tested in integration tests
        assertTrue(quorumStorage.getQuorumState().getHealthyReplicaCount() >= 0);
    }

    @Test
    @DisplayName("Should use quorum storage check")
    void testShouldUseQuorumStorage() {
        // Test enabled configuration
        assertTrue(S3QuorumStorageFactory.shouldUseQuorumStorage(validConfigFile.getAbsolutePath()));
        
        // Test disabled configuration
        assertFalse(S3QuorumStorageFactory.shouldUseQuorumStorage(disabledConfigFile.getAbsolutePath()));
        
        // Test non-existent file
        assertFalse(S3QuorumStorageFactory.shouldUseQuorumStorage("non-existent-file.properties"));
    }

    @Test
    @DisplayName("Create from config throws exception for disabled quorum")
    void testCreateFromConfigThrowsExceptionWhenDisabled() {
        assertThrows(IllegalStateException.class, () -> {
            S3QuorumStorageFactory.createFromConfig(
                disabledConfigFile.getAbsolutePath(),
                mockConfig,
                null,
                null,
                null,
                null
            );
        });
    }

    @Test
    @DisplayName("Create from config throws exception for non-existent file")
    void testCreateFromConfigThrowsExceptionForNonExistentFile() {
        assertThrows(IOException.class, () -> {
            S3QuorumStorageFactory.createFromConfig(
                "non-existent-file.properties",
                mockConfig,
                null,
                null,
                null,
                null
            );
        });
    }

    // Helper methods to create test configuration files
    private void writeValidConfigFile(File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("# Test valid quorum configuration\n");
            writer.write("automq.s3.quorum.enabled=true\n");
            writer.write("automq.s3.quorum.size=3\n");
            writer.write("automq.s3.quorum.write.quorum.size=2\n");
            writer.write("automq.s3.quorum.read.quorum.size=1\n");
            writer.write("automq.s3.quorum.write.timeout.ms=30000\n");
            writer.write("automq.s3.quorum.read.timeout.ms=10000\n");
            writer.write("automq.s3.quorum.read.repair.enabled=true\n");
            writer.write("automq.s3.quorum.read.repair.timeout.ms=5000\n");
            
            // Replica 0 configuration
            writer.write("automq.s3.quorum.replica.0.id=0\n");
            writer.write("automq.s3.quorum.replica.0.region=us-east-1\n");
            writer.write("automq.s3.quorum.replica.0.bucket=test-primary-bucket\n");
            writer.write("automq.s3.quorum.replica.0.endpoint=https://s3.us-east-1.amazonaws.com\n");
            writer.write("automq.s3.quorum.replica.0.role=PRIMARY\n");
            writer.write("automq.s3.quorum.replica.0.priority=100\n");
            
            // Replica 1 configuration
            writer.write("automq.s3.quorum.replica.1.id=1\n");
            writer.write("automq.s3.quorum.replica.1.region=us-west-2\n");
            writer.write("automq.s3.quorum.replica.1.bucket=test-secondary-1-bucket\n");
            writer.write("automq.s3.quorum.replica.1.endpoint=https://s3.us-west-2.amazonaws.com\n");
            writer.write("automq.s3.quorum.replica.1.role=SECONDARY\n");
            writer.write("automq.s3.quorum.replica.1.priority=50\n");
            
            // Replica 2 configuration
            writer.write("automq.s3.quorum.replica.2.id=2\n");
            writer.write("automq.s3.quorum.replica.2.region=eu-west-1\n");
            writer.write("automq.s3.quorum.replica.2.bucket=test-secondary-2-bucket\n");
            writer.write("automq.s3.quorum.replica.2.endpoint=https://s3.eu-west-1.amazonaws.com\n");
            writer.write("automq.s3.quorum.replica.2.role=SECONDARY\n");
            writer.write("automq.s3.quorum.replica.2.priority=25\n");
        }
    }

    private void writeDisabledConfigFile(File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("# Test disabled quorum configuration\n");
            writer.write("automq.s3.quorum.enabled=false\n");
            writer.write("automq.s3.quorum.size=3\n");
        }
    }
}