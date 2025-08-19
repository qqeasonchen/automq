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

package com.automq.stream.s3.quorum.config;

import com.automq.stream.s3.Config;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for QuorumConfigLoader
 */
@DisplayName("QuorumConfigLoader Tests")
class QuorumConfigLoaderTest {

    @TempDir
    Path tempDir;

    private Config baseConfig;
    private File validConfigFile;
    private File disabledConfigFile;
    private File invalidConfigFile;

    @BeforeEach
    void setUp() throws IOException {
        // Set up environment variables for testing
        System.setProperty("AWS_ACCESS_KEY_ID", "test-access-key");
        System.setProperty("AWS_SECRET_ACCESS_KEY", "test-secret-key");
        
        // Create base config
        baseConfig = new Config();
        baseConfig.nodeId(1);
        baseConfig.walCacheSize(1024);
        
        // Create valid config file
        validConfigFile = tempDir.resolve("valid-quorum.properties").toFile();
        writeValidConfigFile(validConfigFile);
        
        // Create disabled config file
        disabledConfigFile = tempDir.resolve("disabled-quorum.properties").toFile();
        writeDisabledConfigFile(disabledConfigFile);
        
        // Create invalid config file
        invalidConfigFile = tempDir.resolve("invalid-quorum.properties").toFile();
        writeInvalidConfigFile(invalidConfigFile);
    }

    @Test
    @DisplayName("Load valid quorum configuration from file")
    void testLoadValidConfiguration() throws IOException {
        // Override environment variables for this test
        String originalAccessKey = System.getenv("AWS_ACCESS_KEY_ID");
        String originalSecretKey = System.getenv("AWS_SECRET_ACCESS_KEY");
        
        try {
            // Set test environment variables
            System.setProperty("AWS_ACCESS_KEY_ID", "test-access-key");
            System.setProperty("AWS_SECRET_ACCESS_KEY", "test-secret-key");
            
            QuorumConfig config = QuorumConfigLoader.loadFromProperties(
                validConfigFile.getAbsolutePath(), baseConfig);
            
            // Verify basic quorum settings
            assertNotNull(config);
            assertEquals(3, config.getQuorumSize());
            assertEquals(2, config.getWriteQuorumSize());
            assertEquals(1, config.getReadQuorumSize());
            assertEquals(30000L, config.getWriteTimeoutMs());
            assertEquals(10000L, config.getReadTimeoutMs());
            assertTrue(config.isEnableReadRepair());
            assertEquals(5000L, config.getReadRepairTimeoutMs());
            
            // Verify replica configurations
            List<ReplicaConfig> replicas = config.getReplicaConfigs();
            assertNotNull(replicas);
            assertEquals(3, replicas.size());
            
            // Verify primary replica (replica 0)
            ReplicaConfig primary = replicas.get(0);
            assertEquals(0, primary.getReplicaId());
            assertEquals("us-east-1", primary.getRegion());
            assertEquals("test-primary-bucket", primary.getBucket());
            assertEquals("https://s3.us-east-1.amazonaws.com", primary.getEndpoint());
            assertEquals(ReplicaConfig.ReplicaRole.PRIMARY, primary.getRole());
            assertEquals(100L, primary.getPriority());
            
            // Verify secondary replica (replica 1)
            ReplicaConfig secondary1 = replicas.get(1);
            assertEquals(1, secondary1.getReplicaId());
            assertEquals("us-west-2", secondary1.getRegion());
            assertEquals("test-secondary-1-bucket", secondary1.getBucket());
            assertEquals("https://s3.us-west-2.amazonaws.com", secondary1.getEndpoint());
            assertEquals(ReplicaConfig.ReplicaRole.SECONDARY, secondary1.getRole());
            assertEquals(50L, secondary1.getPriority());
            
            // Verify second secondary replica (replica 2)
            ReplicaConfig secondary2 = replicas.get(2);
            assertEquals(2, secondary2.getReplicaId());
            assertEquals("eu-west-1", secondary2.getRegion());
            assertEquals("test-secondary-2-bucket", secondary2.getBucket());
            assertEquals("https://s3.eu-west-1.amazonaws.com", secondary2.getEndpoint());
            assertEquals(ReplicaConfig.ReplicaRole.SECONDARY, secondary2.getRole());
            assertEquals(25L, secondary2.getPriority());
            
        } finally {
            // Restore original environment variables
            if (originalAccessKey != null) {
                System.setProperty("AWS_ACCESS_KEY_ID", originalAccessKey);
            }
            if (originalSecretKey != null) {
                System.setProperty("AWS_SECRET_ACCESS_KEY", originalSecretKey);
            }
        }
    }

    @Test
    @DisplayName("Check if quorum is enabled in configuration file")
    void testIsQuorumEnabled() {
        assertTrue(QuorumConfigLoader.isQuorumEnabled(validConfigFile.getAbsolutePath()));
        assertFalse(QuorumConfigLoader.isQuorumEnabled(disabledConfigFile.getAbsolutePath()));
        assertFalse(QuorumConfigLoader.isQuorumEnabled("non-existent-file.properties"));
    }

    @Test
    @DisplayName("Throw exception when quorum is disabled")
    void testDisabledQuorumThrowsException() {
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            QuorumConfigLoader.loadFromProperties(disabledConfigFile.getAbsolutePath(), baseConfig);
        });
        
        assertTrue(exception.getMessage().contains("not enabled"));
    }

    @Test
    @DisplayName("Throw exception for invalid configuration")
    void testInvalidConfigurationThrowsException() {
        // Remove environment variables to simulate missing credentials
        System.clearProperty("AWS_ACCESS_KEY_ID");
        System.clearProperty("AWS_SECRET_ACCESS_KEY");
        
        assertThrows(IllegalStateException.class, () -> {
            QuorumConfigLoader.loadFromProperties(invalidConfigFile.getAbsolutePath(), baseConfig);
        });
    }

    @Test
    @DisplayName("Throw exception for non-existent file")
    void testNonExistentFileThrowsException() {
        assertThrows(IOException.class, () -> {
            QuorumConfigLoader.loadFromProperties("non-existent-file.properties", baseConfig);
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

    private void writeInvalidConfigFile(File file) throws IOException {
        try (FileWriter writer = new FileWriter(file)) {
            writer.write("# Test invalid quorum configuration (missing required properties)\n");
            writer.write("automq.s3.quorum.enabled=true\n");
            writer.write("automq.s3.quorum.size=3\n");
            writer.write("automq.s3.quorum.write.quorum.size=2\n");
            writer.write("automq.s3.quorum.read.quorum.size=1\n");
            // Missing replica configurations
        }
    }
}