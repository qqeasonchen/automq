/*
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

package org.apache.kafka.server.common;

import com.automq.stream.s3.operator.ObjectStorage;

import java.util.List;

/**
 * Configuration class for S3 Kraft snapshot operations.
 * This class provides a centralized configuration mechanism for S3 snapshot reading and writing,
 * avoiding direct dependencies on core modules and preventing circular dependencies.
 */
public class S3SnapshotConfig {

    /**
     * Configuration key for enabling S3 Kraft snapshot reading.
     * This should match kafka.automq.AutoMQConfig.S3_KRAFT_SNAPSHOT_READ_ENABLE_CONFIG
     */
    public static final String S3_KRAFT_SNAPSHOT_READ_ENABLE_CONFIG = "s3.kraft.snapshot.read.enable";

    /**
     * Data bucket configuration for S3 operations.
     * Inner class to encapsulate bucket information.
     */
    public static class DataBucket {
        private final String bucket;
        private final String region;

        public DataBucket(String bucket, String region) {
            this.bucket = bucket;
            this.region = region;
        }

        public String bucket() {
            return bucket;
        }

        public String region() {
            return region;
        }
    }

    private boolean s3KraftSnapshotReadEnabled;
    private boolean s3KraftSnapshotWriteEnabled;
    private final ObjectStorage objectStorage;
    private final List<DataBucket> dataBuckets;
    private final boolean objectTagging;
    private final List<String> logDirs;

    /**
     * Create a new S3SnapshotConfig instance.
     *
     * @param s3KraftSnapshotReadEnabled Whether S3 Kraft snapshot reading is enabled
     * @param s3KraftSnapshotWriteEnabled Whether S3 Kraft snapshot reading is enabled
     * @param objectStorage The ObjectStorage instance for S3 operations, can be null if disabled
     * @param dataBuckets List of data buckets for S3 operations
     * @param objectTagging Whether object tagging is enabled
     * @param logDirs List of log directories from log.dirs configuration
     */
    public S3SnapshotConfig(boolean s3KraftSnapshotReadEnabled, boolean s3KraftSnapshotWriteEnabled, ObjectStorage objectStorage,
                           List<DataBucket> dataBuckets, boolean objectTagging, List<String> logDirs) {
        this.s3KraftSnapshotReadEnabled = s3KraftSnapshotReadEnabled;
        this.s3KraftSnapshotWriteEnabled = s3KraftSnapshotWriteEnabled;
        this.objectStorage = objectStorage;
        this.dataBuckets = dataBuckets;
        this.objectTagging = objectTagging;
        this.logDirs = logDirs;
    }

    /**
     * Create a new S3SnapshotConfig instance with minimal parameters (for backward compatibility).
     *
     * @param s3KraftSnapshotReadEnabled Whether S3 Kraft snapshot reading is enabled
     * @param s3KraftSnapshotWriteEnabled Whether S3 Kraft snapshot writing is enabled
     * @param objectStorage The ObjectStorage instance for S3 operations, can be null if disabled
     */
    public S3SnapshotConfig(boolean s3KraftSnapshotReadEnabled, boolean s3KraftSnapshotWriteEnabled, ObjectStorage objectStorage) {
        this(s3KraftSnapshotReadEnabled, s3KraftSnapshotWriteEnabled, objectStorage, null, false, null);
    }

    /**
     * Check if S3 Kraft snapshot reading is enabled.
     *
     * @return true if S3 snapshot reading is enabled, false otherwise
     */
    public boolean isS3KraftSnapshotReadEnabled() {
        return s3KraftSnapshotReadEnabled;
    }

    public boolean isS3KraftSnapshotWriteEnabled() {
        return s3KraftSnapshotWriteEnabled;
    }

    /**
     * Get the ObjectStorage instance for S3 operations.
     *
     * @return ObjectStorage instance, or null if S3 snapshot reading is disabled
     */
    public ObjectStorage getObjectStorage() {
        return objectStorage;
    }

    /**
     * Get the list of data buckets for S3 operations.
     *
     * @return List of DataBucket instances, or null if not configured
     */
    public List<DataBucket> getDataBuckets() {
        return dataBuckets;
    }

    /**
     * Check if object tagging is enabled.
     *
     * @return true if object tagging is enabled, false otherwise
     */
    public boolean isObjectTagging() {
        return objectTagging;
    }

    /**
     * Get the list of log directories from log.dirs configuration.
     *
     * @return List of log directory paths, or null if not configured
     */
    public List<String> getLogDirs() {
        return logDirs;
    }

    /**
     * Create a disabled S3SnapshotConfig instance.
     *
     * @return S3SnapshotConfig with S3 snapshot reading disabled
     */
    public static S3SnapshotConfig disabled() {
        return new S3SnapshotConfig(false,false, null);
    }

    @Override
    public String toString() {
        return "S3SnapshotConfig{" +
            "s3KraftSnapshotReadEnabled=" + s3KraftSnapshotReadEnabled +
            "s3KraftSnapshotWriteEnabled=" + s3KraftSnapshotWriteEnabled +
            ", objectStorage=" + (objectStorage != null ? "configured" : "null") +
            ", dataBuckets=" + (dataBuckets != null ? dataBuckets.size() + " buckets" : "null") +
            ", objectTagging=" + objectTagging +
            ", logDirs=" + (logDirs != null ? logDirs.size() + " directories" : "null") +
            '}';
    }
}
