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

import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Factory class for creating S3SnapshotConfig instances.
 * This class provides methods to create S3SnapshotConfig from various configuration sources,
 * while maintaining loose coupling with the core configuration modules.
 */
public class S3SnapshotConfigFactory {

    private static final Logger logger = LoggerFactory.getLogger(S3SnapshotConfigFactory.class);

    /**
     * Create S3SnapshotConfig from configuration properties and ObjectStorage instance.
     * This method allows for flexible configuration while avoiding direct dependencies
     * on core modules.
     *
     * @param properties Configuration properties containing S3 snapshot settings
     * @param objectStorage Pre-configured ObjectStorage instance, can be null
     * @return S3SnapshotConfig instance
     */
    public static S3SnapshotConfig create(Properties properties, ObjectStorage objectStorage) {
        try {
            // Read the S3 Kraft snapshot read enable configuration
            String enabledStr = properties.getProperty(S3SnapshotConfig.S3_KRAFT_SNAPSHOT_READ_ENABLE_CONFIG, "false");
            boolean s3KraftSnapshotReadEnabled = Boolean.parseBoolean(enabledStr);

            logger.info("Creating S3SnapshotConfig with s3KraftSnapshotReadEnabled={}, objectStorage={}",
                s3KraftSnapshotReadEnabled, objectStorage != null ? "configured" : "null");

            return new S3SnapshotConfig(s3KraftSnapshotReadEnabled, objectStorage);

        } catch (Exception e) {
            logger.warn("Failed to create S3SnapshotConfig from properties, using disabled configuration: {}", e.getMessage());
            return S3SnapshotConfig.disabled();
        }
    }

    /**
     * Create S3SnapshotConfig with ObjectStorage creation from parsed configuration.
     * This method creates ObjectStorage internally using the provided bucket URIs and tagging configuration.
     * This method encapsulates the ObjectStorage creation logic previously in SharedServer.
     *
     * @param s3KraftSnapshotReadEnabled Configuration properties containing S3 snapshot settings
     * @param dataBuckets Parsed list of bucket URIs from AutoMQConfig
     * @param objectTaggingMap Object tagging configuration map
     * @return S3SnapshotConfig instance with ObjectStorage created internally
     */
    public static S3SnapshotConfig createWithObjectStorage(boolean s3KraftSnapshotReadEnabled, List<BucketURI> dataBuckets, Map<String, String> objectTaggingMap) {
        try {
            ObjectStorage objectStorage = null;
            List<S3SnapshotConfig.DataBucket> configDataBuckets = null;
            boolean objectTagging = objectTaggingMap != null && !objectTaggingMap.isEmpty();

            if (s3KraftSnapshotReadEnabled && dataBuckets != null && !dataBuckets.isEmpty()) {
                try {
                    // Create ObjectStorage using ObjectStorageFactory - this is the logic moved from SharedServer
                    objectStorage = ObjectStorageFactory.createMainObjectStorage(dataBuckets,
                        objectTaggingMap != null ? objectTaggingMap : Collections.emptyMap());

                    // Convert BucketURIs to our internal DataBucket representation for configuration tracking
                    configDataBuckets = convertBucketURIsToDataBuckets(dataBuckets);

                } catch (Exception e) {
                    logger.warn("Failed to create ObjectStorage from BucketURIs, using null: {}", e.getMessage());
                }
            }

            logger.info("Creating S3SnapshotConfig with s3KraftSnapshotReadEnabled={}, dataBuckets={}, objectTagging={}, objectStorage={}",
                s3KraftSnapshotReadEnabled, configDataBuckets != null ? configDataBuckets.size() + " buckets" : "null",
                objectTagging, objectStorage != null ? "configured" : "null");

            return new S3SnapshotConfig(s3KraftSnapshotReadEnabled, objectStorage, configDataBuckets, objectTagging);

        } catch (Exception e) {
            logger.warn("Failed to create S3SnapshotConfig with ObjectStorage from configuration, using disabled configuration: {}", e.getMessage());
            return S3SnapshotConfig.disabled();
        }
    }

    /**
     * Create S3SnapshotConfig with explicit parameters.
     * This method provides direct control over the configuration parameters.
     *
     * @param s3KraftSnapshotReadEnabled Whether S3 Kraft snapshot reading is enabled
     * @param objectStorage ObjectStorage instance for S3 operations
     * @return S3SnapshotConfig instance
     */
    public static S3SnapshotConfig create(boolean s3KraftSnapshotReadEnabled, ObjectStorage objectStorage) {
        logger.info("Creating S3SnapshotConfig with explicit parameters: s3KraftSnapshotReadEnabled={}, objectStorage={}",
            s3KraftSnapshotReadEnabled, objectStorage != null ? "configured" : "null");

        return new S3SnapshotConfig(s3KraftSnapshotReadEnabled, objectStorage);
    }

    /**
     * Create a disabled S3SnapshotConfig instance.
     * This is useful for environments where S3 snapshot functionality is not needed.
     *
     * @return Disabled S3SnapshotConfig instance
     */
    public static S3SnapshotConfig createDisabled() {
        logger.info("Creating disabled S3SnapshotConfig");
        return S3SnapshotConfig.disabled();
    }

    /**
     * Convert BucketURI list to DataBucket list for configuration tracking.
     * This is a private helper method to transform external BucketURI objects
     * to our internal DataBucket representation.
     *
     * @param bucketURIs List of BucketURI objects from AutoMQConfig
     * @return List of DataBucket objects for internal use
     */
    private static List<S3SnapshotConfig.DataBucket> convertBucketURIsToDataBuckets(List<BucketURI> bucketURIs) {
        List<S3SnapshotConfig.DataBucket> dataBuckets = new ArrayList<>();
        for (BucketURI bucketURI : bucketURIs) {
            dataBuckets.add(new S3SnapshotConfig.DataBucket(bucketURI.bucket(), bucketURI.region()));
        }
        return dataBuckets;
    }
}
