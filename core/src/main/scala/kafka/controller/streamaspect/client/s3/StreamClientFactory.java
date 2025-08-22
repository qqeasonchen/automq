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

package kafka.controller.streamaspect.client.s3;

import kafka.controller.streamaspect.client.Context;
import kafka.controller.streamaspect.client.StreamClientFactoryProxy;
import kafka.log.stream.s3.ConfigUtils;

import org.apache.kafka.controller.stream.StreamClient;

import com.automq.stream.s3.Config;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.ObjectStorageFactory;

import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_BACKGROUND;
import static com.automq.stream.s3.operator.ObjectStorageFactory.EXTENSION_TYPE_KEY;

public class StreamClientFactory {

    /**
     * This method will be called by {@link StreamClientFactoryProxy}
     */
    public static StreamClient get(Context context) {
        System.err.println("=== StreamClientFactory.get() called ===");
        
        Config streamConfig = ConfigUtils.to(context.kafkaConfig);
        
        System.err.println("=== ConfigUtils.to() completed ===");
        
        // Debug logging for quorum configuration
        System.err.println("S3 StreamClientFactory Debug:");
        System.err.println("  Quorum Enabled: " + streamConfig.quorumEnabled());
        System.err.println("  Data Buckets: " + streamConfig.dataBuckets());
        System.err.println("  Data Buckets size: " + (streamConfig.dataBuckets() != null ? streamConfig.dataBuckets().size() : "null"));
        if (streamConfig.dataBuckets() != null) {
            for (int i = 0; i < streamConfig.dataBuckets().size(); i++) {
                System.err.println("    buckets[" + i + "]: " + streamConfig.dataBuckets().get(i));
                System.err.println("    buckets[" + i + "].protocol(): " + streamConfig.dataBuckets().get(i).protocol());
            }
        }
        System.err.println("  Quorum Size: " + streamConfig.quorumSize());
        System.err.println("  Write Quorum Size: " + streamConfig.writeQuorumSize());
        System.err.println("  Read Quorum Size: " + streamConfig.readQuorumSize());
        
        try {
            System.err.println("🔧 StreamClientFactory.get() - before creating ObjectStorage");
            System.err.println("  About to call ObjectStorageFactory.instance().builder()");
            System.err.println("    with quorumEnabled: " + streamConfig.quorumEnabled());
            System.err.println("    with buckets: " + streamConfig.dataBuckets());
            System.err.println("    buckets.size(): " + (streamConfig.dataBuckets() != null ? streamConfig.dataBuckets().size() : "null"));
            
            ObjectStorage objectStorage = ObjectStorageFactory.instance().builder()
                .buckets(streamConfig.dataBuckets())
                .tagging(streamConfig.objectTagging())
                .quorumEnabled(streamConfig.quorumEnabled())
                .quorumSize(streamConfig.quorumSize())
                .writeQuorumSize(streamConfig.writeQuorumSize())
                .readQuorumSize(streamConfig.readQuorumSize())
                .extension(EXTENSION_TYPE_KEY, EXTENSION_TYPE_BACKGROUND)
                .build();
                
            System.err.println("🔧 StreamClientFactory ObjectStorage created successfully");
            System.err.println("  ObjectStorage class: " + objectStorage.getClass().getName());
            
            return StreamClient.builder()
                .streamConfig(streamConfig)
                .objectStorage(objectStorage)
                .build();
        } catch (Exception e) {
            System.err.println("Error creating ObjectStorage: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }
}
