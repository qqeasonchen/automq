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

package kafka.log.stream.s3;

import kafka.automq.AutoMQConfig;
import kafka.server.KafkaConfig;

import com.automq.stream.s3.Config;

public class ConfigUtils {

    public static Config to(KafkaConfig s) {
        System.err.println("=== ConfigUtils.to() starting ===");
        AutoMQConfig config = s.automq();
        System.err.println("=== ConfigUtils.to() calling getQuorumEnabled ===");
        boolean quorumEnabled = getQuorumEnabled(s);
        System.err.println("=== ConfigUtils.to() quorumEnabled result: " + quorumEnabled + " ===");
        return new Config()
            .nodeId(s.nodeId())
            .dataBuckets(config.dataBuckets())
            .walConfig(config.walConfig())
            .walCacheSize(s.s3WALCacheSize())
            .walUploadThreshold(s.s3WALUploadThreshold())
            .walUploadIntervalMs(s.s3WALUploadIntervalMs())
            .streamSplitSize(s.s3StreamSplitSize())
            .objectBlockSize(s.s3ObjectBlockSize())
            .objectPartSize(s.s3ObjectPartSize())
            .blockCacheSize(s.s3BlockCacheSize())
            .streamObjectCompactionIntervalMinutes(s.s3StreamObjectCompactionTaskIntervalMinutes())
            .streamObjectCompactionMaxSizeBytes(s.s3StreamObjectCompactionMaxSizeBytes())
            .controllerRequestRetryMaxCount(s.s3ControllerRequestRetryMaxCount())
            .controllerRequestRetryBaseDelayMs(s.s3ControllerRequestRetryBaseDelayMs())
            .streamSetObjectCompactionInterval(s.s3StreamSetObjectCompactionInterval())
            .streamSetObjectCompactionCacheSize(s.s3StreamSetObjectCompactionCacheSize())
            .maxStreamNumPerStreamSetObject(s.s3MaxStreamNumPerStreamSetObject())
            .maxStreamObjectNumPerCommit(s.s3MaxStreamObjectNumPerCommit())
            .streamSetObjectCompactionStreamSplitSize(s.s3StreamSetObjectCompactionStreamSplitSize())
            .streamSetObjectCompactionForceSplitPeriod(s.s3StreamSetObjectCompactionForceSplitMinutes())
            .streamSetObjectCompactionMaxObjectNum(s.s3StreamSetObjectCompactionMaxObjectNum())
            .mockEnable(s.s3MockEnable())
            .networkBaselineBandwidth(s.s3NetworkBaselineBandwidthProp())
            .refillPeriodMs(s.s3RefillPeriodMsProp())
            .objectRetentionTimeInSecond(s.s3ObjectDeleteRetentionTimeInSecond())
            .quorumEnabled(quorumEnabled)
            .quorumSize(getQuorumSize(s))
            .writeQuorumSize(getWriteQuorumSize(s))
            .readQuorumSize(getReadQuorumSize(s))
            .writeTimeoutMs(getWriteTimeoutMs(s))
            .readTimeoutMs(getReadTimeoutMs(s))
            .readRepairEnabled(getReadRepairEnabled(s))
            .readRepairTimeoutMs(getReadRepairTimeoutMs(s));
    }

    /**
     * Helper methods to extract quorum configuration from KafkaConfig
     */
    private static boolean getQuorumEnabled(KafkaConfig config) {
        try {
            // Try to get quorum enabled from configuration using originals
            Object quorumEnabledValue = config.originals().get("s3.stream.quorum.enabled");
            System.err.println("ConfigUtils.getQuorumEnabled() DEBUG:");
            System.err.println("  s3.stream.quorum.enabled value: " + quorumEnabledValue);
            if (quorumEnabledValue != null) {
                boolean result = Boolean.parseBoolean(quorumEnabledValue.toString());
                System.err.println("  parsed result: " + result);
                return result;
            }
        } catch (Exception e) {
            System.err.println("  exception in getQuorumEnabled: " + e.getMessage());
        }
        System.err.println("  returning default: false");
        return false; // Default: quorum disabled
    }

    private static int getQuorumSize(KafkaConfig config) {
        try {
            Object quorumSizeValue = config.originals().get("s3.stream.quorum.size");
            if (quorumSizeValue != null) {
                return Integer.parseInt(quorumSizeValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return 3; // Default quorum size
    }

    private static int getWriteQuorumSize(KafkaConfig config) {
        try {
            Object writeQuorumSizeValue = config.originals().get("s3.stream.quorum.write.size");
            if (writeQuorumSizeValue != null) {
                return Integer.parseInt(writeQuorumSizeValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return 2; // Default write quorum size
    }

    private static int getReadQuorumSize(KafkaConfig config) {
        try {
            Object readQuorumSizeValue = config.originals().get("s3.stream.quorum.read.size");
            if (readQuorumSizeValue != null) {
                return Integer.parseInt(readQuorumSizeValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return 1; // Default read quorum size
    }

    private static long getWriteTimeoutMs(KafkaConfig config) {
        try {
            Object writeTimeoutValue = config.originals().get("s3.stream.quorum.write.timeout.ms");
            if (writeTimeoutValue != null) {
                return Long.parseLong(writeTimeoutValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return 15000L; // Default write timeout
    }

    private static long getReadTimeoutMs(KafkaConfig config) {
        try {
            Object readTimeoutValue = config.originals().get("s3.stream.quorum.read.timeout.ms");
            if (readTimeoutValue != null) {
                return Long.parseLong(readTimeoutValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return 5000L; // Default read timeout
    }

    private static boolean getReadRepairEnabled(KafkaConfig config) {
        try {
            Object readRepairEnabledValue = config.originals().get("s3.stream.quorum.read.repair.enabled");
            if (readRepairEnabledValue != null) {
                return Boolean.parseBoolean(readRepairEnabledValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return true; // Default: read repair enabled
    }

    private static long getReadRepairTimeoutMs(KafkaConfig config) {
        try {
            Object readRepairTimeoutValue = config.originals().get("s3.stream.quorum.read.repair.timeout.ms");
            if (readRepairTimeoutValue != null) {
                return Long.parseLong(readRepairTimeoutValue.toString());
            }
        } catch (Exception e) {
            // Ignore and fall back to default
        }
        return 2000L; // Default read repair timeout
    }
}
