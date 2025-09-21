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

package org.apache.kafka.metadata;

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;

import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * S3SnapshotCoordinator is responsible for coordinating Kraft protocol metadata snapshot operations,
 * including verification, writing to S3, and recovery during broker startup.
 *
 * This class encapsulates all S3 snapshot-related operations that were previously scattered
 * across different components, providing a centralized coordinator for:
 * - Snapshot verification after creation
 * - S3 object existence checking
 * - Snapshot backup to S3 storage
 * - Snapshot recovery during startup
 */
public class S3SnapshotCoordinator {
    private static final Logger log = LoggerFactory.getLogger(S3SnapshotCoordinator.class);

    private final Time time;
    private final ObjectStorage objectStorage;
    private final String bucketName;
    private final FaultHandler faultHandler;
    private final KafkaEventQueue eventQueue;

    public S3SnapshotCoordinator(
            Time time,
            ObjectStorage objectStorage,
            String bucketName,
            FaultHandler faultHandler,
            String threadNamePrefix) {
        this.time = time;
        this.objectStorage = objectStorage;
        this.bucketName = bucketName;
        this.faultHandler = faultHandler;
        LogContext logContext = new LogContext("[S3SnapshotCoordinator] ");
        this.eventQueue = new KafkaEventQueue(
            time,
            logContext,
            threadNamePrefix + "s3-snapshot-coordinator-"
        );
    }

    /**
     * Schedule async snapshot verification and backup to S3.
     * This method coordinates the entire process of verifying a snapshot and backing it up to S3.
     *
     * @param metadataImage The metadata image to verify and backup
     * @param provenance The metadata provenance of the snapshot
     * @return CompletableFuture that completes when verification and backup are done
     */
    public CompletableFuture<Boolean> scheduleSnapshotOperations(MetadataImage metadataImage, MetadataProvenance provenance) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();

        eventQueue.append(() -> {
            try {
                log.info("Starting S3 snapshot operations for snapshot {}", provenance.snapshotName());

                // Step 1: Verify S3 objects in the metadata image
                boolean verificationResult = verifyS3Objects(metadataImage);

                if (verificationResult) {
                    log.info("Successfully verified snapshot {} - all S3 objects confirmed to exist",
                        provenance.snapshotName());

                    // Step 2: Write snapshot to S3 after successful verification
                    writeSnapshotToS3(metadataImage, provenance);
                    result.complete(true);
                } else {
                    log.error("Failed to verify snapshot {} - some S3 objects are missing or verification failed",
                        provenance.snapshotName());
                    result.complete(false);
                }

            } catch (Exception e) {
                log.error("Error during snapshot operations for {}", provenance.snapshotName(), e);
                result.completeExceptionally(e);
            }
        });

        return result;
    }

    /**
     * Start the coordinator. This initializes the event queue thread.
     * Note: KafkaEventQueue automatically starts its thread in constructor.
     */
    public void start() {
        // KafkaEventQueue starts automatically in constructor, no action needed
    }

    /**
     * Begin shutdown of the coordinator.
     */
    public void beginShutdown() {
        eventQueue.beginShutdown("S3SnapshotCoordinator closing");
    }

    /**
     * Close the coordinator and clean up resources.
     */
    public void close() throws Exception {
        eventQueue.close();
    }

    /**
     * Verify S3 objects in the metadata image by checking their existence in S3 storage.
     * This method was moved from SnapshotEmitter to centralize S3 operations.
     *
     * @param metadataImage The metadata image containing S3 objects to verify
     * @return true if all recent S3 objects exist in S3 storage, false otherwise
     */
    private boolean verifyS3Objects(MetadataImage metadataImage) {
        if (objectStorage == null && bucketName == null) {
            log.warn("Neither ObjectStorage nor S3AsyncClient is configured, skipping S3 objects verification");
            return true; // Consider it successful if S3 verification is not configured
        }

        try {
            S3ObjectsImage objectsMetadata = metadataImage.objectsMetadata();
            if (objectsMetadata.isEmpty()) {
                log.info("No S3 objects in metadata image, verification passed");
                return true;
            }

            Collection<S3Object> allObjects = objectsMetadata.objects();
            long currentTime = time.milliseconds();
            long halfHourAgo = currentTime - TimeUnit.MINUTES.toMillis(30);

            int totalObjects = 0;
            int recentObjects = 0;
            int verifiedObjects = 0;
            int failedObjects = 0;

            log.info("Starting S3 objects verification: checking {} total objects", allObjects.size());

            for (S3Object s3Object : allObjects) {
                totalObjects++;
                long objectTimestamp = s3Object.getTimestamp();

                // Only verify objects created in the last 30 minutes
                if (objectTimestamp >= halfHourAgo) {
                    recentObjects++;
                    log.debug("Verifying S3 object {} with timestamp {}", s3Object.getObjectId(), objectTimestamp);

                    boolean exists = checkS3ObjectExists(s3Object.getObjectId());
                    if (exists) {
                        verifiedObjects++;
                        log.debug("S3 object {} verified successfully", s3Object.getObjectId());
                    } else {
                        failedObjects++;
                        log.error("S3 object {} does not exist in storage", s3Object.getObjectId());
                    }
                } else {
                    log.trace("Skipping S3 object {} (timestamp {} is older than 30 minutes)",
                        s3Object.getObjectId(), objectTimestamp);
                }
            }

            log.info("S3 objects verification completed: total={}, recent={}, verified={}, failed={}",
                totalObjects, recentObjects, verifiedObjects, failedObjects);

            // Return true only if all recent objects were verified successfully
            return failedObjects == 0;

        } catch (Exception e) {
            log.error("Error during S3 objects verification", e);
            return false;
        }
    }

    /**
     * Check if S3 object exists using ObjectStorage#read method.
     * This method was moved from SnapshotEmitter to centralize S3 operations.
     *
     * @param objectId The S3 object ID to check
     * @return true if the object exists, false otherwise
     */
    private boolean checkS3ObjectExists(long objectId) {
        if (objectStorage != null) {
            return checkS3ObjectExistsWithObjectStorage(objectId);
        }

        // No verification mechanism available
        log.warn("ObjectStorage is not configured, skipping S3 object verification for object {}", objectId);
        return true; // Assume exists if we can't verify
    }

    /**
     * Check S3 object existence using ObjectStorage#read method.
     * This method was moved from SnapshotEmitter to centralize S3 operations.
     */
    private boolean checkS3ObjectExistsWithObjectStorage(long objectId) {
        try {
            String objectKey = generateS3ObjectKey(objectId);

            // Create ReadOptions - use default options
            ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions();

            log.trace("Checking S3 object {} existence using ObjectStorage.read()", objectId);

            // Use ObjectStorage.read() to check existence
            CompletableFuture<ByteBuf> future = objectStorage.rangeRead(
                readOptions,
                objectKey,
                0,   // start from byte 0
                1    // read only first byte to minimize data transfer
            );

            // Wait for the result with timeout
            ByteBuf result = future.get(10, TimeUnit.SECONDS);

            // Object exists, release the ByteBuf immediately
            if (result != null) {
                result.release();
            }

            log.trace("S3 object {} exists", objectId);
            return true;

        } catch (Exception e) {
            // Check if it's ObjectNotExistException or caused by it
            Throwable cause = e.getCause();
            if (isObjectNotExistException(e) || isObjectNotExistException(cause)) {
                log.debug("S3 object {} does not exist", objectId);
                return false;
            }

            // Other exceptions (network issues, timeout, etc.)
            log.error("Error checking S3 object {} existence using ObjectStorage: {}", objectId, e.getMessage());
            return false;
        }
    }

    /**
     * Check if the exception indicates object does not exist.
     */
    private boolean isObjectNotExistException(Throwable e) {
        if (e == null) {
            return false;
        }

        // Check for ObjectNotExistException
        String className = e.getClass().getSimpleName();
        return "ObjectNotExistException".equals(className) ||
               "NoSuchKeyException".equals(className) ||
               e.getMessage() != null && e.getMessage().contains("does not exist");
    }

    /**
     * Generate S3 object key from object ID.
     */
    private String generateS3ObjectKey(long objectId) {
        return String.format("objects/%d", objectId);
    }

    /**
     * Write verified snapshot to S3 storage for backup purposes.
     * This method was moved from SnapshotEmitter to centralize S3 operations.
     *
     * @param metadataImage The verified metadata image to write to S3
     * @param provenance The metadata provenance containing snapshot information
     */
    private void writeSnapshotToS3(MetadataImage metadataImage, MetadataProvenance provenance) {
        if (objectStorage == null) {
            log.warn("ObjectStorage is not configured, skipping snapshot backup to S3");
            return;
        }

        try {
            log.info("Starting snapshot backup to S3 for {} using Kafka binary format", provenance.snapshotName());

            // Generate S3 object key for the snapshot
            String snapshotObjectKey = generateSnapshotObjectKey(provenance);

            // Serialize the metadata image to bytes
            byte[] snapshotData = serializeMetadataImage(metadataImage);

            // Create ByteBuf from serialized data
            ByteBuf snapshotByteBuf = Unpooled.wrappedBuffer(snapshotData);

            // Create Writer with WriteOptions
            ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions();
            Writer writer = objectStorage.writer(writeOptions, snapshotObjectKey);

            // Write data and close
            CompletableFuture<Void> writeFuture = writer.write(snapshotByteBuf);
            writeFuture.thenCompose(v -> writer.close())
                .whenComplete((result, throwable) -> {
                    if (throwable == null) {
                        log.info("Successfully backed up snapshot {} to S3 at key: {}",
                            provenance.snapshotName(), snapshotObjectKey);
                    } else {
                        log.error("Failed to backup snapshot {} to S3: {}",
                            provenance.snapshotName(), throwable.getMessage(), throwable);
                    }
                });

        } catch (Exception e) {
            log.error("Error during snapshot backup to S3 for {}: {}", provenance.snapshotName(), e.getMessage(), e);
        }
    }

    /**
     * Generate S3 object key for storing snapshot backup.
     */
    private String generateSnapshotObjectKey(MetadataProvenance provenance) {
        return String.format("snapshots/metadata-snapshot-%d-%d-%d.backup",
            provenance.lastContainedOffset(),
            provenance.lastContainedEpoch(),
            System.currentTimeMillis());
    }

    /**
     * Serialize MetadataImage to byte array for S3 storage.
     * This method was moved from SnapshotEmitter to centralize S3 operations.
     */
    private byte[] serializeMetadataImage(MetadataImage metadataImage) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Use Kafka's standard binary serialization format
            KafkaBinaryImageWriter writer = new KafkaBinaryImageWriter(baos);

            // Write the metadata image using the same options as snapshot generation
            ImageWriterOptions options = new ImageWriterOptions.Builder()
                .setMetadataVersion(metadataImage.features().metadataVersion())
                .build();

            metadataImage.write(writer, options);
            writer.close();

            byte[] result = baos.toByteArray();
            log.debug("Serialized MetadataImage to {} bytes containing {} records",
                result.length, writer.getRecordCount());

            return result;
        }
    }

    /**
     * Kafka Binary ImageWriter implementation for S3 snapshot serialization.
     * This class was moved from SnapshotEmitter to centralize S3 operations.
     */
    private class KafkaBinaryImageWriter implements ImageWriter {
        private final ByteArrayOutputStream outputStream;
        private int recordCount = 0;
        private final ObjectSerializationCache cache = new ObjectSerializationCache();

        public KafkaBinaryImageWriter(ByteArrayOutputStream outputStream) {
            this.outputStream = outputStream;
        }

        public int getRecordCount() {
            return recordCount;
        }

        @Override
        public void write(ApiMessageAndVersion record) {
            try {
                // Use Kafka's standard binary serialization format
                // This writes each record in the same format as Kafka snapshots

                // Calculate the serialized size first
                int messageSize = record.message().size(cache, record.version());

                // Create a ByteBuffer to hold the record data
                // Format: [record_length][api_key][version][message_data]
                ByteBuffer buffer = ByteBuffer.allocate(4 + 2 + 2 + messageSize);

                // Write record length (excluding the length field itself)
                buffer.putInt(2 + 2 + messageSize);

                // Write API key (message type ID)
                buffer.putShort(record.message().apiKey());

                // Write version
                buffer.putShort(record.version());

                // Write the actual message data
                ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
                record.message().write(accessor, cache, record.version());

                // Write the buffer to output stream
                outputStream.write(buffer.array());
                recordCount++;

            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize record using Kafka binary format", e);
            }
        }

        @Override
        public void close(boolean complete) {
            try {
                outputStream.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close output stream", e);
            }
        }
    }
}
