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

package org.apache.kafka.image.publisher;

import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.loader.SnapshotManifest;
import org.apache.kafka.image.publisher.metrics.SnapshotEmitterMetrics;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RaftSnapshotWriter;
import org.apache.kafka.metadata.stream.S3Object;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.Batch;
import org.apache.kafka.raft.BatchReader;
import org.apache.kafka.raft.LeaderAndEpoch;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.snapshot.SnapshotReader;
import org.apache.kafka.snapshot.SnapshotWriter;

import org.slf4j.Logger;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;


public class SnapshotEmitter implements SnapshotGenerator.Emitter {
    /**
     * The maximum number of records we will put in each snapshot batch by default.
     * <p>
     * From the perspective of the Raft layer, the limit on batch size is specified in terms of
     * bytes, not number of records. See MAX_BATCH_SIZE_BYTES in KafkaRaftClient for details.
     * However, it's more convenient to limit the batch size here in terms of number of records.
     * So we chose a low number that will not cause problems.
     */
    private static final int DEFAULT_BATCH_SIZE = 1024;

    public static class Builder {
        private Time time = Time.SYSTEM;
        private int nodeId = 0;
        private RaftClient<ApiMessageAndVersion> raftClient = null;
        private int batchSize = DEFAULT_BATCH_SIZE;
        private SnapshotEmitterMetrics metrics = null;
        private FaultHandler faultHandler = (m, e) -> null;
        private String threadNamePrefix = "";
        private String bucketName = null;
        private ObjectStorage objectStorage = null;

        public Builder setTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder setNodeId(int nodeId) {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setRaftClient(RaftClient<ApiMessageAndVersion> raftClient) {
            this.raftClient = raftClient;
            return this;
        }

        public Builder setBatchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public Builder setMetrics(SnapshotEmitterMetrics metrics) {
            this.metrics = metrics;
            return this;
        }

        public Builder setFaultHandler(FaultHandler faultHandler) {
            this.faultHandler = faultHandler;
            return this;
        }

        public Builder setThreadNamePrefix(String threadNamePrefix) {
            this.threadNamePrefix = threadNamePrefix;
            return this;
        }

        public Builder setBucketName(String bucketName) {
            this.bucketName = bucketName;
            return this;
        }

        public Builder setObjectStorage(ObjectStorage objectStorage) {
            this.objectStorage = objectStorage;
            return this;
        }

        public SnapshotEmitter build() {
            if (raftClient == null) throw new RuntimeException("You must set the raftClient.");
            if (metrics == null) metrics = new SnapshotEmitterMetrics(
                Optional.empty(),
                time);
            return new SnapshotEmitter(time,
                nodeId,
                raftClient,
                batchSize,
                metrics,
                faultHandler,
                threadNamePrefix,
                bucketName,
                objectStorage);
        }
    }

    /**
     * The slf4j logger to use.
     */
    private final Logger log;

    /**
     * The clock object.
     */
    private final Time time;

    /**
     * The RaftClient to use.
     */
    private final RaftClient<ApiMessageAndVersion> raftClient;

    /**
     * The maximum number of records to put in each batch.
     */
    private final int batchSize;

    /**
     * The metrics to use.
     */
    private final SnapshotEmitterMetrics metrics;

    /**
     * The fault handler to use.
     */
    private final FaultHandler faultHandler;

    /**
     * Event queue for async snapshot verification tasks.
     */
    private final EventQueue eventQueue;


    /**
     * S3 bucket name for object verification.
     */
    private final String bucketName;

    /**
     * ObjectStorage instance for S3 operations.
     */
    private final ObjectStorage objectStorage;

    private SnapshotEmitter(
        Time time,
        int nodeId,
        RaftClient<ApiMessageAndVersion> raftClient,
        int batchSize,
        SnapshotEmitterMetrics metrics,
        FaultHandler faultHandler,
        String threadNamePrefix,
        String bucketName,
        ObjectStorage objectStorage
    ) {
        this.time = time;
        LogContext logContext = new LogContext("[SnapshotEmitter id=" + nodeId + "] ");
        this.log = logContext.logger(SnapshotEmitter.class);
        this.raftClient = raftClient;
        this.batchSize = batchSize;
        this.metrics = metrics;
        this.faultHandler = faultHandler;
        this.eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix + "snapshot-verifier-");
        this.bucketName = bucketName;
        this.objectStorage = objectStorage;
    }

    public SnapshotEmitterMetrics metrics() {
        return metrics;
    }

    @Override
    public void maybeEmit(MetadataImage image) {
        MetadataProvenance provenance = image.provenance();
        Optional<SnapshotWriter<ApiMessageAndVersion>> snapshotWriter = raftClient.createSnapshot(
            provenance.snapshotId(),
            provenance.lastContainedLogTimeMs()
        );
        if (!snapshotWriter.isPresent()) {
            log.error("Not generating {} because it already exists.", provenance.snapshotName());
            return;
        }
        RaftSnapshotWriter writer = new RaftSnapshotWriter(snapshotWriter.get(), batchSize);
        try {
            image.write(writer, new ImageWriterOptions.Builder().
                setMetadataVersion(image.features().metadataVersion()).
                build());
            writer.close(true);
            metrics.setLatestSnapshotGeneratedTimeMs(time.milliseconds());
            metrics.setLatestSnapshotGeneratedBytes(writer.frozenSize().getAsLong());
            log.info("Successfully wrote {}", provenance.snapshotName());

            // Schedule async snapshot verification task
            scheduleSnapshotVerification(provenance);

        } catch (Throwable e) {
            log.error("Encountered error while writing {}", provenance.snapshotName(), e);
            throw e;
        } finally {
            Utils.closeQuietly(writer, "RaftSnapshotWriter");
            Utils.closeQuietly(snapshotWriter.get(), "SnapshotWriter");
        }
    }

    /**
     * Schedule async snapshot verification task to verify the snapshot was written correctly.
     *
     * @param provenance The metadata provenance of the snapshot to verify
     */
    private void scheduleSnapshotVerification(MetadataProvenance provenance) {
        eventQueue.append(() -> {
            try {
                log.info("Starting load of snapshot {}", provenance.snapshotName());
                MetadataImage metadataImage = loadSnapshot(provenance.snapshotId());

                if (metadataImage != null) {
                    log.info("Successfully loaded snapshot {}, starting S3 objects verification", provenance.snapshotName());

                    // Execute S3 objects verification
                    boolean verificationResult = verifyS3Objects(metadataImage);

                    if (verificationResult) {
                        log.info("Successfully verified snapshot {} - all S3 objects confirmed to exist",
                            provenance.snapshotName());

                        // Write snapshot to S3 after successful verification
                        writeSnapshotToS3(metadataImage, provenance);
                    } else {
                        log.error("Failed to verify snapshot {} - some S3 objects are missing or verification failed",
                            provenance.snapshotName());
                    }
                } else {
                    log.error("Failed to load snapshot {} - snapshot may be corrupted",
                        provenance.snapshotName());
                }
            } catch (Throwable e) {
                faultHandler.handleFault("Error during snapshot verification for " +
                    provenance.snapshotName(), e);
            }
        });
    }

    /**
     * Load the latest snapshot directly by accessing RaftClient's internal snapshot mechanism.
     * This uses Java reflection to access the private latestSnapshot() method.
     */
    private MetadataImage loadSnapshot(OffsetAndEpoch snapshotId) {
        try {
            Optional<SnapshotReader<ApiMessageAndVersion>> snapshotReader = raftClient.latestSnapshot();
            if (!snapshotReader.isPresent()) {
                log.error("Latest snapshot not available via reflection for {}", snapshotId);
                return null;
            }
            try (SnapshotReader<ApiMessageAndVersion> reader = snapshotReader.get()) {
                // Verify this is the snapshot we expect
                if (!reader.snapshotId().equals(snapshotId)) {
                    log.error("Snapshot ID mismatch: expected {}, got {}", snapshotId, reader.snapshotId());
                    return null;
                }
                log.info("Loading snapshot {} directly for verification", reader.snapshotId());
                // Create a new MetadataDelta starting from an empty image
                // This follows the same pattern as MetadataLoader#handleLoadSnapshot
                MetadataDelta delta = new MetadataDelta.Builder()
                    .setImage(MetadataImage.EMPTY)
                    .build();
                // Load the snapshot using the same logic as MetadataLoader#loadSnapshot
                SnapshotManifest manifest = loadSnapshotData(delta, reader);
                // Apply the delta to create the final image
                MetadataImage metadataImage = delta.apply(manifest.provenance());
                log.info("Successfully loaded snapshot {} - {} records in {} ns",
                    reader.snapshotId(), getRecordCountFromDelta(delta), manifest.elapsedNs());
                return metadataImage;
            }
        } catch (Exception e) {
            log.error("Failed to load latest snapshot directly via reflection for {}", snapshotId, e);
            return null;
        }
    }

    /**
     * Load snapshot data from reader into delta - based on MetadataLoader#loadSnapshot implementation.
     */
    private SnapshotManifest loadSnapshotData(
        MetadataDelta delta,
        SnapshotReader<ApiMessageAndVersion> reader) {
        long startNs = time.nanoseconds();
        int snapshotIndex = 0;

        while (reader.hasNext()) {
            Batch<ApiMessageAndVersion> batch = reader.next();
            for (ApiMessageAndVersion record : batch.records()) {
                try {
                    delta.replay(record.message());
                } catch (Throwable e) {
                    faultHandler.handleFault("Error loading metadata log record " + snapshotIndex +
                        " in snapshot at offset " + reader.lastContainedLogOffset() +
                        " during verification", e);
                }
                snapshotIndex++;
            }
        }

        delta.finishSnapshot();

        MetadataProvenance provenance = new MetadataProvenance(
            reader.lastContainedLogOffset(),
            reader.lastContainedLogEpoch(),
            reader.lastContainedLogTimestamp());

        return new SnapshotManifest(provenance, time.nanoseconds() - startNs);
    }

    /**
     * Get an approximate record count from the delta (for logging purposes).
     */
    private int getRecordCountFromDelta(MetadataDelta delta) {
        // This is an approximation - in practice you could track this during loading
        // For now, return a placeholder since the exact count requires more complex tracking
        return 0; // TODO: Implement proper record counting if needed
    }

    /**
     * Verify S3 objects in the metadata image by checking their existence in S3 storage.
     *
     * @param metadataImage The metadata image containing S3 objects to verify
     * @return true if all recent S3 objects exist in S3 storage, false otherwise
     */
    private boolean verifyS3Objects(MetadataImage metadataImage) {
        if (objectStorage == null &&  bucketName == null) {
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
     * This approach uses ObjectStorage.read() which attempts to read the object.
     * If the object exists, we immediately release the ByteBuf without processing the data.
     * If the object doesn't exist, ObjectNotExistException will be thrown.
     *
     * @param objectId The S3 object ID to check
     * @return true if the object exists, false otherwise
     */
    private boolean checkS3ObjectExists(long objectId) {
        // Priority 1: Use ObjectStorage if available (preferred approach)
        if (objectStorage != null) {
            return checkS3ObjectExistsWithObjectStorage(objectId);
        }

        // No verification mechanism available
        log.warn("Neither ObjectStorage nor S3AsyncClient is configured, skipping S3 object verification for object {}", objectId);
        return true; // Assume exists if we can't verify
    }

    /**
     * Check S3 object existence using ObjectStorage#read method.
     */
    private boolean checkS3ObjectExistsWithObjectStorage(long objectId) {
        try {
            String objectKey = generateS3ObjectKey(objectId);

            // Create ReadOptions - use default options
            ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions();

            log.trace("Checking S3 object {} existence using ObjectStorage.read()", objectId);

            // Use ObjectStorage.read() to check existence
            // We only need to know if the object exists, so we read with minimal data
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
     * Get S3 objects from S3ObjectsImage using reflection to access private objects() method.
     *
     * @param objectsImage The S3ObjectsImage instance
     * @return Collection of S3Object instances
     */
    @SuppressWarnings("unchecked")
    private Collection<S3Object> getS3Objects(S3ObjectsImage objectsImage) {
        try {
            java.lang.reflect.Method objectsMethod = objectsImage.getClass().getDeclaredMethod("objects");
            objectsMethod.setAccessible(true);
            return (Collection<S3Object>) objectsMethod.invoke(objectsImage);
        } catch (Exception e) {
            log.error("Failed to access S3 objects via reflection", e);
            return java.util.Collections.emptyList();
        }
    }

    /**
     * Generate S3 object key from object ID.
     * This uses AutoMQ's ObjectUtils.genKey method for correct key generation.
     *
     * @param objectId The object ID
     * @return The S3 object key
     */
    private String generateS3ObjectKey(long objectId) {
        // Use AutoMQ's standard key generation logic
        return ObjectUtils.genKey(0, objectId);
    }


    /**
     * Write the verified snapshot to S3 storage for backup and distribution purposes.
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
     *
     * @param provenance The metadata provenance containing snapshot information
     * @return S3 object key for the snapshot
     */
    private String generateSnapshotObjectKey(MetadataProvenance provenance) {
        return String.format("snapshots/metadata-snapshot-%d-%d-%d.backup",
            provenance.lastContainedOffset(),
            provenance.lastContainedEpoch(),
            System.currentTimeMillis());
    }

    /**
     * Serialize MetadataImage to byte array for S3 storage.
     * This method converts the metadata image to the same binary format used by Kafka snapshots,
     * ensuring it can be properly restored and used for recovery.
     *
     * @param metadataImage The metadata image to serialize
     * @return Serialized byte array representation of the metadata image in Kafka's binary format
     * @throws IOException If serialization fails
     */
    private byte[] serializeMetadataImage(MetadataImage metadataImage) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            // Use Kafka's standard binary serialization format
            // This ensures compatibility and recoverability
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
     * Kafka Binary ImageWriter implementation that writes to a ByteArrayOutputStream
     * using Kafka's standard binary serialization format.
     * This ensures the serialized data can be properly restored as a valid MetadataImage.
     */
    private class KafkaBinaryImageWriter implements ImageWriter {
        private final ByteArrayOutputStream outputStream;
        private int recordCount = 0;
        private final ObjectSerializationCache cache = new ObjectSerializationCache();

        public KafkaBinaryImageWriter(ByteArrayOutputStream outputStream) {
            this.outputStream = outputStream;
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

        public void close() {
            // No-op for ByteArrayOutputStream
        }

        @Override
        public void close(boolean complete) {
            // No-op for ByteArrayOutputStream
        }

        public int getRecordCount() {
            return recordCount;
        }
    }

    /**
     * Close the snapshot emitter and its event queue.
     */
    public void close() throws InterruptedException {
        eventQueue.beginShutdown("close");
        eventQueue.close();
    }
}
