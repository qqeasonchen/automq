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
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.UnalignedMemoryRecords;
import org.apache.kafka.common.record.UnalignedRecords;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.S3ObjectsImage;
import org.apache.kafka.image.writer.ImageWriter;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.metadata.stream.S3Object;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.OffsetAndEpoch;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.common.S3SnapshotConfig;
import org.apache.kafka.server.common.serialization.RecordSerde;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.snapshot.RawSnapshotReader;
import org.apache.kafka.snapshot.Snapshots;

import com.automq.stream.s3.metadata.ObjectUtils;
import com.automq.stream.s3.operator.ObjectStorage;
import com.automq.stream.s3.operator.Writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
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
public class SnapshotController<T> {
    private static final Logger log = LoggerFactory.getLogger(SnapshotController.class);

    private final Time time;
    private final ObjectStorage objectStorage;
    private final String bucketName;
    private final FaultHandler faultHandler;
    private final KafkaEventQueue eventQueue;
    private final RecordSerde<T> serde;

    // Configuration for S3 Kraft snapshot reading
    private static volatile S3SnapshotConfig s3SnapshotConfig = S3SnapshotConfig.disabled();

    /**
     * Set the S3 snapshot configuration.
     * This should be called during server startup.
     */
    public static void setS3SnapshotConfig(S3SnapshotConfig config) {
        s3SnapshotConfig = config != null ? config : S3SnapshotConfig.disabled();
    }

    /**
     * Get the current S3 snapshot configuration.
     */
    public static S3SnapshotConfig getS3SnapshotConfig() {
        return s3SnapshotConfig;
    }

    public SnapshotController(
            Time time,
            ObjectStorage objectStorage,
            String bucketName,
            FaultHandler faultHandler,
            String threadNamePrefix,
            RecordSerde<T> serde) {
        this.time = time;
        this.objectStorage = objectStorage;
        this.bucketName = bucketName;
        this.faultHandler = faultHandler;
        this.serde = serde;
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
            //Backup KRaft directory to S3 after successful snapshot
            try {
                if (getS3SnapshotConfig() != null && getS3SnapshotConfig().isS3KraftSnapshotWriteEnabled()) {
                    String kraftLogDir = getKraftLogDirectory();
                    if (kraftLogDir != null) {
                        log.info("Starting KRaft directory backup to S3 for directory: {}", kraftLogDir);
                        boolean backupSuccess = backupKRaftDirectoryToS3(kraftLogDir);
                        if (backupSuccess) {
                            log.info("Successfully backed up KRaft directory to S3: {}", kraftLogDir);
                        } else {
                            log.warn("Failed to backup KRaft directory to S3: {}", kraftLogDir);
                        }
                    } else {
                        log.warn("KRaft log directory not configured, skipping directory backup");
                    }
                } else {
                    log.debug("S3 KRaft snapshot write is disabled, skipping directory backup");
                }
            } catch (Exception e) {
                log.error("Error during KRaft directory backup", e);
            }
            result.complete(true);
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
            // Generate correct S3 object key using ObjectUtils
            String objectKey = ObjectUtils.genKey(0, objectId);

            // Create ReadOptions with correct bucket ID (bucket 0 for data buckets)
            ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions().bucket((short) 0);

            log.trace("Checking S3 object {} existence using ObjectStorage.read() with key: {}", objectId, objectKey);

            // Use ObjectStorage.read() to check existence
            CompletableFuture<ByteBuf> future = objectStorage.rangeRead(
                readOptions,
                objectKey,
                0,   // start from byte 0
                1    // read only first byte to minimize data transfer
            );

            // Wait for the result with timeout
            ByteBuf result = future.get(10, TimeUnit.SECONDS);

            // Use try-with-resources pattern for automatic cleanup
            try {
                if (result != null) {
                    // Object exists, ByteBuf will be automatically released
                    log.trace("S3 object {} exists at key: {}", objectId, objectKey);
                    return true;
                } else {
                    log.trace("S3 object {} returned null ByteBuf", objectId);
                    return false;
                }
            } finally {
                // Ensure ByteBuf is always released
                if (result != null && result.refCnt() > 0) {
                    try {
                        result.release();
                        log.trace("Released ByteBuf for S3 object existence check: {}", objectId);
                    } catch (Exception releaseException) {
                        log.warn("Error releasing ByteBuf for object {} existence check: {}",
                            objectId, releaseException.getMessage());
                    }
                }
            }

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
     * Generate S3 object key from object ID using ObjectUtils.
     * @deprecated Use ObjectUtils.genKey(0, objectId) directly in the calling method
     */
    @Deprecated
    private String generateS3ObjectKey(long objectId) {
        return ObjectUtils.genKey(0, objectId);
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

        ByteBuf snapshotByteBuf = null;
        try {
            log.info("Starting snapshot backup to S3 for {} using Kafka binary format", provenance.snapshotName());

            // Generate S3 object key for the snapshot
            String snapshotObjectKey = Snapshots.generateSnapshotObjectKey(provenance.snapshotId());

            log.info("Starting snapshot backup to S3 for snapshotObjectKey {} using Kafka binary format", snapshotObjectKey);
            // Serialize the metadata image to bytes
            byte[] snapshotData = serializeMetadataImage(metadataImage);

            // Create ByteBuf from serialized data
            snapshotByteBuf = Unpooled.wrappedBuffer(snapshotData);

            // Keep a reference for proper cleanup in async callback
            final ByteBuf bufferToRelease = snapshotByteBuf;

            // Validate ByteBuf before writing
            if (snapshotByteBuf.readableBytes() == 0) {
                log.error("ByteBuf has no readable bytes! Cannot write empty data to S3");
                throw new IllegalStateException("ByteBuf is empty");
            }

            long dataSize = snapshotByteBuf.readableBytes();
            log.info("About to write ByteBuf to S3 - readable bytes: {}, refCnt: {}",
                dataSize, snapshotByteBuf.refCnt());

            // Create WriteOptions
            ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions();

            // Use different upload strategy based on file size
            CompletableFuture<Void> uploadFuture;
            Writer writer = null;

            if (dataSize < Writer.MIN_PART_SIZE) {
                // For small files (< 5MB), use direct ObjectStorage.write() instead of MultiPartWriter
                log.info("Using direct S3 PUT for small file ({} bytes) - more efficient than multipart", dataSize);

                uploadFuture = objectStorage.write(writeOptions, snapshotObjectKey, snapshotByteBuf)
                    .thenApply(writeResult -> {
                        log.info("Successfully uploaded small snapshot {} to S3 using direct PUT", provenance.snapshotName());
                        return null;
                    });

            } else {
                // For large files (>= 5MB), use MultiPartWriter
                log.info("Using MultiPartWriter for large file ({} bytes)", dataSize);
                log.info("Creating writer for object key: {} with ObjectStorage: {}",
                    snapshotObjectKey, objectStorage.getClass().getSimpleName());

                writer = objectStorage.writer(writeOptions, snapshotObjectKey);
                log.info("Successfully created writer: {}", writer.getClass().getSimpleName());

                // Keep reference for cleanup
                final Writer writerToRelease = writer;

                uploadFuture = writer.write(snapshotByteBuf)
                    .thenCompose(v -> {
                        log.info("Data written to writer, now closing to force upload...");
                        return writerToRelease.close();
                    })
                    .whenComplete((result, throwable) -> {
                        // Always release writer resources
                        try {
                            writerToRelease.release().get(10, TimeUnit.SECONDS);
                            log.trace("Successfully released writer resources for snapshot: {}", provenance.snapshotName());
                        } catch (Exception releaseException) {
                            log.warn("Error releasing writer resources for snapshot {}: {}",
                                provenance.snapshotName(), releaseException.getMessage());
                        }
                    });
            }

            // Wait for upload to complete with timeout and handle Writer cleanup on exception
            final Writer finalWriter = writer; // For exception cleanup
            try {
                uploadFuture.get(30, TimeUnit.SECONDS);
                log.info("Successfully completed S3 upload for snapshot: {}", provenance.snapshotName());
            } catch (Exception uploadException) {
                log.error("S3 upload failed for snapshot {}: {}",
                    provenance.snapshotName(), uploadException.getMessage(), uploadException);

                // Release writer resources if upload failed and writer was created
                if (finalWriter != null) {
                    try {
                        finalWriter.release().get(10, TimeUnit.SECONDS);
                        log.trace("Released writer resources after upload failure for snapshot: {}", provenance.snapshotName());
                    } catch (Exception releaseException) {
                        log.warn("Error releasing writer resources after upload failure for snapshot {}: {}",
                            provenance.snapshotName(), releaseException.getMessage());
                    }
                }

                throw new RuntimeException("Failed to upload snapshot to S3", uploadException);
            }

            // Create a dummy future for cleanup
            CompletableFuture<Void> cleanupFuture = CompletableFuture.completedFuture(null);
            cleanupFuture.whenComplete((result, throwable) -> {
                    // Always release ByteBuf regardless of success or failure
                    try {
                        if (bufferToRelease.refCnt() > 0) {
                            bufferToRelease.release();
                            log.trace("Released ByteBuf for snapshot backup: {}", provenance.snapshotName());
                        }
                    } catch (Exception releaseException) {
                        log.warn("Error releasing ByteBuf for snapshot {}: {}",
                            provenance.snapshotName(), releaseException.getMessage());
                    }

                    // Log operation result
                    if (throwable == null) {
                        log.info("Successfully backed up snapshot {} to S3 at key: {}",
                            provenance.snapshotName(), snapshotObjectKey);
                    } else {
                        log.error("Failed to backup snapshot {} to S3: {}",
                            provenance.snapshotName(), throwable.getMessage(), throwable);
                    }
                });

        } catch (Exception e) {
            // Release ByteBuf in case of exception before async operation starts
            if (snapshotByteBuf != null && snapshotByteBuf.refCnt() > 0) {
                try {
                    snapshotByteBuf.release();
                    log.trace("Released ByteBuf after exception during snapshot backup setup: {}", provenance.snapshotName());
                } catch (Exception releaseException) {
                    log.warn("Error releasing ByteBuf after exception for snapshot {}: {}",
                        provenance.snapshotName(), releaseException.getMessage());
                }
            }
            log.error("Error during snapshot backup to S3 for {}: {}", provenance.snapshotName(), e.getMessage(), e);
        }
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
    private static class KafkaBinaryImageWriter implements ImageWriter {
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



    /**
     * Load snapshot from S3 storage using the snapshot name with '.backup' suffix.
     */
    public static Optional<RawSnapshotReader> loadSnapshotFromS3() {
        try {

            OffsetAndEpoch snapshotId = new OffsetAndEpoch(30861, 27);
            String snapshotObjectKey = Snapshots.generateSnapshotObjectKey(snapshotId);

            // Use the configured ObjectStorage for reading from S3
            ObjectStorage objectStorage = s3SnapshotConfig.getObjectStorage();
            if (objectStorage == null) {
                log.warn("ObjectStorage is not configured for S3 snapshot reading");
                return Optional.empty();
            }

            // Read snapshot data from S3
            byte[] snapshotData = readSnapshotDataFromS3(objectStorage, snapshotObjectKey);
            if (snapshotData == null) {
                log.warn("Failed to read snapshot data from S3 for key: {}", snapshotObjectKey);
                return Optional.empty();
            }

            // Convert byte array to RawSnapshotReader
            RawSnapshotReader reader = createRawSnapshotReaderFromBytes(snapshotData, snapshotId);
            if (reader != null) {
                log.info("Successfully created RawSnapshotReader from S3 data for snapshot: {}", snapshotId);
                return Optional.of(reader);
            } else {
                log.error("Failed to create RawSnapshotReader from S3 data");
                return Optional.empty();
            }

        } catch (Exception e) {
            log.error("Error loading snapshot from S3: {}", e.getMessage(), e);
            return Optional.empty();
        }
    }


    /**
     * Read snapshot data from S3 using ObjectStorage.
     */
    private static byte[] readSnapshotDataFromS3(ObjectStorage objectStorage, String objectKey) {
        try {
            // Create ReadOptions with correct bucket ID (bucket 0 for data buckets)
            ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions().bucket((short) 0);

            CompletableFuture<ByteBuf> future = objectStorage.read(
                readOptions,
                objectKey
            );

            ByteBuf byteBuf = future.get(30, TimeUnit.SECONDS);
            if (byteBuf == null) {
                return null;
            }

            try {
                byte[] data = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(data);
                return data;
            } finally {
                byteBuf.release();
            }

        } catch (Exception e) {
            log.error("Failed to read snapshot data from S3 object {}: {}", objectKey, e.getMessage(), e);
            return null;
        }
    }

    /**
     * Create RawSnapshotReader from serialized byte array data read from S3.
     * This reverses the serialization process done by SnapshotEmitter.
     */
    private static RawSnapshotReader createRawSnapshotReaderFromBytes(byte[] snapshotData, OffsetAndEpoch snapshotId) {
        try {
            // Create a ByteArrayInputStream from the data
            ByteArrayInputStream bais = new ByteArrayInputStream(snapshotData);

            // Create a custom RawSnapshotReader that reads from the byte array
            return new S3ByteArrayRawSnapshotReader(bais, snapshotId, snapshotData.length);

        } catch (Exception e) {
            log.error("Failed to create SnapshotReader from byte array: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * Custom RawSnapshotReader implementation that reads snapshot data from a byte array
     * loaded from S3 storage.
     */
    private static class S3ByteArrayRawSnapshotReader implements RawSnapshotReader, AutoCloseable {
        private final ByteArrayInputStream inputStream;
        private final OffsetAndEpoch snapshotId;
        private final long sizeInBytes;

        public S3ByteArrayRawSnapshotReader(ByteArrayInputStream inputStream, OffsetAndEpoch snapshotId, long sizeInBytes) {
            this.inputStream = inputStream;
            this.snapshotId = snapshotId;
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public OffsetAndEpoch snapshotId() {
            return snapshotId;
        }

        @Override
        public long sizeInBytes() {
            return sizeInBytes;
        }

        @Override
        public UnalignedRecords slice(long position, int size) {
            // For simplicity, return the entire snapshot data as UnalignedRecords
            // In a full implementation, you'd need to handle position and size properly
            byte[] data = inputStream.readAllBytes();
            inputStream.reset();
            return new UnalignedMemoryRecords(ByteBuffer.wrap(data));
        }

        @Override
        public Records records() {
            byte[] data = inputStream.readAllBytes();
            inputStream.reset();
            return MemoryRecords.readableRecords(ByteBuffer.wrap(data));
        }

        @Override
        public void close() {
            // Close the input stream
            try {
                inputStream.close();
            } catch (Exception e) {
                // Ignore close errors
            }
        }
    }

    /**
     * ======================= KRaft 目录级别备份和恢复功能 =======================
     */

    /**
     * 将整个KRaft元数据目录打包并上传到S3
     * @param kraftLogDir KRaft日志目录路径 (例如: D:\workspace_java\qqeasonchen\automq2\kraft-combined-logs-idc-b)
     * @return 上传是否成功
     */
    public static boolean backupKRaftDirectoryToS3(String kraftLogDir) {
        try {
            log.info("Starting KRaft directory backup to S3: {}", kraftLogDir);

            // 检查S3配置
            if (s3SnapshotConfig == null || !s3SnapshotConfig.isS3KraftSnapshotWriteEnabled()) {
                log.warn("S3 KRaft snapshot is not enabled, skipping directory backup");
                return false;
            }

            ObjectStorage objectStorage = s3SnapshotConfig.getObjectStorage();
            if (objectStorage == null) {
                log.warn("ObjectStorage is not configured for S3 directory backup");
                return false;
            }
            // 创建目录打包（使用安全模式，跳过锁定文件）
            byte[] zipData = createKRaftDirectoryZipSafely(kraftLogDir);
            if (zipData == null || zipData.length == 0) {
                log.error("Failed to create KRaft directory zip package");
                return false;
            }
            log.info("Created KRaft directory zip package: {} bytes", zipData.length);
            // 生成S3对象key
            String s3ObjectKey = generateKRaftDirectoryBackupKeyByNowTime();
            // 上传到S3
            return uploadZipToS3(objectStorage, s3ObjectKey, zipData);

        } catch (Exception e) {
            log.error("Error during KRaft directory backup to S3: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 从S3恢复整个KRaft元数据目录
     * @param kraftLogDir 目标KRaft日志目录路径
     * @return 恢复是否成功
     */
    public static boolean restoreKRaftDirectoryFromS3(String kraftLogDir) {
        try {
            log.info("Starting KRaft directory restore from S3 to: {}", kraftLogDir);

            // 检查S3配置
            if (s3SnapshotConfig == null || !s3SnapshotConfig.isS3KraftSnapshotReadEnabled()) {
                log.warn("S3 KRaft snapshot is not enabled, skipping directory restore");
                return false;
            }

            ObjectStorage objectStorage = s3SnapshotConfig.getObjectStorage();
            if (objectStorage == null) {
                log.warn("ObjectStorage is not configured for S3 directory restore");
                return false;
            }

            // 获取恢复时间戳
            String restoreTimestamp = s3SnapshotConfig.getRestoreTimestamp();
            if (restoreTimestamp == null || restoreTimestamp.trim().isEmpty()) {
                log.warn("Restore timestamp is not configured, skipping directory restore. Please set s3.kraft.snapshot.restore.timestamp in configuration");
                return false;
            }

            log.info("Attempting to restore KRaft directory from S3 backup with timestamp: {}", restoreTimestamp);

            // 生成S3对象key
            String s3ObjectKey = generateKRaftDirectoryBackupKeyByTime(restoreTimestamp);

            // 从S3下载zip数据
            byte[] zipData = downloadZipFromS3(objectStorage, s3ObjectKey);
            if (zipData == null || zipData.length == 0) {
                log.error("Failed to download KRaft directory backup from S3");
                return false;
            }

            log.info("Downloaded KRaft directory backup from S3: {} bytes", zipData.length);

            // 恢复目录
            return restoreKRaftDirectoryFromZip(kraftLogDir, zipData);

        } catch (Exception e) {
            log.error("Error during KRaft directory restore from S3: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 创建KRaft目录的zip包（安全模式，跳过锁定文件）
     */
    private static byte[] createKRaftDirectoryZipSafely(String kraftLogDirPath) {
        java.io.ByteArrayOutputStream baos = null;
        java.util.zip.ZipOutputStream zos = null;
        int totalFiles = 0;
        int skippedFiles = 0;

        try {
            java.io.File kraftLogDir = new java.io.File(kraftLogDirPath);
            if (!kraftLogDir.exists() || !kraftLogDir.isDirectory()) {
                log.error("KRaft directory does not exist: {}", kraftLogDirPath);
                return null;
            }

            baos = new java.io.ByteArrayOutputStream();
            zos = new java.util.zip.ZipOutputStream(baos);

            // 使用安全的文件添加方法
            int[] counters = addDirectoryToZipSafely(kraftLogDir, "", zos);
            totalFiles = counters[0];
            skippedFiles = counters[1];

            zos.finish();
            byte[] zipData = baos.toByteArray();

            log.info("Created KRaft directory zip: {} bytes, {} files included, {} files skipped due to locks",
                    zipData.length, totalFiles, skippedFiles);

            return zipData;

        } catch (Exception e) {
            log.error("Failed to create KRaft directory zip: {}", e.getMessage(), e);
            return null;
        } finally {
            if (zos != null) {
                try {
                    zos.close();
                } catch (Exception e) {
                    log.warn("Error closing zip output stream: {}", e.getMessage());
                }
            }
            if (baos != null) {
                try {
                    baos.close();
                } catch (Exception e) {
                    log.warn("Error closing byte array output stream: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 创建KRaft目录的zip包（原版本，保留向后兼容）
     */
    private static byte[] createKRaftDirectoryZip(String kraftLogDirPath) {
        java.io.ByteArrayOutputStream baos = null;
        java.util.zip.ZipOutputStream zos = null;

        try {
            java.io.File kraftLogDir = new java.io.File(kraftLogDirPath);
            if (!kraftLogDir.exists() || !kraftLogDir.isDirectory()) {
                log.error("KRaft log directory does not exist or is not a directory: {}", kraftLogDirPath);
                return null;
            }

            baos = new java.io.ByteArrayOutputStream();
            zos = new java.util.zip.ZipOutputStream(baos);

            // 递归添加目录中的所有文件
            addDirectoryToZip(kraftLogDir, "", zos);

            zos.finish();
            byte[] zipData = baos.toByteArray();

            log.info("Successfully created KRaft directory zip: {} files, {} bytes",
                    countFilesInDirectory(kraftLogDir), zipData.length);

            return zipData;

        } catch (Exception e) {
            log.error("Failed to create KRaft directory zip: {}", e.getMessage(), e);
            return null;
        } finally {
            try {
                if (zos != null) zos.close();
                if (baos != null) baos.close();
            } catch (Exception e) {
                log.warn("Error closing streams: {}", e.getMessage());
            }
        }
    }

    /**
     * 递归添加目录到zip文件（安全版本，返回计数）
     * @return int[] {totalFiles, skippedFiles}
     */
    private static int[] addDirectoryToZipSafely(java.io.File sourceDir, String basePath, java.util.zip.ZipOutputStream zos) {
        int totalFiles = 0;
        int skippedFiles = 0;

        java.io.File[] files = sourceDir.listFiles();
        if (files == null) {
            return new int[]{0, 0};
        }

        for (java.io.File file : files) {
            String entryName = basePath.isEmpty() ? file.getName() : basePath + "/" + file.getName();

            if (file.isDirectory()) {
                try {
                    // 添加目录条目
                    zos.putNextEntry(new java.util.zip.ZipEntry(entryName + "/"));
                    zos.closeEntry();

                    // 递归处理子目录
                    int[] subCounters = addDirectoryToZipSafely(file, entryName, zos);
                    totalFiles += subCounters[0];
                    skippedFiles += subCounters[1];
                } catch (java.io.IOException e) {
                    log.warn("Failed to add directory entry: {} - {}", entryName, e.getMessage());
                    skippedFiles++;
                }
            } else {
                // 添加文件，使用安全的方法
                if (addFileToZipSafely(file, entryName, zos)) {
                    totalFiles++;
                } else {
                    skippedFiles++;
                }
            }
        }

        return new int[]{totalFiles, skippedFiles};
    }

    /**
     * 递归添加目录到zip（原版本）
     */
    private static void addDirectoryToZip(java.io.File sourceDir, String basePath, java.util.zip.ZipOutputStream zos)
            throws java.io.IOException {
        java.io.File[] files = sourceDir.listFiles();
        if (files == null) return;

        for (java.io.File file : files) {
            String entryName = basePath.isEmpty() ? file.getName() : basePath + "/" + file.getName();

            if (file.isDirectory()) {
                // 添加目录条目
                zos.putNextEntry(new java.util.zip.ZipEntry(entryName + "/"));
                zos.closeEntry();

                // 递归添加子目录
                addDirectoryToZip(file, entryName, zos);
            } else {
                // 添加文件，使用安全的文件读取方法
                if (!addFileToZipSafely(file, entryName, zos)) {
                    log.warn("Skipped file due to access restriction: {}", entryName);
                } else {
                    log.debug("Added file to zip: {} ({} bytes)", entryName, file.length());
                }
            }
        }
    }

    /**
     * 安全地添加文件到zip，处理Windows文件锁定问题
     */
    private static boolean addFileToZipSafely(java.io.File file, String entryName, java.util.zip.ZipOutputStream zos) {
        // 跳过可能被锁定的活跃日志文件
        String fileName = file.getName().toLowerCase();
        if (isActiveLogFile(fileName)) {
            log.debug("Skipping active log file that may be locked: {}", entryName);
            return false;
        }

        int maxRetries = 3;
        int retryDelay = 100; // 100ms

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                zos.putNextEntry(new java.util.zip.ZipEntry(entryName));

                // 使用NIO Files.copy 或者带重试的FileInputStream
                if (attempt == 1) {
                    // 第一次尝试使用NIO方式
                    try {
                        java.nio.file.Files.copy(file.toPath(), zos);
                        zos.closeEntry();
                        return true;
                    } catch (java.io.IOException nioException) {
                        log.debug("NIO copy failed for {}, trying FileInputStream: {}", entryName, nioException.getMessage());
                        zos.closeEntry(); // 关闭已经打开的entry
                        // 继续到FileInputStream尝试
                    }
                }

                // 使用传统的FileInputStream方式，带重试
                zos.putNextEntry(new java.util.zip.ZipEntry(entryName));

                try (java.io.FileInputStream fis = new java.io.FileInputStream(file)) {
                    byte[] buffer = new byte[8192];
                    int length;
                    while ((length = fis.read(buffer)) > 0) {
                        zos.write(buffer, 0, length);
                    }
                }

                zos.closeEntry();
                return true;

            } catch (java.io.IOException e) {
                String errorMsg = e.getMessage();
                if (errorMsg != null && (errorMsg.contains("另一个程序") ||
                                       errorMsg.contains("process cannot access") ||
                                       errorMsg.contains("being used by another process"))) {

                    if (attempt < maxRetries) {
                        log.debug("File locked, retrying {}/{} for {}: {}", attempt, maxRetries, entryName, errorMsg);
                        try {
                            Thread.sleep(retryDelay * attempt); // 递增延迟
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            log.warn("Thread interrupted while waiting to retry file access for: {}", entryName);
                            return false;
                        }
                        continue;
                    } else {
                        log.warn("File remains locked after {} attempts, skipping: {} - {}", maxRetries, entryName, errorMsg);
                        return false;
                    }
                } else {
                    // 其他类型的IO异常，直接失败
                    log.error("IO error adding file to zip: {} - {}", entryName, errorMsg);
                    return false;
                }
            }
        }

        return false;
    }

    /**
     * 判断是否为可能被锁定的活跃日志文件
     */
    private static boolean isActiveLogFile(String fileName) {
        // 活跃的日志文件通常被锁定，但我们可以尝试备份它们
        // 只跳过明确知道会被锁定的临时文件和检查点文件

        // 跳过临时和锁定文件
        if (fileName.endsWith(".swap") ||
            fileName.endsWith(".tmp") ||
            fileName.endsWith(".lock") ||
            fileName.endsWith(".txnindex")) {
            return true;
        }

        // 跳过可能正在写入的检查点文件
        if (fileName.equals("recovery-point-offset-checkpoint") ||
            fileName.equals("log-start-offset-checkpoint") ||
            fileName.equals("cleaner-offset-checkpoint") ||
            fileName.equals("replication-offset-checkpoint")) {
            return true;
        }

        // 让.log、.index、.timeindex、.snapshot文件通过重试机制尝试备份
        // 因为它们是KRaft恢复的关键文件
        return false;
    }

    /**
     * 上传zip数据到S3
     */
    private static boolean uploadZipToS3(ObjectStorage objectStorage, String objectKey, byte[] zipData) {
        ByteBuf zipByteBuf = null;

        try {
            log.info("Uploading KRaft directory backup to S3: key={}, size={} bytes", objectKey, zipData.length);

            // 创建ByteBuf
            zipByteBuf = Unpooled.wrappedBuffer(zipData);

            // 创建WriteOptions
            ObjectStorage.WriteOptions writeOptions = new ObjectStorage.WriteOptions();

            // 上传策略：根据文件大小选择直接上传或分片上传
            CompletableFuture<Void> uploadFuture;
            Writer writer = null;

            if (zipData.length < Writer.MIN_PART_SIZE) {
                // 小文件直接上传
                log.info("Using direct S3 PUT for KRaft backup ({} bytes)", zipData.length);
                uploadFuture = objectStorage.write(writeOptions, objectKey, zipByteBuf)
                    .thenApply(writeResult -> {
                        log.info("Successfully uploaded KRaft directory backup to S3 using direct PUT");
                        return null;
                    });
            } else {
                // 大文件分片上传
                log.info("Using MultiPartWriter for KRaft backup ({} bytes)", zipData.length);
                writer = objectStorage.writer(writeOptions, objectKey);

                final Writer writerToRelease = writer;
                uploadFuture = writer.write(zipByteBuf)
                    .thenCompose(v -> {
                        log.info("KRaft backup data written to writer, closing...");
                        return writerToRelease.close();
                    })
                    .whenComplete((result, throwable) -> {
                        // 释放writer资源
                        try {
                            writerToRelease.release().get(30, TimeUnit.SECONDS);
                            log.trace("Successfully released writer resources for KRaft backup");
                        } catch (Exception releaseException) {
                            log.warn("Error releasing writer resources for KRaft backup: {}", releaseException.getMessage());
                        }
                    });
            }

            // 等待上传完成
            uploadFuture.get(300, TimeUnit.SECONDS); // 5分钟超时
            log.info("Successfully uploaded KRaft directory backup to S3: {}", objectKey);
            return true;

        } catch (Exception e) {
            log.error("Failed to upload KRaft directory backup to S3: {}", e.getMessage(), e);
            return false;
        } finally {
            // 释放ByteBuf
            if (zipByteBuf != null && zipByteBuf.refCnt() > 0) {
                try {
                    zipByteBuf.release();
                    log.trace("Released ByteBuf for KRaft directory backup");
                } catch (Exception releaseException) {
                    log.warn("Error releasing ByteBuf for KRaft backup: {}", releaseException.getMessage());
                }
            }
        }
    }

    /**
     * 从S3下载zip数据
     */
    private static byte[] downloadZipFromS3(ObjectStorage objectStorage, String objectKey) {
        try {
            log.info("Downloading KRaft directory backup from S3: {}", objectKey);

            // 创建ReadOptions
            ObjectStorage.ReadOptions readOptions = new ObjectStorage.ReadOptions().bucket((short) 0);

            // 从S3读取数据
            CompletableFuture<ByteBuf> future = objectStorage.read(readOptions, objectKey);
            ByteBuf byteBuf = future.get(300, TimeUnit.SECONDS); // 5分钟超时

            if (byteBuf == null) {
                log.error("Failed to download KRaft backup from S3: received null data");
                return null;
            }

            try {
                byte[] zipData = new byte[byteBuf.readableBytes()];
                byteBuf.readBytes(zipData);

                log.info("Successfully downloaded KRaft directory backup from S3: {} bytes", zipData.length);
                return zipData;

            } finally {
                byteBuf.release();
            }

        } catch (Exception e) {
            log.error("Failed to download KRaft directory backup from S3 object {}: {}", objectKey, e.getMessage(), e);
            return null;
        }
    }

    /**
     * 从zip数据恢复KRaft目录
     */
    private static boolean restoreKRaftDirectoryFromZip(String kraftLogDirPath, byte[] zipData) {
        java.io.File tempDir = null;

        try {
            log.info("Restoring KRaft directory from zip data: {} bytes to {}", zipData.length, kraftLogDirPath);

            // 创建临时目录
            tempDir = java.nio.file.Files.createTempDirectory("kraft-restore").toFile();
            log.debug("Created temporary directory for restoration: {}", tempDir.getAbsolutePath());

            // 解压zip到临时目录
            if (!extractZipToDirectory(zipData, tempDir)) {
                log.error("Failed to extract KRaft backup zip to temporary directory");
                return false;
            }

            // 验证解压后的内容
            java.io.File[] extractedDirs = tempDir.listFiles(java.io.File::isDirectory);
            if (extractedDirs == null || extractedDirs.length == 0) {
                log.error("No directories found in extracted KRaft backup");
                return false;
            }

            java.io.File sourceDir = extractedDirs[0].getParentFile(); // 取第一个目录
            log.info("Found extracted KRaft directory: {}", sourceDir.getName());

            // 验证目录结构
            if (!validateKRaftDirectory(sourceDir)) {
                log.error("KRaft directory validation failed");
                return false;
            }

            // 清理目标目录
            java.io.File targetDir = new java.io.File(kraftLogDirPath);
            if (targetDir.exists()) {
                log.info("Cleaning existing KRaft directory: {}", kraftLogDirPath);
                if (!deleteDirectoryRecursively(targetDir)) {
                    log.error("Failed to clean existing KRaft directory");
                    return false;
                }
            }

            // 创建父目录
            targetDir.getParentFile().mkdirs();

            // 复制解压后的目录到目标位置
            if (!copyDirectoryRecursively(sourceDir, targetDir)) {
                log.error("Failed to copy restored KRaft directory");
                return false;
            }

            log.info("Successfully restored KRaft directory: {} files", countFilesInDirectory(targetDir));
            return true;

        } catch (Exception e) {
            log.error("Failed to restore KRaft directory from zip: {}", e.getMessage(), e);
            return false;
        } finally {
            // 清理临时目录
            if (tempDir != null && tempDir.exists()) {
                try {
                    deleteDirectoryRecursively(tempDir);
                    log.debug("Cleaned up temporary directory: {}", tempDir.getAbsolutePath());
                } catch (Exception e) {
                    log.warn("Failed to clean temporary directory: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 解压zip数据到目录
     */
    private static boolean extractZipToDirectory(byte[] zipData, java.io.File targetDir) {
        try (java.util.zip.ZipInputStream zis = new java.util.zip.ZipInputStream(
                new java.io.ByteArrayInputStream(zipData))) {

            java.util.zip.ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                java.io.File file = new java.io.File(targetDir, entry.getName());

                if (entry.isDirectory()) {
                    file.mkdirs();
                } else {
                    // 创建父目录
                    file.getParentFile().mkdirs();

                    // 写入文件内容
                    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(file)) {
                        byte[] buffer = new byte[8192];
                        int length;
                        while ((length = zis.read(buffer)) > 0) {
                            fos.write(buffer, 0, length);
                        }
                    }

                    log.debug("Extracted file: {} ({} bytes)", entry.getName(), file.length());
                }

                zis.closeEntry();
            }

            return true;
        } catch (Exception e) {
            log.error("Failed to extract zip data to directory: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * 验证KRaft目录结构
     */
    private static boolean validateKRaftDirectory(java.io.File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            log.error("KRaft directory does not exist: {}", dir.getAbsolutePath());
            return false;
        }

        // 检查关键文件
        String[] criticalFiles = {"meta.properties"};
        for (String fileName : criticalFiles) {
            if (!new java.io.File(dir, fileName).exists()) {
                log.error("Critical file missing in KRaft directory: {}", fileName);
                return false;
            }
        }

        // 检查__cluster_metadata-0目录
        java.io.File metadataDir = new java.io.File(dir, "__cluster_metadata-0");
        if (!metadataDir.exists() || !metadataDir.isDirectory()) {
            log.error("__cluster_metadata-0 directory missing in KRaft backup");
            return false;
        }

        log.info("KRaft directory validation passed");
        return true;
    }

    /**
     * 递归复制目录
     */
    private static boolean copyDirectoryRecursively(java.io.File source, java.io.File target) {
        try {
            if (source.isDirectory()) {
                if (!target.exists()) {
                    target.mkdirs();
                }

                java.io.File[] files = source.listFiles();
                if (files != null) {
                    for (java.io.File file : files) {
                        java.io.File targetFile = new java.io.File(target, file.getName());
                        if (!copyDirectoryRecursively(file, targetFile)) {
                            return false;
                        }
                    }
                }
            } else {
                java.nio.file.Files.copy(
                    source.toPath(),
                    target.toPath(),
                    java.nio.file.StandardCopyOption.REPLACE_EXISTING
                );
                log.debug("Copied file: {} ({} bytes)", source.getName(), source.length());
            }
            return true;
        } catch (Exception e) {
            log.error("Failed to copy {} to {}: {}", source.getAbsolutePath(), target.getAbsolutePath(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 递归删除目录
     */
    private static boolean deleteDirectoryRecursively(java.io.File dir) {
        try {
            if (dir.exists()) {
                if (dir.isDirectory()) {
                    java.io.File[] files = dir.listFiles();
                    if (files != null) {
                        for (java.io.File file : files) {
                            if (!deleteDirectoryRecursively(file)) {
                                return false;
                            }
                        }
                    }
                }
                return dir.delete();
            }
            return true;
        } catch (Exception e) {
            log.error("Failed to delete {}: {}", dir.getAbsolutePath(), e.getMessage(), e);
            return false;
        }
    }

    /**
     * 生成KRaft目录备份的S3对象key
     */
    private static String generateKRaftDirectoryBackupKeyByNowTime() {
        // 使用人类可读的时间格式：年月日小时分钟 (yyyyMMddHHmm)
        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        java.time.format.DateTimeFormatter formatter = java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        String timestamp = now.format(formatter);
//        String timestamp = "2025092519";
        return "kraft_directory_backup/kraft-metadata-" + timestamp + ".zip";
    }

    private static String generateKRaftDirectoryBackupKeyByTime(String timestamp) {
        // 使用人类可读的时间格式：年月日小时分钟 (yyyyMMdd_HHmm)
//        String timestamp = "2025092519";
        return "kraft_directory_backup/kraft-metadata-" + timestamp + ".zip";
    }



    /**
     * 统计目录中的文件数量
     */
    private static int countFilesInDirectory(java.io.File dir) {
        if (!dir.exists() || !dir.isDirectory()) {
            return 0;
        }

        int count = 0;
        java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (java.io.File file : files) {
                if (file.isDirectory()) {
                    count += countFilesInDirectory(file);
                } else {
                    count++;
                }
            }
        }
        return count;
    }

    /**
     * 便捷方法：检查是否需要从S3恢复KRaft目录
     */
    public static boolean shouldRestoreKRaftDirectoryFromS3(String kraftLogDirPath) {
        try {
            java.io.File kraftLogDir = new java.io.File(kraftLogDirPath);

            // 目录不存在
            if (!kraftLogDir.exists()) {
                log.info("KRaft log directory does not exist, should restore from S3");
                return true;
            }

            // 目录为空
            java.io.File[] files = kraftLogDir.listFiles();
            if (files == null || files.length == 0) {
                log.info("KRaft log directory is empty, should restore from S3");
                return true;
            }

            // 检查关键文件是否存在
            if (!new java.io.File(kraftLogDir, "meta.properties").exists() ||
                !new java.io.File(kraftLogDir, "__cluster_metadata-0").exists()) {
                log.info("Critical KRaft files missing, should restore from S3");
                return true;
            }

            return false;
        } catch (Exception e) {
            log.warn("Error checking KRaft directory restore status: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 获取KRaft日志目录路径
     * 参考ServerLogConfigs.LOG_DIRS_CONFIG的解析方式
     */
    private static String getKraftLogDirectory() {
        try {
            S3SnapshotConfig config = getS3SnapshotConfig();
            if (config == null) {
                log.warn("S3 snapshot configuration is not available");
                return null;
            }

            // 首先尝试从S3SnapshotConfig中获取logDirs
            if (config.getLogDirs() != null && !config.getLogDirs().isEmpty()) {
                String kraftLogDir = config.getLogDirs().get(0);
                log.debug("Resolved KRaft log directory from S3SnapshotConfig: {}", kraftLogDir);
                return kraftLogDir;
            }

            // 回退到系统属性和环境变量
            String logDirsConfig = getConfigProperty("log.dirs");
            if (logDirsConfig == null) {
                // 如果log.dirs不存在，尝试log.dir
                logDirsConfig = getConfigProperty("log.dir");
            }

            if (logDirsConfig == null) {
                // 使用默认值
                logDirsConfig = "/tmp/kafka-logs";
                log.debug("Using default log directory: {}", logDirsConfig);
            }

            // 解析CSV格式的目录列表，取第一个目录
            String[] logDirs = logDirsConfig.split(",");
            if (logDirs.length > 0) {
                String kraftLogDir = logDirs[0].trim();
                log.debug("Resolved KRaft log directory from fallback: {}", kraftLogDir);
                return kraftLogDir;
            }

            log.warn("No valid log directories found in configuration");
            return null;
        } catch (Exception e) {
            log.error("Error getting KRaft log directory", e);
            return null;
        }
    }

    /**
     * 从系统属性或环境变量中获取配置属性
     */
    private static String getConfigProperty(String key) {
        // 首先尝试系统属性
        String value = System.getProperty(key);
        if (value != null) {
            return value;
        }

        // 然后尝试环境变量 (将.替换为_并转为大写)
        String envKey = key.replace(".", "_").toUpperCase();
        value = System.getenv(envKey);
        if (value != null) {
            return value;
        }

        // 硬编码已知的默认路径（基于用户的配置文件）
        if ("log.dirs".equals(key) || "log.dir".equals(key)) {
            // 基于用户当前的配置，使用这个路径作为默认值
            return "D:/workspace_java/qqeasonchen/automq2/kraft-combined-logs-idc-b";
        }

        return null;
    }
}
