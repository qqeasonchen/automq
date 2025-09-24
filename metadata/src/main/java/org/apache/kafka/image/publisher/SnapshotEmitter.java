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
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.MetadataProvenance;
import org.apache.kafka.image.publisher.metrics.SnapshotEmitterMetrics;
import org.apache.kafka.image.writer.ImageWriterOptions;
import org.apache.kafka.image.writer.RaftSnapshotWriter;
import org.apache.kafka.metadata.MetadataRecordSerde;
import org.apache.kafka.metadata.SnapshotController;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.raft.RaftClient;
import org.apache.kafka.server.common.ApiMessageAndVersion;
import org.apache.kafka.server.fault.FaultHandler;
import org.apache.kafka.snapshot.SnapshotWriter;

import com.automq.stream.s3.operator.ObjectStorage;

import org.slf4j.Logger;

import java.util.Optional;


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

    /**
     * S3SnapshotCoordinator for handling S3 snapshot operations.
     */
    private final SnapshotController<ApiMessageAndVersion> snapshotController;

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

        // Create and start snapshotController if ObjectStorage is available
        if (objectStorage != null) {
            this.snapshotController = new SnapshotController<>(
                time,
                objectStorage,
                bucketName,
                faultHandler,
                threadNamePrefix,
                MetadataRecordSerde.INSTANCE
            );
            this.snapshotController.start();
        } else {
            this.snapshotController = null;
        }
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

            // Use snapshotController for async snapshot operations
            if (snapshotController != null) {
                snapshotController.scheduleSnapshotOperations(image, provenance);
            }

        } catch (Throwable e) {
            log.error("Encountered error while writing {}", provenance.snapshotName(), e);
            throw e;
        } finally {
            Utils.closeQuietly(writer, "RaftSnapshotWriter");
            Utils.closeQuietly(snapshotWriter.get(), "SnapshotWriter");
        }
    }


    /**
     * Close the snapshot emitter and its event queue.
     */
    public void close() throws InterruptedException {
        eventQueue.beginShutdown("close");
        eventQueue.close();

        // Close snapshotController if it exists
        if (snapshotController != null) {
            try {
                snapshotController.beginShutdown();
                snapshotController.close();
            } catch (Exception e) {
                log.warn("Error closing S3SnapshotCoordinator", e);
            }
        }
    }
}
