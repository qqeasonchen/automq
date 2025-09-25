/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log

import java.io.File
import kafka.utils.Logging
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{BufferSupplier, Time}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.metadata.SnapshotController
import org.apache.kafka.snapshot.RawSnapshotReader
import org.apache.kafka.storage.internals.epoch.LeaderEpochFileCache
import org.apache.kafka.storage.internals.log.{LogConfig, LogSegment, ProducerStateManager}

import java.util.Optional
import scala.compat.java8.OptionConverters._

/**
 * Helper class for recovering KRaft metadata log from S3 checkpoint snapshots.
 * This class handles the complete recovery process including:
 * - Reading checkpoint data from S3 using SnapshotController
 * - Converting checkpoint data to proper Kafka log format
 * - Creating log, index, timeindex, and txnindex files
 * - Rebuilding indexes from recovered data
 */
class CheckpointRecoveryHelper(
  dir: File,
  topicPartition: TopicPartition,
  config: LogConfig,
  time: Time,
  leaderEpochCache: Optional[LeaderEpochFileCache],
  producerStateManager: ProducerStateManager
) extends Logging {

  logIdent = s"[CheckpointRecovery partition=$topicPartition, dir=${dir.getParent}] "

  /**
   * Attempt to recover from S3 checkpoint if available.
   * This method tries to load a snapshot from S3 and convert it to proper log segment files.
   *
   * @return Some(LogSegment) if recovery was successful, None otherwise
   */
  def recoverFromS3Checkpoint(): Option[LogSegment] = {
    try {
      info("Attempting to recover from S3 checkpoint...")

      // Check if S3 snapshot reading is enabled
      val s3Config = SnapshotController.getS3SnapshotConfig()
      if (s3Config == null || !s3Config.isS3KraftSnapshotReadEnabled()) {
        debug("S3 Kraft snapshot reading is disabled, skipping checkpoint recovery")
        return None
      }

      // Load snapshot from S3
      val s3SnapshotOpt = SnapshotController.loadSnapshotFromS3()
      s3SnapshotOpt.asScala match {
        case Some(s3Snapshot) =>
          info(s"Successfully loaded snapshot from S3: ${s3Snapshot.snapshotId()}")
          convertCheckpointToLogSegment(s3Snapshot)
        case None =>
          debug("No snapshot found in S3 storage or failed to load")
          None
      }
    } catch {
      case e: Exception =>
        warn(s"Error during S3 checkpoint recovery: ${e.getMessage}", e)
        None
    }
  }

  /**
   * Convert S3 checkpoint RawSnapshotReader to a proper LogSegment with all index files.
   * This is the core method that handles the conversion process.
   *
   * @param snapshotReader RawSnapshotReader from S3 checkpoint
   * @return Some(LogSegment) if conversion successful, None otherwise
   */
  private def convertCheckpointToLogSegment(snapshotReader: RawSnapshotReader): Option[LogSegment] = {
    val snapshotId = snapshotReader.snapshotId()
    val baseOffset = snapshotId.offset()
    val epoch = snapshotId.epoch()

    try {
      info(s"Converting S3 checkpoint to log segment: offset=$baseOffset, epoch=$epoch")

      // Step 1: Create new log segment with the checkpoint's base offset
      val logSegment = LogSegment.open(
        dir,
        baseOffset,
        config,
        time,
        config.initFileSize,
        config.preallocate
      )

      // Step 2: Process records from checkpoint and write to log segment
      var totalRecordCount = 0

      try {
        val records = snapshotReader.records()
        val batches = records.batches()

        // Process each batch from the checkpoint
        batches.forEach { batch =>
          info(s"Processing batch: baseOffset=${batch.baseOffset()}, lastOffset=${batch.lastOffset()}")

          // Create a list to hold converted records for this batch
          val convertedRecords = new java.util.ArrayList[SimpleRecord]()

          // Process each record in the batch
          val iterator = batch.streamingIterator(BufferSupplier.create())
          try {
            while (iterator.hasNext) {
              val record = iterator.next()
              try {
                // Convert checkpoint record to SimpleRecord format
                val key = if (record.hasKey) {
                  val keyBytes = new Array[Byte](record.keySize())
                  record.key().get(keyBytes)
                  keyBytes
                } else null

                val value = if (record.hasValue) {
                  val valueBytes = new Array[Byte](record.valueSize())
                  record.value().get(valueBytes)
                  valueBytes
                } else null

                // Create SimpleRecord for the log segment
                val simpleRecord = new SimpleRecord(
                  record.timestamp(),
                  key,
                  value,
                  record.headers().toArray
                )

                convertedRecords.add(simpleRecord)
                totalRecordCount += 1

                debug(s"Converted record: timestamp=${record.timestamp()}, " +
                      s"keySize=${record.keySize()}, valueSize=${record.valueSize()}")

              } catch {
                case e: Exception =>
                  warn(s"Failed to convert record: ${e.getMessage}", e)
              }
            }
          } finally {
            iterator.close()
          }

          // Create MemoryRecords from converted records and append to segment
          if (!convertedRecords.isEmpty) {
            try {
              val memoryRecords = MemoryRecords.withRecords(
                batch.baseOffset(),
                Compression.NONE,
                convertedRecords.toArray(new Array[SimpleRecord](convertedRecords.size())): _*
              )

              // Append the records to the log segment
              logSegment.append(
                batch.lastOffset() + 1, // largestOffset
                batch.maxTimestamp(),   // largestTimestamp
                batch.baseOffset(),     // shallowOffsetOfMaxTimestamp
                memoryRecords
              )

              debug(s"Appended batch to segment: baseOffset=${batch.baseOffset()}, " +
                    s"lastOffset=${batch.lastOffset()}, recordCount=${convertedRecords.size()}")

            } catch {
              case e: Exception =>
                error(s"Failed to append batch to log segment: ${e.getMessage}", e)
                throw e
            }
          }
        }

        info(s"Successfully processed $totalRecordCount records from checkpoint")

        // Step 3: Flush the segment to ensure data is written to disk
        logSegment.flush()
        info("Flushed log segment data to disk")

        // Step 4: Close and reopen to trigger index building
        logSegment.close()
        info("Closed initial log segment")

        // Reopen the segment - this will trigger index file creation/validation
        val reopenedSegment = LogSegment.open(
          dir,
          baseOffset,
          config,
          time,
          true, // fileAlreadyExists = true (files now exist)
          0,
          false,
          ""
        )

        // Step 5: Recover the segment to rebuild all indexes
        info("Starting segment recovery to rebuild indexes...")
        val tempProducerStateManager = new ProducerStateManager(
          topicPartition,
          dir,
          producerStateManager.maxTransactionTimeoutMs(),
          producerStateManager.producerStateManagerConfig(),
          time
        )

        val bytesRecovered = reopenedSegment.recover(tempProducerStateManager, leaderEpochCache)
        info(s"Segment recovery completed: bytesRecovered=$bytesRecovered")

        // Step 6: Validate the segment and its indexes
        try {
          reopenedSegment.sanityCheck(false) // timeIndexFileNewlyCreated = false
          info("Segment sanity check passed - all index files are valid")
        } catch {
          case e: Exception =>
            warn(s"Segment sanity check failed, but continuing: ${e.getMessage}")
        }

        info(s"Successfully converted S3 checkpoint to log segment: " +
             s"baseOffset=$baseOffset, nextOffset=${reopenedSegment.readNextOffset}, " +
             s"recordCount=$totalRecordCount")

        Some(reopenedSegment)

      } finally {
        // Always close the snapshot reader if it's AutoCloseable
        try {
          snapshotReader match {
            case closeable: AutoCloseable => closeable.close()
            case _ => // Do nothing if not closeable
          }
        } catch {
          case e: Exception =>
            warn(s"Error closing snapshot reader: ${e.getMessage}")
        }
      }

    } catch {
      case e: Exception =>
        error(s"Failed to convert checkpoint to log segment: ${e.getMessage}", e)

        // Cleanup any partially created files
        try {
          cleanupPartialFiles(baseOffset)
        } catch {
          case cleanupEx: Exception =>
            warn(s"Error during cleanup: ${cleanupEx.getMessage}")
        }

        None
    }
  }

  /**
   * Clean up any partially created files if recovery fails.
   */
  private def cleanupPartialFiles(baseOffset: Long): Unit = {
    val offsetStr = String.format("%020d", baseOffset.asInstanceOf[java.lang.Long])
    val filesToDelete = Array(
      new File(dir, s"$offsetStr.log"),
      new File(dir, s"$offsetStr.index"),
      new File(dir, s"$offsetStr.timeindex"),
      new File(dir, s"$offsetStr.txnindex")
    )

    filesToDelete.foreach { file =>
      if (file.exists()) {
        try {
          file.delete()
          debug(s"Cleaned up partial file: ${file.getName}")
        } catch {
          case e: Exception =>
            warn(s"Failed to delete partial file ${file.getName}: ${e.getMessage}")
        }
      }
    }
  }

  /**
   * Check if recovery from S3 checkpoint is possible and recommended.
   * This method can be called before attempting recovery to decide if it should be attempted.
   *
   * @return true if S3 checkpoint recovery should be attempted
   */
  def shouldAttemptS3Recovery(): Boolean = {
    try {
      val s3Config = SnapshotController.getS3SnapshotConfig()
      s3Config != null && s3Config.isS3KraftSnapshotReadEnabled()
    } catch {
      case _: Exception => false
    }
  }
}