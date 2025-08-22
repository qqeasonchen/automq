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

package kafka

import com.automq.shell.log.S3LogConfig
import com.automq.stream.s3.operator.{ObjectStorage, ObjectStorageFactory}
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer}

class KafkaS3LogConfig(
  config: KafkaConfig,
  kafkaServer: KafkaServer,
  kafkaRaftServer: KafkaRaftServer
) extends S3LogConfig {

  private val _objectStorage = if (config.automq.opsBuckets().isEmpty) {
    null
  } else {
    // Check if quorum is enabled for ops buckets
    val quorumEnabled = config.originals().get("s3.stream.quorum.enabled") match {
      case value: String => java.lang.Boolean.parseBoolean(value)
      case _ => false
    }
    if (config.automq.opsBuckets().size() > 1 && quorumEnabled) {
      // Use quorum-enabled ObjectStorage for multi-bucket ops
      ObjectStorageFactory.instance()
        .builder()
        .buckets(config.automq.opsBuckets())
        .quorumEnabled(true)
        .quorumSize(config.automq.opsBuckets().size())
        .writeQuorumSize(config.automq.opsBuckets().size())
        .readQuorumSize(Math.max(1, config.automq.opsBuckets().size() / 2 + 1))
        .threadPrefix("s3-log")
        .build()
    } else {
      // Single bucket mode (legacy)
      ObjectStorageFactory.instance().builder(config.automq.opsBuckets().get(0)).threadPrefix("s3-log").build()
    }
  }

  override def isEnabled: Boolean = config.s3OpsTelemetryEnabled

  override def isActiveController: Boolean = {

    if (kafkaServer != null) {
      false
    } else {
      kafkaRaftServer.controller.exists(controller => controller.controller != null && controller.controller.isActive)
    }
  }

  override def clusterId(): String = {
    if (kafkaServer != null) {
      kafkaServer.clusterId
    } else {
      kafkaRaftServer.getSharedServer().clusterId
    }
  }

  override def nodeId(): Int = config.nodeId

  override def objectStorage(): ObjectStorage = {
    _objectStorage
  }

}
