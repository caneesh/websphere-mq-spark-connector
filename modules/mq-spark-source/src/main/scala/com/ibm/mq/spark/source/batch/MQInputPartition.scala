package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.InputPartition

/**
 * Represents a single partition for reading MQ messages.
 * Currently uses a single partition per queue.
 */
case class MQInputPartition(
    partitionId: Int,
    options: MQSourceOptions
) extends InputPartition with Serializable
