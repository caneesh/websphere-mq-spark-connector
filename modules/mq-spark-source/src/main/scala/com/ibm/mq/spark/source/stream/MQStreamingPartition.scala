package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.InputPartition

/**
 * Input partition for streaming MQ reads.
 * Tracks the offset range for this micro-batch.
 */
case class MQStreamingPartition(
    partitionId: Int,
    startOffset: MQOffset,
    endOffset: MQOffset,
    options: MQSourceOptions
) extends InputPartition with Serializable {

  def expectedMessageCount: Long = {
    math.max(0, endOffset.messageCount - startOffset.messageCount)
  }
}
