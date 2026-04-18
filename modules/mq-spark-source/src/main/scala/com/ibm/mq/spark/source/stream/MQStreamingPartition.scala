package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.InputPartition

/**
 * Input partition for streaming MQ reads.
 *
 * MQ Partition Model:
 * - Unlike Kafka, MQ partitions don't correspond to queue positions
 * - This partition represents a "batch of work" to process
 * - expectedMessageCount is the target batch size, actual may differ
 */
case class MQStreamingPartition(
    partitionId: Int,
    startOffset: MQOffset,
    endOffset: MQOffset,
    options: MQSourceOptions
) extends InputPartition with Serializable {

  /**
   * Expected number of messages to read in this micro-batch.
   *
   * Note: This is a target, not a guarantee. The actual number depends on:
   * - Messages available in the queue
   * - Poll timeout reaching before batch fills
   * - Processing errors
   */
  def expectedMessageCount: Long = {
    options.batchSize.toLong
  }
}
