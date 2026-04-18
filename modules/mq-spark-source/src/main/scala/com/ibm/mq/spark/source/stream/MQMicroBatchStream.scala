package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Spark Structured Streaming micro-batch source for MQ.
 *
 * MQ Progress Model:
 * ==================
 * MQ does not have sequential offsets like Kafka. This source uses a
 * batch-based progress model:
 *
 * 1. Each micro-batch has a unique batchId (monotonically increasing)
 * 2. The source polls for available messages up to batchSize
 * 3. Progress is tracked as "how many batches completed" not "which messages"
 *
 * Offset Semantics:
 * - initialOffset(): Returns batch 0 (no messages processed)
 * - latestOffset(): Returns next batch to process (batchId + 1)
 * - planInputPartitions(start, end): Plans to read (end.batchId - start.batchId) batches
 * - commit(end): Records that batch(es) up to end have been checkpointed
 *
 * Delivery Semantics:
 * - At-least-once: Messages committed to MQ when reader completes
 * - Restart behavior: Resumes from queue's current state, not from offset
 * - Duplicates possible if: Spark fails after MQ commit but before checkpoint
 *
 * To achieve effectively-once:
 * - Use message_id for downstream idempotency
 * - Store processed message_ids in external state
 *
 * Known Limitations:
 * - Cannot replay from arbitrary offset (MQ doesn't support this)
 * - latestOffset() estimates batch size, actual may differ
 * - No backpressure based on downstream processing time
 */
class MQMicroBatchStream(
    options: MQSourceOptions,
    schema: StructType
) extends MicroBatchStream {

  private val log = LoggerFactory.getLogger(getClass)

  @volatile private var currentOffset: MQOffset = MQOffset.Initial
  @volatile private var lastCommittedOffset: MQOffset = MQOffset.Initial

  override def initialOffset(): Offset = {
    log.info("Returning initial offset")
    MQOffset.Initial
  }

  override def latestOffset(): Offset = {
    val nextOffset = currentOffset.nextBatch(options.batchSize)
    currentOffset = nextOffset
    log.debug(s"Latest offset: batchId=${nextOffset.batchId}, expectedCount=${options.batchSize}")
    nextOffset
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start match {
      case o: MQOffset => o
      case _ => throw new IllegalArgumentException(
        s"Expected MQOffset but got ${start.getClass.getName}. This indicates a Spark internal error or incompatible offset format.")
    }
    val endOffset = end match {
      case o: MQOffset => o
      case _ => throw new IllegalArgumentException(
        s"Expected MQOffset but got ${end.getClass.getName}. This indicates a Spark internal error or incompatible offset format.")
    }

    val batchesToProcess = (endOffset.batchId - startOffset.batchId).toInt
    if (batchesToProcess <= 0) {
      log.warn(s"No batches to process: start=${startOffset.batchId}, end=${endOffset.batchId}")
      return Array.empty
    }

    log.info(s"Planning $batchesToProcess partition(s) from batch ${startOffset.batchId} to ${endOffset.batchId}")

    Array(MQStreamingPartition(
      partitionId = 0,
      startOffset = startOffset,
      endOffset = endOffset,
      options = options
    ))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    MQStreamingReaderFactory(schema)
  }

  override def deserializeOffset(json: String): Offset = {
    MQOffset.fromJson(json)
  }

  override def commit(end: Offset): Unit = {
    val offset = end.asInstanceOf[MQOffset]
    lastCommittedOffset = offset
    log.info(s"Checkpoint committed: batchId=${offset.batchId}, totalProcessed=${offset.processedCount}")
  }

  override def stop(): Unit = {
    log.info(s"Stopping MQ stream. Last committed: batchId=${lastCommittedOffset.batchId}, processed=${lastCommittedOffset.processedCount}")
  }
}
