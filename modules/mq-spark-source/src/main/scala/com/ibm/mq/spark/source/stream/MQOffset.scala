package com.ibm.mq.spark.source.stream

import org.apache.spark.sql.connector.read.streaming.Offset

/**
 * Offset representation for MQ streaming source.
 *
 * MQ Progress Model (Honest Design):
 * ==================================
 * Unlike Kafka or other log-based systems, MQ does not have sequential offsets.
 * This offset represents "progress markers" rather than replay positions.
 *
 * What this offset tracks:
 * - batchId: Monotonically increasing batch/epoch counter (for ordering)
 * - processedCount: Total messages processed across all batches (for monitoring)
 * - lastBatchCount: Messages in the last completed batch
 * - lastBatchTimestamp: When the last batch was processed
 * - lastMessageIds: Up to N message IDs from the last batch (for deduplication hints)
 *
 * What this offset CANNOT do:
 * - Replay from a specific position (MQ doesn't support this)
 * - Skip to a future position
 * - Guarantee no duplicates on restart (at-least-once semantics)
 *
 * Restart Semantics:
 * - On restart, processing continues from the queue's current state
 * - Messages delivered but not committed will be redelivered
 * - Downstream must handle potential duplicates using message_id
 *
 * Checkpoint Payload Example:
 * {
 *   "batchId": 42,
 *   "processedCount": 10500,
 *   "lastBatchCount": 250,
 *   "lastBatchTimestamp": 1704067200000,
 *   "lastMessageIds": ["414d5120514d312020202020...", "414d5120514d312020202021..."]
 * }
 */
case class MQOffset(
    batchId: Long,
    processedCount: Long,
    lastBatchCount: Long,
    lastBatchTimestamp: Long,
    lastMessageIds: Seq[String] = Seq.empty
) extends Offset with Ordered[MQOffset] with Serializable {

  override def json(): String = {
    val idsJson = lastMessageIds.take(10).map(id => s""""$id"""").mkString("[", ",", "]")
    s"""{"batchId":$batchId,"processedCount":$processedCount,"lastBatchCount":$lastBatchCount,"lastBatchTimestamp":$lastBatchTimestamp,"lastMessageIds":$idsJson}"""
  }

  override def compare(that: MQOffset): Int = {
    this.batchId.compareTo(that.batchId)
  }

  def nextBatch(messagesInBatch: Long, messageIds: Seq[String] = Seq.empty): MQOffset = {
    MQOffset(
      batchId = batchId + 1,
      processedCount = processedCount + messagesInBatch,
      lastBatchCount = messagesInBatch,
      lastBatchTimestamp = System.currentTimeMillis(),
      lastMessageIds = messageIds.take(10)
    )
  }

  def expectedMessageCount: Long = lastBatchCount
}

object MQOffset {

  val Initial: MQOffset = MQOffset(
    batchId = 0,
    processedCount = 0,
    lastBatchCount = 0,
    lastBatchTimestamp = 0L,
    lastMessageIds = Seq.empty
  )

  def fromJson(json: String): MQOffset = {
    val batchIdPattern = """"batchId":(\d+)""".r
    val processedPattern = """"processedCount":(\d+)""".r
    val lastBatchCountPattern = """"lastBatchCount":(\d+)""".r
    val tsPattern = """"lastBatchTimestamp":(\d+)""".r
    val idsPattern = """"lastMessageIds":\[(.*?)\]""".r

    val batchId = batchIdPattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)
    val processed = processedPattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)
    val lastBatchCount = lastBatchCountPattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)
    val ts = tsPattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)

    val ids = idsPattern.findFirstMatchIn(json).map { m =>
      val idsStr = m.group(1)
      if (idsStr.isEmpty) Seq.empty
      else idsStr.split(",").map(_.trim.stripPrefix("\"").stripSuffix("\"")).toSeq
    }.getOrElse(Seq.empty)

    MQOffset(batchId, processed, lastBatchCount, ts, ids)
  }

  def apply(messageCount: Long, timestamp: Long): MQOffset = {
    MQOffset(
      batchId = messageCount,
      processedCount = messageCount,
      lastBatchCount = 0,
      lastBatchTimestamp = timestamp,
      lastMessageIds = Seq.empty
    )
  }
}
