package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.core.client.DefaultMQClient
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.source.schema.MQRowConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Reads messages for a streaming micro-batch partition.
 *
 * Transaction Boundary Design for Streaming:
 * ------------------------------------------
 * Unlike batch processing, streaming has a more complex commit model:
 *
 * 1. Messages are read under MQ syncpoint (transactional get)
 * 2. Reader commits when the expected message count is reached
 * 3. Spark then checkpoints the offset via MQMicroBatchStream.commit()
 * 4. If reader fails, messages are rolled back and redelivered
 *
 * Delivery Semantics:
 * - At-least-once: Messages committed to MQ before Spark checkpoint
 * - Window of potential duplicates: Between MQ commit and Spark checkpoint
 * - For effectively-once: Downstream must be idempotent (use message_id for dedup)
 *
 * Failure Recovery:
 * - Reader fails mid-batch → rollback, full micro-batch replays
 * - Reader commits, Spark fails before checkpoint → messages re-read, duplicates
 * - Spark checkpoints successfully → progress is durable
 *
 * Why Commit in Reader (not MicroBatchStream.commit()):
 * - MicroBatchStream.commit() runs on driver, reader runs on executor
 * - No direct communication channel between them
 * - MQ transaction is bound to the connection on the executor
 *
 * Known Limitation:
 * This creates a window where MQ messages are committed but Spark hasn't
 * checkpointed. If Spark fails in this window, messages will be replayed.
 * This is the best achievable with MQ's transactional model.
 */
class MQStreamingPartitionReader(
    partition: MQStreamingPartition,
    schema: StructType,
    clientFactory: () => DefaultMQClient
) extends PartitionReader[InternalRow] {

  private val log = LoggerFactory.getLogger(getClass)

  private val options = partition.options
  private val expectedCount = partition.expectedMessageCount
  private val client: DefaultMQClient = clientFactory()
  private val rowConverter = MQRowConverter(schema)

  private var messages: Iterator[RawMQMessage] = Iterator.empty
  private var currentRow: InternalRow = _
  private var initialized = false
  private var messagesRead = 0L
  private var closed = false
  private var readingComplete = false
  private var readingFailed = false

  override def next(): Boolean = {
    if (closed) return false

    try {
      if (!initialized) {
        initialize()
        initialized = true
        if (currentRow == null) {
          markReadingComplete()
          return false
        }
        return true
      }

      if (messagesRead >= expectedCount) {
        markReadingComplete()
        return false
      }

      if (messages.hasNext) {
        val msg = messages.next()
        val row = rowConverter.toRow(msg)
        currentRow = toInternalRow(row)
        messagesRead += 1
        checkAndCommit()
        true
      } else {
        val hasMore = fetchMore()
        if (!hasMore) {
          markReadingComplete()
        }
        hasMore
      }
    } catch {
      case e: Exception =>
        readingFailed = true
        log.error(s"Error reading messages: ${e.getMessage}", e)
        throw e
    }
  }

  override def get(): InternalRow = currentRow

  /**
   * Closes the reader and releases resources.
   *
   * If reading did not complete successfully, rollback uncommitted messages.
   */
  override def close(): Unit = {
    if (!closed) {
      try {
        if (readingFailed || !readingComplete) {
          log.warn(s"Streaming reader closing without completion (failed=$readingFailed, complete=$readingComplete). Rolling back uncommitted messages.")
          client.rollback()
        }
      } catch {
        case e: Exception =>
          log.warn(s"Error during rollback: ${e.getMessage}")
      } finally {
        try {
          client.close()
        } catch {
          case e: Exception =>
            log.warn(s"Error closing client: ${e.getMessage}")
        }
        closed = true
      }
    }
  }

  private def initialize(): Unit = {
    client.connect(options.toConnectorConfig)
    fetchMore()
  }

  private def fetchMore(): Boolean = {
    if (messagesRead >= expectedCount) {
      return false
    }

    val remaining = math.min(options.batchSize.toLong, expectedCount - messagesRead).toInt
    val polled = client.poll(remaining, options.pollTimeoutMs)

    if (polled.nonEmpty) {
      messages = polled.iterator
      val msg = messages.next()
      val row = rowConverter.toRow(msg)
      currentRow = toInternalRow(row)
      messagesRead += 1
      checkAndCommit()
      true
    } else {
      false
    }
  }

  /**
   * Check if we've read the expected count and commit if so.
   */
  private def checkAndCommit(): Unit = {
    if (messagesRead >= expectedCount && !readingComplete) {
      markReadingComplete()
    }
  }

  /**
   * Mark reading as complete and commit the MQ transaction.
   */
  private def markReadingComplete(): Unit = {
    if (!readingComplete && !readingFailed) {
      readingComplete = true
      log.info(s"Micro-batch complete: read $messagesRead messages. Committing MQ transaction.")
      client.commit()
    }
  }

  private def toInternalRow(row: Row): InternalRow = {
    InternalRow.fromSeq(row.toSeq.map {
      case bytes: Array[Byte] => bytes
      case ts: java.sql.Timestamp => ts.getTime * 1000
      case other => other
    })
  }
}
