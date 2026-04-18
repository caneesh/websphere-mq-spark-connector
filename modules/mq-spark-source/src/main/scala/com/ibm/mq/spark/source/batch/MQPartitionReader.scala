package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.core.checkpoint.ConnectorCheckpoint
import com.ibm.mq.spark.core.client.DefaultMQClient
import com.ibm.mq.spark.core.interfaces.CheckpointStore
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.source.schema.MQRowConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Reads MQ messages for a single partition and converts them to Spark rows.
 *
 * Transaction Boundary Design:
 * ----------------------------
 * This reader implements a "read-all-then-commit" pattern for batch processing:
 *
 * 1. Messages are read under MQ syncpoint (transactional get)
 * 2. No commit happens during iteration - messages remain uncommitted
 * 3. Commit occurs ONLY after ALL messages are successfully read (when iterator exhausted)
 * 4. If any failure occurs during reading, close() will rollback
 * 5. close() only releases resources - it does NOT commit
 *
 * Checkpoint Integration:
 * -----------------------
 * When a checkpoint store is provided:
 * 1. After successful MQ commit, checkpoint is saved
 * 2. Checkpoint includes: last message ID, messages processed, bytes processed
 * 3. On restart, application can load checkpoint for progress tracking
 *
 * IMPORTANT: Checkpoint is saved AFTER MQ commit, not before.
 * This creates a window where messages are committed but checkpoint is not saved.
 * On failure in this window: messages lost (committed) but checkpoint stale.
 * This is the best achievable with non-2PC storage.
 *
 * For tighter coordination, use a transactional checkpoint store (e.g., database)
 * that can participate in distributed transactions.
 *
 * Delivery Semantics:
 * - At-least-once: If Spark fails after our commit but before writing output,
 *   messages will be reprocessed on retry
 * - NOT exactly-once: MQ does not provide offset-based replay
 *
 * Failure Scenarios:
 * - Reader fails during iteration → rollback, messages redelivered
 * - Reader completes but Spark task fails → messages committed, may duplicate
 * - Spark job fails after some partitions commit → partial commit, some messages lost
 *
 * Limitations:
 * - Commit boundary is per-partition, not per-job
 * - No cross-partition transaction coordination
 * - Large batches increase commit latency and memory pressure
 */
class MQPartitionReader(
    partition: MQInputPartition,
    schema: StructType,
    clientFactory: () => DefaultMQClient,
    checkpointStore: Option[CheckpointStore] = None
) extends PartitionReader[InternalRow] {

  private val log = LoggerFactory.getLogger(getClass)

  private val options = partition.options
  private val client: DefaultMQClient = clientFactory()
  private val rowConverter = MQRowConverter(schema)

  private var messages: Iterator[RawMQMessage] = Iterator.empty
  private var currentRow: InternalRow = _
  private var initialized = false
  private var messagesRead = 0
  private var bytesRead = 0L
  private var lastMessageId: Option[String] = None
  private var lastCorrelationId: Option[String] = None
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

      if (messagesRead >= options.batchSize) {
        markReadingComplete()
        return false
      }

      if (messages.hasNext) {
        processMessage(messages.next())
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
   * IMPORTANT: This method does NOT commit the MQ transaction automatically.
   * - If reading completed successfully, commit was done in markReadingComplete()
   * - If reading failed or was interrupted, rollback happens here
   */
  override def close(): Unit = {
    if (!closed) {
      try {
        if (readingFailed || !readingComplete) {
          log.warn(s"Reader closing without successful completion (failed=$readingFailed, complete=$readingComplete). Rolling back $messagesRead uncommitted messages.")
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
    if (messagesRead >= options.batchSize) {
      return false
    }

    val remaining = options.batchSize - messagesRead
    val polled = client.poll(remaining, options.pollTimeoutMs)

    if (polled.nonEmpty) {
      messages = polled.iterator
      processMessage(messages.next())
      true
    } else {
      false
    }
  }

  private def processMessage(msg: RawMQMessage): Unit = {
    val row = rowConverter.toRow(msg)
    currentRow = toInternalRow(row)
    messagesRead += 1
    bytesRead += msg.payload.length
    lastMessageId = Some(msg.messageIdHex)
    if (msg.correlationIdHex.nonEmpty) {
      lastCorrelationId = Some(msg.correlationIdHex)
    }
  }

  /**
   * Called when all messages have been successfully read.
   * This is the safe point to commit the MQ transaction and save checkpoint.
   */
  private def markReadingComplete(): Unit = {
    if (!readingComplete && !readingFailed) {
      readingComplete = true
      log.info(s"Successfully read $messagesRead messages ($bytesRead bytes). Committing MQ transaction.")
      client.commit()

      saveCheckpoint()
    }
  }

  /**
   * Saves checkpoint after successful commit.
   *
   * Note: If this fails, the MQ messages are already committed.
   * The checkpoint will be stale until the next successful batch.
   */
  private def saveCheckpoint(): Unit = {
    checkpointStore.foreach { store =>
      val checkpoint = ConnectorCheckpoint(
        queueName = options.queueName,
        queueManager = options.queueManager,
        lastMessageId = lastMessageId,
        lastCorrelationId = lastCorrelationId,
        lastProcessedTimestamp = System.currentTimeMillis(),
        messagesProcessed = messagesRead.toLong,
        bytesProcessed = bytesRead,
        partitionId = partition.partitionId,
        createdAt = System.currentTimeMillis()
      )

      store.save(checkpoint) match {
        case scala.util.Success(_) =>
          log.debug(s"Checkpoint saved: partition=${partition.partitionId}, messages=$messagesRead")
        case scala.util.Failure(e) =>
          log.warn(s"Failed to save checkpoint: ${e.getMessage}. MQ messages already committed.")
      }
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
