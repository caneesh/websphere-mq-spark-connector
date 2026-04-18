package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.core.client.DefaultMQClient
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.source.schema.MQRowConverter
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

/**
 * Reads MQ messages for a single partition and converts them to Spark rows.
 */
class MQPartitionReader(
    partition: MQInputPartition,
    schema: StructType,
    clientFactory: () => DefaultMQClient
) extends PartitionReader[InternalRow] {

  private val options = partition.options
  private val client: DefaultMQClient = clientFactory()
  private val rowConverter = MQRowConverter(schema)

  private var messages: Iterator[RawMQMessage] = Iterator.empty
  private var currentRow: InternalRow = _
  private var initialized = false
  private var messagesRead = 0
  private var closed = false

  override def next(): Boolean = {
    if (closed) return false

    if (!initialized) {
      initialize()
      initialized = true
      return currentRow != null
    }

    if (messagesRead >= options.batchSize) {
      return false
    }

    if (messages.hasNext) {
      val msg = messages.next()
      val row = rowConverter.toRow(msg)
      currentRow = toInternalRow(row)
      messagesRead += 1
      true
    } else {
      fetchMore()
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    if (!closed) {
      try {
        client.commit()
      } finally {
        client.close()
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
      val msg = messages.next()
      val row = rowConverter.toRow(msg)
      currentRow = toInternalRow(row)
      messagesRead += 1
      true
    } else {
      false
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
