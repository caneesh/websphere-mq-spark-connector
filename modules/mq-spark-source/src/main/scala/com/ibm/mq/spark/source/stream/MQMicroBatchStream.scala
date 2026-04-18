package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit}
import org.apache.spark.sql.types.StructType

import java.util.concurrent.atomic.AtomicLong

/**
 * Spark Structured Streaming micro-batch source for MQ.
 */
class MQMicroBatchStream(
    options: MQSourceOptions,
    schema: StructType
) extends MicroBatchStream {

  private val messageCounter = new AtomicLong(0)
  @volatile private var lastCommittedOffset: MQOffset = MQOffset.Initial

  override def initialOffset(): Offset = {
    MQOffset.Initial
  }

  override def latestOffset(): Offset = {
    val count = messageCounter.get()
    val ts = System.currentTimeMillis()
    MQOffset(count + options.batchSize, ts)
  }

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val startOffset = start.asInstanceOf[MQOffset]
    val endOffset = end.asInstanceOf[MQOffset]

    Array(MQStreamingPartition(0, startOffset, endOffset, options))
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
    messageCounter.set(offset.messageCount)
  }

  override def stop(): Unit = {
  }
}
