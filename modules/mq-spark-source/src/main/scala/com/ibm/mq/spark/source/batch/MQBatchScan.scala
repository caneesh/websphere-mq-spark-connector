package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

/**
 * Spark DataSource V2 Scan implementation for batch MQ reading.
 */
case class MQBatchScan(
    options: MQSourceOptions,
    schema: StructType
) extends Scan with Batch {

  override def readSchema(): StructType = schema

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    Array(MQInputPartition(0, options))
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    MQPartitionReaderFactory(schema)
  }

  override def description(): String = {
    s"MQBatchScan[queue=${options.queueName}, batchSize=${options.batchSize}]"
  }
}
