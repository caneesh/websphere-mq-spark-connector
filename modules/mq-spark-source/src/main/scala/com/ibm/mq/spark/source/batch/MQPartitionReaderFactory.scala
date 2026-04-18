package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.core.client.DefaultMQClient
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * Factory for creating partition readers for MQ batch reads.
 */
case class MQPartitionReaderFactory(
    schema: StructType
) extends PartitionReaderFactory with Serializable {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val mqPartition = partition.asInstanceOf[MQInputPartition]
    new MQPartitionReader(
      mqPartition,
      schema,
      () => new DefaultMQClient(new MQJmsTransport(mqPartition.options))
    )
  }
}
