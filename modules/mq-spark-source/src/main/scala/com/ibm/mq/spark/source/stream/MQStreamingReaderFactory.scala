package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.core.client.DefaultMQClient
import com.ibm.mq.spark.source.batch.MQJmsTransport
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType

/**
 * Factory for creating streaming partition readers.
 */
case class MQStreamingReaderFactory(
    schema: StructType
) extends PartitionReaderFactory with Serializable {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val streamPartition = partition.asInstanceOf[MQStreamingPartition]
    new MQStreamingPartitionReader(
      streamPartition,
      schema,
      () => new DefaultMQClient(new MQJmsTransport(streamPartition.options))
    )
  }
}
