package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.source.MQSourceOptions
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.types.StructType

/**
 * Builder for creating MQ batch scans.
 */
case class MQScanBuilder(
    options: MQSourceOptions,
    schema: StructType
) extends ScanBuilder {

  override def build(): Scan = {
    MQBatchScan(options, schema)
  }
}
