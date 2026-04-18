package com.ibm.mq.spark.source

import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util

/**
 * Apache Spark DataSource V2 implementation for IBM MQ.
 *
 * Enables reading messages from IBM MQ queues as a Spark DataFrame.
 * Register this data source using the short name "ibm-mq" or the
 * fully qualified class name.
 */
class MQDataSource extends TableProvider with DataSourceRegister {

  override def shortName(): String = "ibm-mq"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    MQDataSource.defaultSchema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    new MQTable(schema, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}

object MQDataSource {

  import org.apache.spark.sql.types._

  val defaultSchema: StructType = StructType(Seq(
    StructField("messageId", BinaryType, nullable = false),
    StructField("correlationId", BinaryType, nullable = true),
    StructField("payload", BinaryType, nullable = false),
    StructField("payloadString", StringType, nullable = true),
    StructField("timestamp", TimestampType, nullable = false),
    StructField("format", StringType, nullable = true),
    StructField("expiry", IntegerType, nullable = true)
  ))
}

/**
 * Spark Table implementation for IBM MQ.
 */
private[source] class MQTable(
    tableSchema: StructType,
    properties: util.Map[String, String]
) extends Table {

  override def name(): String = "MQTable"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[org.apache.spark.sql.connector.catalog.TableCapability] = {
    util.EnumSet.of(
      org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ
    )
  }
}
