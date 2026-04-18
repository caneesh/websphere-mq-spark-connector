package com.ibm.mq.spark.source

import com.ibm.mq.spark.source.batch.MQScanBuilder
import com.ibm.mq.spark.source.schema.MQSchemaProvider
import com.ibm.mq.spark.source.stream.MQMicroBatchStream
import org.apache.spark.sql.connector.catalog.{SupportsRead, Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._

/**
 * Apache Spark DataSource V2 implementation for IBM MQ.
 *
 * Supported Capabilities:
 * =======================
 * - BATCH_READ: Read messages as a batch DataFrame
 * - MICRO_BATCH_READ: Read messages as a streaming DataFrame
 *
 * Usage Examples:
 * ---------------
 * Batch Read:
 *   spark.read
 *     .format("ibm-mq")
 *     .option("queueManager", "QM1")
 *     .option("channel", "DEV.APP.SVRCONN")
 *     .option("connectionName", "localhost(1414)")
 *     .option("queueName", "DEV.QUEUE.1")
 *     .load()
 *
 * Streaming Read:
 *   spark.readStream
 *     .format("ibm-mq")
 *     .option("queueManager", "QM1")
 *     .option("channel", "DEV.APP.SVRCONN")
 *     .option("connectionName", "localhost(1414)")
 *     .option("queueName", "DEV.QUEUE.1")
 *     .load()
 *     .writeStream
 *     .outputMode("append")
 *     .format("console")
 *     .start()
 *
 * Required Options:
 * - queueManager: MQ queue manager name
 * - channel: MQ channel name
 * - connectionName: Host and port in format "host(port)"
 * - queueName: Target queue name
 *
 * Optional Options:
 * - user: Authentication username
 * - password: Authentication password
 * - sslCipherSuite: SSL cipher suite
 * - batchSize: Messages per batch (default: 1000)
 * - pollTimeoutMs: Poll timeout in milliseconds (default: 5000)
 *
 * Delivery Semantics:
 * - At-least-once for both batch and streaming
 * - For effectively-once: use message_id for downstream deduplication
 */
class MQDataSource extends TableProvider with DataSourceRegister {

  private val log = LoggerFactory.getLogger(getClass)

  override def shortName(): String = "ibm-mq"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    MQSchemaProvider.canonicalSchema
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): Table = {
    log.debug(s"Creating MQ table with ${properties.size()} properties")
    new MQTable(schema, properties)
  }

  override def supportsExternalMetadata(): Boolean = true
}

object MQDataSource {

  val defaultSchema: StructType = MQSchemaProvider.canonicalSchema
}

/**
 * Spark Table implementation for IBM MQ.
 *
 * Implements SupportsRead to provide both batch and micro-batch reading.
 */
private[source] class MQTable(
    tableSchema: StructType,
    properties: util.Map[String, String]
) extends Table with SupportsRead {

  private val log = LoggerFactory.getLogger(getClass)

  private lazy val sourceOptions: MQSourceOptions = {
    MQSourceOptions.fromMap(properties.asScala.toMap)
  }

  override def name(): String = s"MQTable(${sourceOptions.queueName})"

  override def schema(): StructType = tableSchema

  override def capabilities(): util.Set[TableCapability] = {
    util.EnumSet.of(
      TableCapability.BATCH_READ,
      TableCapability.MICRO_BATCH_READ
    )
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    val mergedOptions = mergeOptions(properties.asScala.toMap, options.asScala.toMap)
    val parsedOptions = MQSourceOptions.fromMap(mergedOptions)
    validateOptions(parsedOptions)

    log.info(s"Creating scan builder for queue ${parsedOptions.queueName}")
    new MQScanBuilderWithStreaming(parsedOptions, tableSchema)
  }

  private def mergeOptions(
      tableOptions: Map[String, String],
      scanOptions: Map[String, String]
  ): Map[String, String] = {
    tableOptions ++ scanOptions
  }

  private def validateOptions(options: MQSourceOptions): Unit = {
    val errors = List.newBuilder[String]

    if (options.queueManager.isEmpty) errors += "queueManager is required"
    if (options.channel.isEmpty) errors += "channel is required"
    if (options.connectionName.isEmpty) errors += "connectionName is required"
    if (options.queueName.isEmpty) errors += "queueName is required"
    if (options.batchSize <= 0) errors += "batchSize must be positive"
    if (options.pollTimeoutMs <= 0) errors += "pollTimeoutMs must be positive"

    val errorList = errors.result()
    if (errorList.nonEmpty) {
      throw new IllegalArgumentException(s"Invalid MQ source options: ${errorList.mkString(", ")}")
    }
  }
}

/**
 * Scan builder that supports both batch and streaming scans.
 */
private[source] class MQScanBuilderWithStreaming(
    options: MQSourceOptions,
    schema: StructType
) extends ScanBuilder {

  override def build(): Scan = {
    new MQUnifiedScan(options, schema)
  }
}

/**
 * Unified scan that provides both batch and streaming capabilities.
 *
 * Implements Batch for batch reads and provides MicroBatchStream for streaming.
 */
private[source] class MQUnifiedScan(
    options: MQSourceOptions,
    tableSchema: StructType
) extends Scan with org.apache.spark.sql.connector.read.Batch {

  import com.ibm.mq.spark.source.batch._

  private val log = LoggerFactory.getLogger(getClass)

  override def readSchema(): StructType = tableSchema

  override def toBatch: org.apache.spark.sql.connector.read.Batch = this

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    log.info(s"Creating micro-batch stream for ${options.queueName}, checkpoint at $checkpointLocation")
    new MQMicroBatchStream(options, tableSchema)
  }

  override def planInputPartitions(): Array[org.apache.spark.sql.connector.read.InputPartition] = {
    log.info(s"Planning batch input partitions for ${options.queueName}")
    Array(MQInputPartition(0, options))
  }

  override def createReaderFactory(): org.apache.spark.sql.connector.read.PartitionReaderFactory = {
    MQPartitionReaderFactory(tableSchema)
  }
}
