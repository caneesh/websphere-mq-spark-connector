package com.ibm.mq.spark.source.schema

import org.apache.spark.sql.types._

/**
 * Provides standardized Spark schemas for MQ message data.
 */
object MQSchemaProvider {

  /**
   * Canonical schema with all MQ message fields.
   *
   * MQ Metadata Available:
   * - messageId: Unique 24-byte identifier (MQMD.MsgId)
   * - correlationId: For request-reply patterns (MQMD.CorrelId)
   * - payload: Message body as raw bytes
   * - payloadString: Decoded payload using CCSID
   * - timestamp: Put timestamp (MQMD.PutDate/PutTime)
   * - queueName: Source queue name
   * - ccsid: Character set ID for payload (MQMD.CodedCharSetId)
   * - encoding: Numeric encoding (MQMD.Encoding)
   * - priority: Message priority 0-9 (MQMD.Priority)
   * - expiry: Expiry in tenths of second, -1=unlimited (MQMD.Expiry)
   * - backoutCount: Redelivery count (MQMD.BackoutCount)
   * - replyToQueue: Reply destination (MQMD.ReplyToQ)
   * - replyToQueueManager: Reply queue manager (MQMD.ReplyToQMgr)
   * - format: Message format descriptor (MQMD.Format)
   * - persistence: 0=not persistent, 1=persistent (MQMD.Persistence)
   * - messageType: Request/reply/datagram/report (MQMD.MsgType)
   * - userId: Putting user ID (MQMD.UserIdentifier)
   * - applicationName: Putting application (MQMD.PutApplName)
   * - putApplicationType: Application type code (MQMD.PutApplType)
   * - groupId: Message group identifier (MQMD.GroupId)
   * - messageSequenceNumber: Sequence within group (MQMD.MsgSeqNumber)
   *
   * Not Available via Standard API:
   * - Original put timestamp with sub-second precision
   * - Queue manager internal tracking fields
   */
  val canonicalSchema: StructType = StructType(Seq(
    StructField("messageId", BinaryType, nullable = false),
    StructField("correlationId", BinaryType, nullable = true),
    StructField("payload", BinaryType, nullable = false),
    StructField("payloadString", StringType, nullable = true),
    StructField("timestamp", TimestampType, nullable = false),
    StructField("queueName", StringType, nullable = false),
    StructField("ccsid", IntegerType, nullable = false),
    StructField("encoding", IntegerType, nullable = false),
    StructField("priority", IntegerType, nullable = false),
    StructField("expiry", IntegerType, nullable = false),
    StructField("backoutCount", IntegerType, nullable = false),
    StructField("replyToQueue", StringType, nullable = true),
    StructField("replyToQueueManager", StringType, nullable = true),
    StructField("format", StringType, nullable = true),
    StructField("persistence", IntegerType, nullable = false),
    StructField("messageType", IntegerType, nullable = false),
    StructField("userId", StringType, nullable = true),
    StructField("applicationName", StringType, nullable = true),
    StructField("putApplicationType", IntegerType, nullable = false),
    StructField("groupId", BinaryType, nullable = true),
    StructField("messageSequenceNumber", IntegerType, nullable = false)
  ))

  /**
   * Minimal schema with only essential fields.
   */
  val minimalSchema: StructType = StructType(Seq(
    StructField("messageId", BinaryType, nullable = false),
    StructField("payload", BinaryType, nullable = false),
    StructField("timestamp", TimestampType, nullable = false)
  ))

  /**
   * Creates a new schema builder for custom schemas.
   */
  def builder(): SchemaBuilder = new SchemaBuilder()
}

/**
 * Builder for constructing custom MQ schemas.
 */
class SchemaBuilder {
  private var fields: Seq[StructField] = Seq.empty

  def withMessageId(): SchemaBuilder = {
    fields = fields :+ StructField("messageId", BinaryType, nullable = false)
    this
  }

  def withCorrelationId(): SchemaBuilder = {
    fields = fields :+ StructField("correlationId", BinaryType, nullable = true)
    this
  }

  def withPayload(): SchemaBuilder = {
    fields = fields :+ StructField("payload", BinaryType, nullable = false)
    this
  }

  def withPayloadString(): SchemaBuilder = {
    fields = fields :+ StructField("payloadString", StringType, nullable = true)
    this
  }

  def withTimestamp(): SchemaBuilder = {
    fields = fields :+ StructField("timestamp", TimestampType, nullable = false)
    this
  }

  def withQueueName(): SchemaBuilder = {
    fields = fields :+ StructField("queueName", StringType, nullable = false)
    this
  }

  def withMetadata(): SchemaBuilder = {
    fields = fields ++ Seq(
      StructField("ccsid", IntegerType, nullable = false),
      StructField("encoding", IntegerType, nullable = false),
      StructField("priority", IntegerType, nullable = false),
      StructField("expiry", IntegerType, nullable = false),
      StructField("backoutCount", IntegerType, nullable = false),
      StructField("format", StringType, nullable = true)
    )
    this
  }

  def withReplyTo(): SchemaBuilder = {
    fields = fields ++ Seq(
      StructField("replyToQueue", StringType, nullable = true),
      StructField("replyToQueueManager", StringType, nullable = true)
    )
    this
  }

  def withField(name: String, dataType: DataType, nullable: Boolean): SchemaBuilder = {
    fields = fields :+ StructField(name, dataType, nullable)
    this
  }

  def build(): StructType = StructType(fields)
}
