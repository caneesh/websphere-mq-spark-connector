package com.ibm.mq.spark.source.schema

import com.ibm.mq.spark.core.message.RawMQMessage
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class MQSchemaSpec extends AnyFlatSpec with Matchers {

  "MQSchemaProvider" should "provide canonical message schema" in {
    val schema = MQSchemaProvider.canonicalSchema

    schema.fieldNames should contain ("messageId")
    schema.fieldNames should contain ("correlationId")
    schema.fieldNames should contain ("payload")
    schema.fieldNames should contain ("timestamp")
  }

  it should "include all MQ metadata fields" in {
    val schema = MQSchemaProvider.canonicalSchema

    schema.fieldNames should contain ("queueName")
    schema.fieldNames should contain ("ccsid")
    schema.fieldNames should contain ("encoding")
    schema.fieldNames should contain ("priority")
    schema.fieldNames should contain ("expiry")
    schema.fieldNames should contain ("backoutCount")
    schema.fieldNames should contain ("format")
  }

  it should "include optional reply-to fields" in {
    val schema = MQSchemaProvider.canonicalSchema

    schema.fieldNames should contain ("replyToQueue")
    schema.fieldNames should contain ("replyToQueueManager")
  }

  it should "mark payload as non-nullable" in {
    val schema = MQSchemaProvider.canonicalSchema
    val payloadField = schema.fields.find(_.name == "payload").get

    payloadField.nullable shouldBe false
    payloadField.dataType shouldBe BinaryType
  }

  it should "support payloadString convenience field" in {
    val schema = MQSchemaProvider.canonicalSchema
    val payloadStringField = schema.fields.find(_.name == "payloadString")

    payloadStringField shouldBe defined
    payloadStringField.get.dataType shouldBe StringType
    payloadStringField.get.nullable shouldBe true
  }

  it should "provide minimal schema option" in {
    val minimal = MQSchemaProvider.minimalSchema

    minimal.fieldNames should contain only ("messageId", "payload", "timestamp")
  }

  it should "allow custom schema builder" in {
    val custom = MQSchemaProvider.builder()
      .withMessageId()
      .withPayload()
      .withField("customField", StringType, nullable = true)
      .build()

    custom.fieldNames should contain ("messageId")
    custom.fieldNames should contain ("payload")
    custom.fieldNames should contain ("customField")
    custom.fieldNames should not contain ("correlationId")
  }

  "MQRowConverter" should "convert RawMQMessage to Spark Row" in {
    val message = createTestMessage()
    val converter = MQRowConverter(MQSchemaProvider.canonicalSchema)

    val row = converter.toRow(message)

    row should not be null
    row.length shouldBe MQSchemaProvider.canonicalSchema.length
  }

  it should "preserve message ID bytes" in {
    val msgId = Array[Byte](0xaa.toByte, 0xbb.toByte, 0x01)
    val message = createTestMessage(messageId = msgId)
    val converter = MQRowConverter(MQSchemaProvider.canonicalSchema)

    val row = converter.toRow(message)
    val rowMsgId = row.getAs[Array[Byte]]("messageId")

    rowMsgId shouldBe msgId
  }

  it should "convert timestamp to SQL Timestamp" in {
    val epochMs = 1700000000000L
    val message = createTestMessage(putTimestamp = epochMs)
    val converter = MQRowConverter(MQSchemaProvider.canonicalSchema)

    val row = converter.toRow(message)
    val ts = row.getAs[Timestamp]("timestamp")

    ts.getTime shouldBe epochMs
  }

  it should "handle null optional fields" in {
    val message = createTestMessage(replyToQueue = None)
    val converter = MQRowConverter(MQSchemaProvider.canonicalSchema)

    val row = converter.toRow(message)

    row.getAs[String]("replyToQueue") shouldBe null
  }

  it should "decode payload as string using CCSID" in {
    val payload = "Hello MQ".getBytes("UTF-8")
    val message = createTestMessage(payload = payload, ccsid = 1208)
    val converter = MQRowConverter(MQSchemaProvider.canonicalSchema)

    val row = converter.toRow(message)
    val payloadString = row.getAs[String]("payloadString")

    payloadString shouldBe "Hello MQ"
  }

  it should "convert to minimal row when using minimal schema" in {
    val message = createTestMessage()
    val converter = MQRowConverter(MQSchemaProvider.minimalSchema)

    val row = converter.toRow(message)

    row.length shouldBe 3
    row.schema.fieldNames should contain only ("messageId", "payload", "timestamp")
  }

  it should "handle batch conversion" in {
    val messages = Seq(
      createTestMessage(messageId = Array(1)),
      createTestMessage(messageId = Array(2)),
      createTestMessage(messageId = Array(3))
    )
    val converter = MQRowConverter(MQSchemaProvider.canonicalSchema)

    val rows = converter.toRows(messages)

    rows should have size 3
  }

  private def createTestMessage(
      messageId: Array[Byte] = Array(0xaa.toByte, 0xbb.toByte),
      correlationId: Array[Byte] = Array.emptyByteArray,
      payload: Array[Byte] = "test payload".getBytes("UTF-8"),
      putTimestamp: Long = System.currentTimeMillis(),
      queueName: String = "TEST.QUEUE",
      ccsid: Int = 1208,
      replyToQueue: Option[String] = None
  ): RawMQMessage = {
    RawMQMessage(
      messageId = messageId,
      correlationId = correlationId,
      payload = payload,
      putTimestamp = putTimestamp,
      queueName = queueName,
      ccsid = ccsid,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = 0,
      replyToQueue = replyToQueue,
      replyToQueueManager = None,
      format = "MQSTR"
    )
  }
}
