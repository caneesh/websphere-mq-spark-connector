package com.ibm.mq.spark.core.message

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RawMQMessageSpec extends AnyFlatSpec with Matchers {

  "RawMQMessage" should "store all MQ message metadata" in {
    val messageId = Array[Byte](1, 2, 3, 4)
    val correlationId = Array[Byte](5, 6, 7, 8)
    val payload = "test payload".getBytes("UTF-8")

    val msg = RawMQMessage(
      messageId = messageId,
      correlationId = correlationId,
      payload = payload,
      putTimestamp = 1234567890L,
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = 600,
      backoutCount = 0,
      replyToQueue = Some("REPLY.QUEUE"),
      replyToQueueManager = Some("QM1"),
      format = "MQSTR",
      persistence = 1,
      messageType = 8,
      userId = Some("mquser"),
      applicationName = Some("TestApp"),
      putApplicationType = 28,
      groupId = None,
      messageSequenceNumber = 1,
      offset = 0
    )

    msg.queueName shouldBe "TEST.QUEUE"
    msg.ccsid shouldBe 1208
    msg.priority shouldBe 4
    msg.format shouldBe "MQSTR"
    msg.userId shouldBe Some("mquser")
  }

  it should "convert to hex strings" in {
    val msg = RawMQMessage(
      messageId = Array(0x01.toByte, 0x0a.toByte, 0xff.toByte),
      correlationId = Array(0xde.toByte, 0xad.toByte),
      payload = Array.emptyByteArray,
      putTimestamp = 0L,
      queueName = "Q1",
      ccsid = 1208,
      encoding = 546,
      priority = 0,
      expiry = -1,
      backoutCount = 0
    )

    msg.messageIdHex shouldBe "010aff"
    msg.correlationIdHex shouldBe "dead"
  }

  it should "provide payload as string" in {
    val msg = RawMQMessage(
      messageId = Array.emptyByteArray,
      correlationId = Array.emptyByteArray,
      payload = "Hello MQ".getBytes("UTF-8"),
      putTimestamp = 0L,
      queueName = "Q1",
      ccsid = 1208,
      encoding = 546,
      priority = 0,
      expiry = -1,
      backoutCount = 0
    )

    msg.payloadAsString shouldBe "Hello MQ"
  }

  it should "calculate payload size" in {
    val payload = "test".getBytes("UTF-8")
    val msg = RawMQMessage(
      messageId = Array.emptyByteArray,
      correlationId = Array.emptyByteArray,
      payload = payload,
      putTimestamp = 0L,
      queueName = "Q1",
      ccsid = 1208,
      encoding = 546,
      priority = 0,
      expiry = -1,
      backoutCount = 0
    )

    msg.payloadSize shouldBe 4
  }

  it should "detect redelivery based on backout count" in {
    val firstDelivery = RawMQMessage(
      messageId = Array.emptyByteArray,
      correlationId = Array.emptyByteArray,
      payload = Array.emptyByteArray,
      putTimestamp = 0L,
      queueName = "Q1",
      ccsid = 1208,
      encoding = 546,
      priority = 0,
      expiry = -1,
      backoutCount = 0
    )

    val redelivered = firstDelivery.copy(backoutCount = 2)

    firstDelivery.isRedelivered shouldBe false
    redelivered.isRedelivered shouldBe true
  }
}
