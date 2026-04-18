package com.ibm.mq.spark.core.message

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MQMessageEnvelopeSpec extends AnyFlatSpec with Matchers {

  private def createRawMessage(payload: String = "test"): RawMQMessage = {
    RawMQMessage(
      messageId = Array(1, 2, 3),
      correlationId = Array(4, 5, 6),
      payload = payload.getBytes("UTF-8"),
      putTimestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = 0
    )
  }

  "MQMessageEnvelope" should "wrap a raw message with processing metadata" in {
    val raw = createRawMessage()
    val receivedAt = System.currentTimeMillis()

    val envelope = MQMessageEnvelope(
      raw = raw,
      receivedAt = receivedAt,
      partitionId = 0,
      offset = 100L,
      sourceId = "connector-1"
    )

    envelope.raw shouldBe raw
    envelope.receivedAt shouldBe receivedAt
    envelope.partitionId shouldBe 0
    envelope.offset shouldBe 100L
    envelope.sourceId shouldBe "connector-1"
  }

  it should "calculate processing latency" in {
    val putTime = System.currentTimeMillis() - 1000L
    val raw = createRawMessage().copy(putTimestamp = putTime)
    val receivedAt = System.currentTimeMillis()

    val envelope = MQMessageEnvelope(
      raw = raw,
      receivedAt = receivedAt,
      partitionId = 0,
      offset = 0L,
      sourceId = "test"
    )

    envelope.processingLatencyMs should be >= 1000L
  }

  it should "delegate to raw message for payload access" in {
    val raw = createRawMessage("Hello Envelope")

    val envelope = MQMessageEnvelope(
      raw = raw,
      receivedAt = System.currentTimeMillis(),
      partitionId = 0,
      offset = 0L,
      sourceId = "test"
    )

    envelope.payloadAsString shouldBe "Hello Envelope"
    envelope.messageIdHex shouldBe raw.messageIdHex
  }

  it should "support headers map" in {
    val raw = createRawMessage()
    val envelope = MQMessageEnvelope(
      raw = raw,
      receivedAt = System.currentTimeMillis(),
      partitionId = 0,
      offset = 0L,
      sourceId = "test",
      headers = Map("custom.header" -> "value1")
    )

    envelope.headers shouldBe Map("custom.header" -> "value1")
  }

  it should "be immutable" in {
    val raw = createRawMessage()
    val envelope = MQMessageEnvelope(
      raw = raw,
      receivedAt = System.currentTimeMillis(),
      partitionId = 0,
      offset = 0L,
      sourceId = "test"
    )

    val updated = envelope.copy(offset = 200L)

    envelope.offset shouldBe 0L
    updated.offset shouldBe 200L
  }
}
