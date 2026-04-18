package com.ibm.mq.spark.core.checkpoint

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ConnectorCheckpointSpec extends AnyFlatSpec with Matchers {

  "ConnectorCheckpoint" should "store queue position information" in {
    val checkpoint = ConnectorCheckpoint(
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      lastMessageId = Some("414d51205465737451756575652020202020"),
      lastCorrelationId = None,
      lastProcessedTimestamp = 1234567890L,
      messagesProcessed = 1000L,
      bytesProcessed = 500000L,
      partitionId = 0,
      createdAt = System.currentTimeMillis()
    )

    checkpoint.queueName shouldBe "TEST.QUEUE"
    checkpoint.messagesProcessed shouldBe 1000L
    checkpoint.bytesProcessed shouldBe 500000L
  }

  it should "serialize to JSON" in {
    val checkpoint = ConnectorCheckpoint(
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      lastMessageId = Some("abc123"),
      lastCorrelationId = None,
      lastProcessedTimestamp = 1234567890L,
      messagesProcessed = 100L,
      bytesProcessed = 5000L,
      partitionId = 0,
      createdAt = 1234567890L
    )

    val json = checkpoint.toJson
    json should include("TEST.QUEUE")
    json should include("abc123")
    json should include("1234567890")
  }

  it should "deserialize from JSON" in {
    val json = """{"queueName":"Q1","queueManager":"QM1","lastMessageId":"xyz","lastCorrelationId":null,"lastProcessedTimestamp":999,"messagesProcessed":50,"bytesProcessed":1000,"partitionId":1,"createdAt":888}"""

    val checkpoint = ConnectorCheckpoint.fromJson(json)

    checkpoint.queueName shouldBe "Q1"
    checkpoint.lastMessageId shouldBe Some("xyz")
    checkpoint.messagesProcessed shouldBe 50L
    checkpoint.partitionId shouldBe 1
  }

  it should "support merging checkpoints" in {
    val older = ConnectorCheckpoint(
      queueName = "Q1",
      queueManager = "QM1",
      lastMessageId = Some("old"),
      lastCorrelationId = None,
      lastProcessedTimestamp = 1000L,
      messagesProcessed = 100L,
      bytesProcessed = 5000L,
      partitionId = 0,
      createdAt = 900L
    )

    val newer = older.copy(
      lastMessageId = Some("new"),
      lastProcessedTimestamp = 2000L,
      messagesProcessed = 200L,
      bytesProcessed = 10000L,
      createdAt = 1900L
    )

    val merged = older.merge(newer)

    merged.lastMessageId shouldBe Some("new")
    merged.lastProcessedTimestamp shouldBe 2000L
    merged.messagesProcessed shouldBe 200L
  }

  it should "validate queue name is not empty" in {
    val checkpoint = ConnectorCheckpoint(
      queueName = "",
      queueManager = "QM1",
      lastMessageId = None,
      lastCorrelationId = None,
      lastProcessedTimestamp = 0L,
      messagesProcessed = 0L,
      bytesProcessed = 0L,
      partitionId = 0,
      createdAt = 0L
    )

    checkpoint.validate() should contain("queueName is required")
  }
}
