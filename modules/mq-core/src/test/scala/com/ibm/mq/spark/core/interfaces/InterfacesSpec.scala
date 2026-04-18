package com.ibm.mq.spark.core.interfaces

import com.ibm.mq.spark.core.audit.{AuditEvent, AuditEventType}
import com.ibm.mq.spark.core.checkpoint.{ConnectorCheckpoint, InMemoryCheckpointStore}
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Failure, Success, Try}

/**
 * Behavior tests for connector interfaces.
 *
 * These tests verify the contractual behavior of interface implementations,
 * not just that they can be implemented.
 */
class InterfacesSpec extends AnyFlatSpec with Matchers {

  private def createRawMessage(payload: String = "test", format: String = ""): RawMQMessage = {
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
      backoutCount = 0,
      format = format
    )
  }

  "PayloadParser contract" should "return success result with parsed data for valid input" in {
    val parser: PayloadParser = new TestTextParser()
    val msg = createRawMessage("hello world")

    val result = parser.parse(msg)

    result.isSuccess shouldBe true
    result.parsedData shouldBe Some("hello world")
    result.format shouldBe PayloadFormat.Text
    result.originalSize shouldBe msg.payloadSize
  }

  it should "return failure result with error for invalid input" in {
    val parser: PayloadParser = new FailingParser()
    val msg = createRawMessage("anything")

    val result = parser.parse(msg)

    result.isSuccess shouldBe false
    result.error shouldBe defined
    result.originalPayload shouldBe defined
  }

  it should "report canParse accurately for its supported format" in {
    val textParser: PayloadParser = new TestTextParser()
    val jsonOnlyParser: PayloadParser = new JsonOnlyParser()

    textParser.canParse(createRawMessage("anything")) shouldBe true
    jsonOnlyParser.canParse(createRawMessage("{}", format = "JSON")) shouldBe true
    jsonOnlyParser.canParse(createRawMessage("plain text", format = "TEXT")) shouldBe false
  }

  it should "preserve original payload on failure for error handling" in {
    val parser: PayloadParser = new FailingParser()
    val payload = "original content"
    val msg = createRawMessage(payload)

    val result = parser.parse(msg)

    result.originalPayload shouldBe Some(payload)
  }

  "CheckpointStore contract" should "save and retrieve checkpoint by queue and partition" in {
    val store = InMemoryCheckpointStore()
    val checkpoint = createCheckpoint("Q1", 0, messagesProcessed = 100)

    store.save(checkpoint).isSuccess shouldBe true

    val loaded = store.load("Q1", 0)
    loaded.isSuccess shouldBe true
    loaded.get shouldBe Some(checkpoint)
  }

  it should "isolate checkpoints by partition" in {
    val store = InMemoryCheckpointStore()

    store.save(createCheckpoint("Q1", 0, messagesProcessed = 100))
    store.save(createCheckpoint("Q1", 1, messagesProcessed = 200))

    store.load("Q1", 0).get.get.messagesProcessed shouldBe 100
    store.load("Q1", 1).get.get.messagesProcessed shouldBe 200
  }

  it should "isolate checkpoints by queue" in {
    val store = InMemoryCheckpointStore()

    store.save(createCheckpoint("Q1", 0, messagesProcessed = 100))
    store.save(createCheckpoint("Q2", 0, messagesProcessed = 200))

    store.load("Q1", 0).get.get.messagesProcessed shouldBe 100
    store.load("Q2", 0).get.get.messagesProcessed shouldBe 200
  }

  it should "return None for non-existent checkpoint" in {
    val store = InMemoryCheckpointStore()

    store.load("NONEXISTENT", 0).get shouldBe None
  }

  it should "update existing checkpoint on re-save" in {
    val store = InMemoryCheckpointStore()

    store.save(createCheckpoint("Q1", 0, messagesProcessed = 100))
    store.save(createCheckpoint("Q1", 0, messagesProcessed = 200))

    store.load("Q1", 0).get.get.messagesProcessed shouldBe 200
  }

  it should "remove checkpoint on delete" in {
    val store = InMemoryCheckpointStore()

    store.save(createCheckpoint("Q1", 0))
    store.delete("Q1", 0)

    store.load("Q1", 0).get shouldBe None
  }

  it should "list all stored checkpoints" in {
    val store = InMemoryCheckpointStore()

    store.save(createCheckpoint("Q1", 0))
    store.save(createCheckpoint("Q1", 1))
    store.save(createCheckpoint("Q2", 0))

    val all = store.list().get
    all should have size 3
    all.map(_.queueName).toSet should contain allOf ("Q1", "Q2")
  }

  "AuditSink contract" should "emit events to consumers" in {
    var capturedEvents: List[AuditEvent] = Nil
    val sink: AuditSink = new TestAuditSink(e => capturedEvents = e :: capturedEvents)

    sink.emit(createAuditEvent(AuditEventType.MessageReceived))
    sink.emit(createAuditEvent(AuditEventType.MessageCommitted))

    capturedEvents should have size 2
    capturedEvents.map(_.eventType) should contain allOf (
      AuditEventType.MessageReceived,
      AuditEventType.MessageCommitted
    )
  }

  it should "preserve event details through emit" in {
    var capturedEvent: AuditEvent = null
    val sink: AuditSink = new TestAuditSink(e => capturedEvent = e)

    val event = AuditEvent(
      eventType = AuditEventType.MessageReceived,
      timestamp = 1234567890L,
      queueName = "MY.QUEUE",
      queueManager = "QM1",
      messageId = Some("msg-123"),
      correlationId = Some("corr-456"),
      sourceId = "partition-0",
      details = Map("key" -> "value")
    )

    sink.emit(event)

    capturedEvent.timestamp shouldBe 1234567890L
    capturedEvent.queueName shouldBe "MY.QUEUE"
    capturedEvent.messageId shouldBe Some("msg-123")
    capturedEvent.details("key") shouldBe "value"
  }

  "ConnectorMetrics contract" should "accumulate message counts accurately" in {
    val metrics = new TestMetrics()

    metrics.recordMessageReceived("Q1", 1024)
    metrics.recordMessageReceived("Q1", 2048)
    metrics.recordMessageReceived("Q1", 512)

    metrics.getMessagesReceived("Q1") shouldBe 3
    metrics.getBytesReceived("Q1") shouldBe 3584
  }

  it should "track counts per queue" in {
    val metrics = new TestMetrics()

    metrics.recordMessageReceived("Q1", 100)
    metrics.recordMessageReceived("Q2", 200)
    metrics.recordMessageReceived("Q1", 100)

    metrics.getMessagesReceived("Q1") shouldBe 2
    metrics.getMessagesReceived("Q2") shouldBe 1
    metrics.getBytesReceived("Q1") shouldBe 200
    metrics.getBytesReceived("Q2") shouldBe 200
  }

  it should "record commit and rollback events" in {
    val metrics = new TestMetrics()

    metrics.recordMessageCommitted("Q1")
    metrics.recordMessageCommitted("Q1")
    metrics.recordMessageRolledBack("Q1")

    metrics.commitCount shouldBe 2
    metrics.rollbackCount shouldBe 1
  }

  it should "track retry attempts" in {
    val metrics = new TestMetrics()

    metrics.recordRetry("Q1", 1)
    metrics.recordRetry("Q1", 2)
    metrics.recordRetry("Q1", 3)

    metrics.retryAttempts should contain allOf (1, 2, 3)
  }

  private def createCheckpoint(
      queueName: String,
      partitionId: Int,
      messagesProcessed: Long = 0
  ): ConnectorCheckpoint = {
    ConnectorCheckpoint(
      queueName = queueName,
      queueManager = "QM1",
      lastMessageId = None,
      lastCorrelationId = None,
      lastProcessedTimestamp = System.currentTimeMillis(),
      messagesProcessed = messagesProcessed,
      bytesProcessed = 0L,
      partitionId = partitionId,
      createdAt = System.currentTimeMillis()
    )
  }

  private def createAuditEvent(eventType: AuditEventType): AuditEvent = {
    AuditEvent(
      eventType = eventType,
      timestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      messageId = None,
      correlationId = None,
      sourceId = "test",
      details = Map.empty
    )
  }
}

class TestTextParser extends PayloadParser {
  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    ParsedPayloadResult.success(
      format = PayloadFormat.Text,
      parsedData = message.payloadAsString,
      originalSize = message.payloadSize
    )
  }
  override def canParse(message: RawMQMessage): Boolean = true
  override def format: PayloadFormat = PayloadFormat.Text
}

class FailingParser extends PayloadParser {
  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    ParsedPayloadResult.failure(
      format = PayloadFormat.Unknown,
      error = "Parse error: invalid format",
      originalPayload = message.payloadAsString,
      originalSize = message.payloadSize
    )
  }
  override def canParse(message: RawMQMessage): Boolean = true
  override def format: PayloadFormat = PayloadFormat.Unknown
}

class JsonOnlyParser extends PayloadParser {
  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    ParsedPayloadResult.success(
      format = PayloadFormat.Json,
      parsedData = Map.empty,
      originalSize = message.payloadSize
    )
  }
  override def canParse(message: RawMQMessage): Boolean = {
    message.format.toUpperCase == "JSON"
  }
  override def format: PayloadFormat = PayloadFormat.Json
}

class TestAuditSink(handler: AuditEvent => Unit) extends AuditSink {
  override def emit(event: AuditEvent): Unit = handler(event)
  override def flush(): Unit = {}
  override def close(): Unit = {}
}

class TestMetrics extends ConnectorMetrics {
  private var _messagesReceived: Map[String, Long] = Map.empty.withDefaultValue(0L)
  private var _bytesReceived: Map[String, Long] = Map.empty.withDefaultValue(0L)
  var commitCount: Int = 0
  var rollbackCount: Int = 0
  var retryAttempts: List[Int] = Nil

  override def recordMessageReceived(queueName: String, bytes: Long): Unit = {
    _messagesReceived = _messagesReceived + (queueName -> (_messagesReceived(queueName) + 1))
    _bytesReceived = _bytesReceived + (queueName -> (_bytesReceived(queueName) + bytes))
  }
  override def recordMessageCommitted(queueName: String): Unit = commitCount += 1
  override def recordMessageRolledBack(queueName: String): Unit = rollbackCount += 1
  override def recordParseSuccess(queueName: String, format: String, durationNanos: Long): Unit = {}
  override def recordParseFailure(queueName: String, format: String): Unit = {}
  override def recordRetry(queueName: String, attempt: Int): Unit = retryAttempts = attempt :: retryAttempts
  override def recordConnectionAttempt(queueManager: String, success: Boolean): Unit = {}
  override def recordPollDuration(queueName: String, durationMs: Long, messageCount: Int): Unit = {}
  override def getMessagesReceived(queueName: String): Long = _messagesReceived(queueName)
  override def getBytesReceived(queueName: String): Long = _bytesReceived(queueName)
}
