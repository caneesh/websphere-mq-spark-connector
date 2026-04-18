package com.ibm.mq.spark.observability

import com.ibm.mq.spark.core.audit.{AuditEvent, AuditEventType}
import com.ibm.mq.spark.core.interfaces.{AuditSink, ConnectorMetrics}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.io.{ByteArrayOutputStream, PrintStream}

class MetricsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  "DefaultConnectorMetrics" should "track messages received" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordMessageReceived("TEST.QUEUE", 1024)
    metrics.recordMessageReceived("TEST.QUEUE", 2048)

    metrics.getMessagesReceived("TEST.QUEUE") shouldBe 2
    metrics.getBytesReceived("TEST.QUEUE") shouldBe 3072
  }

  it should "track per-queue metrics separately" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordMessageReceived("QUEUE.A", 100)
    metrics.recordMessageReceived("QUEUE.B", 200)
    metrics.recordMessageReceived("QUEUE.B", 300)

    metrics.getMessagesReceived("QUEUE.A") shouldBe 1
    metrics.getMessagesReceived("QUEUE.B") shouldBe 2
    metrics.getBytesReceived("QUEUE.A") shouldBe 100
    metrics.getBytesReceived("QUEUE.B") shouldBe 500
  }

  it should "track commits and rollbacks" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordMessageCommitted("TEST.QUEUE")
    metrics.recordMessageCommitted("TEST.QUEUE")
    metrics.recordMessageRolledBack("TEST.QUEUE")

    metrics.getCommittedCount("TEST.QUEUE") shouldBe 2
    metrics.getRolledBackCount("TEST.QUEUE") shouldBe 1
  }

  it should "track parse successes and failures" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordParseSuccess("TEST.QUEUE", "JSON", 1000000)
    metrics.recordParseSuccess("TEST.QUEUE", "JSON", 2000000)
    metrics.recordParseFailure("TEST.QUEUE", "XML")

    metrics.getParseSuccessCount("TEST.QUEUE") shouldBe 2
    metrics.getParseFailureCount("TEST.QUEUE") shouldBe 1
  }

  it should "track retry attempts" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordRetry("TEST.QUEUE", 1)
    metrics.recordRetry("TEST.QUEUE", 2)
    metrics.recordRetry("TEST.QUEUE", 3)

    metrics.getRetryCount("TEST.QUEUE") shouldBe 3
  }

  it should "track connection attempts" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordConnectionAttempt("QM1", success = true)
    metrics.recordConnectionAttempt("QM1", success = true)
    metrics.recordConnectionAttempt("QM1", success = false)

    metrics.getConnectionSuccessCount("QM1") shouldBe 2
    metrics.getConnectionFailureCount("QM1") shouldBe 1
  }

  it should "track poll durations" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordPollDuration("TEST.QUEUE", 100, 10)
    metrics.recordPollDuration("TEST.QUEUE", 200, 20)

    metrics.getPollCount("TEST.QUEUE") shouldBe 2
    metrics.getTotalPollDuration("TEST.QUEUE") shouldBe 300
  }

  it should "calculate throughput" in {
    val metrics = DefaultConnectorMetrics()
    val startTime = System.currentTimeMillis()

    metrics.recordMessageReceived("TEST.QUEUE", 1024)
    metrics.recordMessageReceived("TEST.QUEUE", 1024)
    metrics.recordMessageReceived("TEST.QUEUE", 1024)

    Thread.sleep(100)

    val throughput = metrics.getMessageThroughput("TEST.QUEUE")
    throughput should be > 0.0
  }

  it should "export Prometheus-friendly format" in {
    val metrics = DefaultConnectorMetrics()

    metrics.recordMessageReceived("TEST.QUEUE", 1024)
    metrics.recordParseSuccess("TEST.QUEUE", "JSON", 1000000)

    val prometheus = metrics.toPrometheusFormat()

    prometheus should include ("mq_messages_received")
    prometheus should include ("mq_bytes_received")
    prometheus should include ("mq_parse_success")
  }

  "ConsoleAuditSink" should "emit events to console" in {
    val baos = new ByteArrayOutputStream()
    val sink = ConsoleAuditSink(new PrintStream(baos))

    val event = AuditEvent(
      eventType = AuditEventType.MessageReceived,
      timestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      messageId = Some("aabb0001"),
      correlationId = None,
      sourceId = "test",
      details = Map("size" -> "1024")
    )

    sink.emit(event)
    sink.flush()

    val output = baos.toString
    output should include ("MessageReceived")
    output should include ("TEST.QUEUE")
  }

  "StructuredLogAuditSink" should "emit JSON-formatted events" in {
    val baos = new ByteArrayOutputStream()
    val sink = StructuredLogAuditSink(new PrintStream(baos))

    val event = AuditEvent(
      eventType = AuditEventType.MessageCommitted,
      timestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      messageId = Some("aabb0001"),
      correlationId = None,
      sourceId = "test",
      details = Map.empty
    )

    sink.emit(event)
    sink.flush()

    val output = baos.toString
    output should include ("\"eventType\"")
    output should include ("\"queueName\"")
  }

  "InMemoryAuditSink" should "collect events for testing" in {
    val sink = InMemoryAuditSink()

    sink.emit(createEvent(AuditEventType.MessageReceived))
    sink.emit(createEvent(AuditEventType.MessageCommitted))

    sink.events should have size 2
    sink.events.head.eventType shouldBe AuditEventType.MessageReceived
  }

  it should "filter by event type" in {
    val sink = InMemoryAuditSink()

    sink.emit(createEvent(AuditEventType.MessageReceived))
    sink.emit(createEvent(AuditEventType.MessageCommitted))
    sink.emit(createEvent(AuditEventType.ParseError))

    sink.eventsOfType(AuditEventType.ParseError) should have size 1
  }

  private def createEvent(eventType: AuditEventType): AuditEvent = {
    AuditEvent(
      eventType = eventType,
      timestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      messageId = Some("aabb0001"),
      correlationId = None,
      sourceId = "test",
      details = Map.empty
    )
  }
}
