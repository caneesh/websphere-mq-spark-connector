package com.ibm.mq.spark.core.audit

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AuditEventSpec extends AnyFlatSpec with Matchers {

  "AuditEvent" should "capture message processing events" in {
    val event = AuditEvent(
      eventType = AuditEventType.MessageReceived,
      timestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      queueManager = "QM1",
      messageId = Some("abc123"),
      correlationId = None,
      sourceId = "connector-1",
      details = Map("payloadSize" -> "1024")
    )

    event.eventType shouldBe AuditEventType.MessageReceived
    event.queueName shouldBe "TEST.QUEUE"
    event.details("payloadSize") shouldBe "1024"
  }

  it should "support various event types" in {
    AuditEventType.MessageReceived.name shouldBe "MESSAGE_RECEIVED"
    AuditEventType.MessageCommitted.name shouldBe "MESSAGE_COMMITTED"
    AuditEventType.MessageRolledBack.name shouldBe "MESSAGE_ROLLED_BACK"
    AuditEventType.MessageQuarantined.name shouldBe "MESSAGE_QUARANTINED"
    AuditEventType.ParseError.name shouldBe "PARSE_ERROR"
    AuditEventType.ConnectionEstablished.name shouldBe "CONNECTION_ESTABLISHED"
    AuditEventType.ConnectionLost.name shouldBe "CONNECTION_LOST"
    AuditEventType.CheckpointSaved.name shouldBe "CHECKPOINT_SAVED"
  }

  it should "redact sensitive information" in {
    val event = AuditEvent(
      eventType = AuditEventType.ConnectionEstablished,
      timestamp = System.currentTimeMillis(),
      queueName = "Q1",
      queueManager = "QM1",
      messageId = None,
      correlationId = None,
      sourceId = "test",
      details = Map(
        "user" -> "admin",
        "password" -> "secret123",
        "sslKeyStorePassword" -> "keypass"
      )
    )

    val redacted = event.redacted

    redacted.details("user") shouldBe "admin"
    redacted.details("password") shouldBe "[REDACTED]"
    redacted.details("sslKeyStorePassword") shouldBe "[REDACTED]"
  }

  it should "serialize to structured log format" in {
    val event = AuditEvent(
      eventType = AuditEventType.MessageReceived,
      timestamp = 1234567890000L,
      queueName = "Q1",
      queueManager = "QM1",
      messageId = Some("msg1"),
      correlationId = Some("corr1"),
      sourceId = "src1",
      details = Map.empty
    )

    val json = event.toJson

    json should include("MESSAGE_RECEIVED")
    json should include("Q1")
    json should include("msg1")
    json should include("1234567890000")
  }

  it should "support error events with exception details" in {
    val event = AuditEvent.error(
      queueName = "Q1",
      queueManager = "QM1",
      sourceId = "test",
      error = new RuntimeException("Connection failed"),
      messageId = Some("abc")
    )

    event.eventType shouldBe AuditEventType.Error
    event.details should contain key "errorMessage"
    event.details("errorMessage") should include("Connection failed")
  }
}
