package com.ibm.mq.spark.core.interfaces

import com.ibm.mq.spark.core.audit.{AuditEvent, AuditEventType}
import com.ibm.mq.spark.core.checkpoint.ConnectorCheckpoint
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Success, Try}

class InterfacesSpec extends AnyFlatSpec with Matchers {

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

  "PayloadParser" should "be implementable" in {
    val parser: PayloadParser = new PayloadParser {
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

    val msg = createRawMessage("hello")
    parser.canParse(msg) shouldBe true
    parser.parse(msg).isSuccess shouldBe true
    parser.format shouldBe PayloadFormat.Text
  }

  "CheckpointStore" should "be implementable" in {
    val store: CheckpointStore = new CheckpointStore {
      private var stored: Map[String, ConnectorCheckpoint] = Map.empty

      override def save(checkpoint: ConnectorCheckpoint): Try[Unit] = {
        stored = stored + (checkpoint.queueName -> checkpoint)
        Success(())
      }

      override def load(queueName: String, partitionId: Int): Try[Option[ConnectorCheckpoint]] = {
        Success(stored.get(queueName))
      }

      override def delete(queueName: String, partitionId: Int): Try[Unit] = {
        stored = stored - queueName
        Success(())
      }

      override def list(): Try[Seq[ConnectorCheckpoint]] = {
        Success(stored.values.toSeq)
      }
    }

    val checkpoint = ConnectorCheckpoint(
      queueName = "Q1",
      queueManager = "QM1",
      lastMessageId = None,
      lastCorrelationId = None,
      lastProcessedTimestamp = 0L,
      messagesProcessed = 0L,
      bytesProcessed = 0L,
      partitionId = 0,
      createdAt = System.currentTimeMillis()
    )

    store.save(checkpoint).isSuccess shouldBe true
    store.load("Q1", 0).get shouldBe Some(checkpoint)
    store.list().get should have size 1
    store.delete("Q1", 0).isSuccess shouldBe true
    store.load("Q1", 0).get shouldBe None
  }

  "AuditSink" should "be implementable" in {
    var capturedEvents: List[AuditEvent] = Nil

    val sink: AuditSink = new AuditSink {
      override def emit(event: AuditEvent): Unit = {
        capturedEvents = event :: capturedEvents
      }

      override def flush(): Unit = {}

      override def close(): Unit = {}
    }

    val event = AuditEvent(
      eventType = AuditEventType.MessageReceived,
      timestamp = System.currentTimeMillis(),
      queueName = "Q1",
      queueManager = "QM1",
      messageId = None,
      correlationId = None,
      sourceId = "test",
      details = Map.empty
    )

    sink.emit(event)

    capturedEvents should have size 1
    capturedEvents.head.eventType shouldBe AuditEventType.MessageReceived
  }

  "ConnectorMetrics" should "be implementable" in {
    var messagesReceived: Long = 0
    var bytesReceived: Long = 0

    val metrics: ConnectorMetrics = new ConnectorMetrics {
      override def recordMessageReceived(queueName: String, bytes: Long): Unit = {
        messagesReceived += 1
        bytesReceived += bytes
      }

      override def recordMessageCommitted(queueName: String): Unit = {}

      override def recordMessageRolledBack(queueName: String): Unit = {}

      override def recordParseSuccess(queueName: String, format: String, durationNanos: Long): Unit = {}

      override def recordParseFailure(queueName: String, format: String): Unit = {}

      override def recordRetry(queueName: String, attempt: Int): Unit = {}

      override def recordConnectionAttempt(queueManager: String, success: Boolean): Unit = {}

      override def recordPollDuration(queueName: String, durationMs: Long, messageCount: Int): Unit = {}

      override def getMessagesReceived(queueName: String): Long = messagesReceived

      override def getBytesReceived(queueName: String): Long = bytesReceived
    }

    metrics.recordMessageReceived("Q1", 1024)
    metrics.recordMessageReceived("Q1", 2048)

    metrics.getMessagesReceived("Q1") shouldBe 2
    metrics.getBytesReceived("Q1") shouldBe 3072
  }
}
