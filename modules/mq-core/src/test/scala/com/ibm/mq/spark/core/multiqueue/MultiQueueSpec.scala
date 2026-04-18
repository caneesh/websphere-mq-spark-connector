package com.ibm.mq.spark.core.multiqueue

import com.ibm.mq.spark.core.interfaces.PayloadParser
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MultiQueueSpec extends AnyFlatSpec with Matchers {

  "QueueConfig" should "hold per-queue settings" in {
    val config = QueueConfig(
      queueName = "APP.QUEUE.1",
      batchSize = 500,
      pollTimeoutMs = 2000,
      parserType = "JSON"
    )

    config.queueName shouldBe "APP.QUEUE.1"
    config.batchSize shouldBe 500
  }

  it should "have sensible defaults" in {
    val config = QueueConfig(queueName = "MY.QUEUE")

    config.batchSize shouldBe 1000
    config.pollTimeoutMs shouldBe 5000
    config.parserType shouldBe "AUTO"
    config.throttleMessagesPerSecond shouldBe None
  }

  "MultiQueueConfig" should "manage multiple queue configurations" in {
    val config = MultiQueueConfig(Seq(
      QueueConfig("QUEUE.A", batchSize = 100),
      QueueConfig("QUEUE.B", batchSize = 200)
    ))

    config.queueNames should contain allOf ("QUEUE.A", "QUEUE.B")
    config.getConfig("QUEUE.A").get.batchSize shouldBe 100
    config.getConfig("QUEUE.B").get.batchSize shouldBe 200
  }

  it should "return None for unknown queue" in {
    val config = MultiQueueConfig(Seq(QueueConfig("QUEUE.A")))

    config.getConfig("UNKNOWN.QUEUE") shouldBe None
  }

  it should "validate queue names are unique" in {
    val config = MultiQueueConfig(Seq(
      QueueConfig("QUEUE.A"),
      QueueConfig("QUEUE.A")
    ))

    config.validate() should not be empty
    config.validate().exists(_.contains("duplicate")) shouldBe true
  }

  "QueueParserRegistry" should "register parsers for queues" in {
    val registry = QueueParserRegistry()
    val mockParser = new MockParser("JSON")

    registry.register("QUEUE.A", mockParser)

    registry.getParser("QUEUE.A") shouldBe Some(mockParser)
  }

  it should "return None for unregistered queue" in {
    val registry = QueueParserRegistry()

    registry.getParser("UNREGISTERED") shouldBe None
  }

  it should "support default parser fallback" in {
    val defaultParser = new MockParser("DEFAULT")
    val registry = QueueParserRegistry(defaultParser = Some(defaultParser))

    registry.getParser("ANY.QUEUE") shouldBe Some(defaultParser)
  }

  it should "prefer queue-specific parser over default" in {
    val defaultParser = new MockParser("DEFAULT")
    val specificParser = new MockParser("SPECIFIC")
    val registry = QueueParserRegistry(defaultParser = Some(defaultParser))

    registry.register("QUEUE.A", specificParser)

    registry.getParser("QUEUE.A") shouldBe Some(specificParser)
    registry.getParser("QUEUE.B") shouldBe Some(defaultParser)
  }

  "QueueThrottle" should "allow messages when under limit" in {
    val throttle = QueueThrottle(messagesPerSecond = 100)

    throttle.tryAcquire() shouldBe true
    throttle.tryAcquire() shouldBe true
  }

  it should "block when rate exceeded" in {
    val throttle = QueueThrottle(messagesPerSecond = 1)

    throttle.tryAcquire() shouldBe true
    throttle.tryAcquire() shouldBe false
  }

  it should "report current rate" in {
    val throttle = QueueThrottle(messagesPerSecond = 100)

    throttle.tryAcquire()
    throttle.tryAcquire()

    throttle.currentRate should be >= 0.0
  }

  "MultiQueueThrottler" should "manage per-queue throttles" in {
    val throttler = MultiQueueThrottler()

    throttler.setLimit("QUEUE.A", 10)
    throttler.setLimit("QUEUE.B", 20)

    throttler.getLimit("QUEUE.A") shouldBe Some(10)
    throttler.getLimit("QUEUE.B") shouldBe Some(20)
  }

  it should "allow acquisition when no limit set" in {
    val throttler = MultiQueueThrottler()

    throttler.tryAcquire("UNLIMITED.QUEUE") shouldBe true
  }

  it should "enforce per-queue limits" in {
    val throttler = MultiQueueThrottler()
    throttler.setLimit("QUEUE.A", 1)

    throttler.tryAcquire("QUEUE.A") shouldBe true
    throttler.tryAcquire("QUEUE.A") shouldBe false
  }

  class MockParser(name: String) extends PayloadParser {
    override def format: PayloadFormat = PayloadFormat.Text
    override def canParse(message: RawMQMessage): Boolean = true
    override def parse(message: RawMQMessage): ParsedPayloadResult = {
      ParsedPayloadResult.success(PayloadFormat.Text, "parsed", message.payload.length)
    }
    override def toString: String = s"MockParser($name)"
  }
}
