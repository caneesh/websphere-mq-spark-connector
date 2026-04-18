package com.ibm.mq.spark.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MQConsumerConfigSpec extends AnyFlatSpec with Matchers {

  "MQConsumerConfig" should "have sensible defaults" in {
    val config = MQConsumerConfig()

    config.batchSize shouldBe 100
    config.pollTimeoutMs shouldBe 5000L
    config.maxRetries shouldBe 3
    config.backoffMultiplier shouldBe 2.0
    config.initialBackoffMs shouldBe 1000L
    config.maxBackoffMs shouldBe 30000L
  }

  it should "validate batch size is positive" in {
    val config = MQConsumerConfig(batchSize = 0)
    config.validate() should contain("batchSize must be positive")
  }

  it should "validate batch size not too large" in {
    val config = MQConsumerConfig(batchSize = 100001)
    config.validate() should contain("batchSize must not exceed 100000")
  }

  it should "validate poll timeout is positive" in {
    val config = MQConsumerConfig(pollTimeoutMs = -1)
    config.validate() should contain("pollTimeoutMs must be non-negative")
  }

  it should "validate max retries is non-negative" in {
    val config = MQConsumerConfig(maxRetries = -1)
    config.validate() should contain("maxRetries must be non-negative")
  }

  it should "validate backoff multiplier is at least 1.0" in {
    val config = MQConsumerConfig(backoffMultiplier = 0.5)
    config.validate() should contain("backoffMultiplier must be at least 1.0")
  }

  it should "calculate backoff for retry attempt" in {
    val config = MQConsumerConfig(
      initialBackoffMs = 1000L,
      backoffMultiplier = 2.0,
      maxBackoffMs = 10000L
    )

    config.backoffForAttempt(0) shouldBe 1000L
    config.backoffForAttempt(1) shouldBe 2000L
    config.backoffForAttempt(2) shouldBe 4000L
    config.backoffForAttempt(3) shouldBe 8000L
    config.backoffForAttempt(4) shouldBe 10000L // capped at max
  }

  it should "support browse mode" in {
    val config = MQConsumerConfig(browseOnly = true)
    config.browseOnly shouldBe true
  }

  it should "support backout threshold" in {
    val config = MQConsumerConfig(backoutThreshold = Some(5))
    config.backoutThreshold shouldBe Some(5)
  }
}
