package com.ibm.mq.spark.core.retry

import com.ibm.mq.spark.core.message.RawMQMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class RetryPolicySpec extends AnyFlatSpec with Matchers {

  "DefaultRetryPolicy" should "allow retry when attempt is below max" in {
    val policy = DefaultRetryPolicy(maxRetries = 3)

    policy.shouldRetry(attempt = 1, failure = TransientFailure("timeout")) shouldBe true
    policy.shouldRetry(attempt = 2, failure = TransientFailure("timeout")) shouldBe true
    policy.shouldRetry(attempt = 3, failure = TransientFailure("timeout")) shouldBe true
  }

  it should "deny retry when max attempts exceeded" in {
    val policy = DefaultRetryPolicy(maxRetries = 3)

    policy.shouldRetry(attempt = 4, failure = TransientFailure("timeout")) shouldBe false
  }

  it should "never retry permanent failures" in {
    val policy = DefaultRetryPolicy(maxRetries = 5)

    policy.shouldRetry(attempt = 1, failure = PermanentFailure("invalid format")) shouldBe false
  }

  it should "calculate exponential backoff delay" in {
    val policy = DefaultRetryPolicy(
      maxRetries = 5,
      initialDelayMs = 100,
      maxDelayMs = 10000,
      backoffMultiplier = 2.0
    )

    policy.delayMs(attempt = 1) shouldBe 100
    policy.delayMs(attempt = 2) shouldBe 200
    policy.delayMs(attempt = 3) shouldBe 400
    policy.delayMs(attempt = 4) shouldBe 800
  }

  it should "cap delay at max value" in {
    val policy = DefaultRetryPolicy(
      maxRetries = 10,
      initialDelayMs = 1000,
      maxDelayMs = 5000,
      backoffMultiplier = 2.0
    )

    policy.delayMs(attempt = 5) shouldBe 5000  // Would be 16000, capped at 5000
  }

  it should "support jitter in delay calculation" in {
    val policy = DefaultRetryPolicy(
      maxRetries = 3,
      initialDelayMs = 1000,
      jitterFactor = 0.1
    )

    val delays = (1 to 100).map(_ => policy.delayWithJitter(attempt = 1))
    val minDelay = delays.min
    val maxDelay = delays.max

    minDelay should be >= 900L
    maxDelay should be <= 1100L
    minDelay should not be maxDelay  // Jitter should create variation
  }

  "FailureClassifier" should "classify connection errors as transient" in {
    val classifier = DefaultFailureClassifier()

    classifier.classify(new java.net.SocketTimeoutException("timeout")) shouldBe a[TransientFailure]
    classifier.classify(new java.net.ConnectException("refused")) shouldBe a[TransientFailure]
    classifier.classify(new java.io.IOException("network error")) shouldBe a[TransientFailure]
  }

  it should "classify parse errors as permanent" in {
    val classifier = DefaultFailureClassifier()

    classifier.classify(new IllegalArgumentException("invalid format")) shouldBe a[PermanentFailure]
    classifier.classify(new NumberFormatException("not a number")) shouldBe a[PermanentFailure]
  }

  it should "classify unknown errors as transient by default" in {
    val classifier = DefaultFailureClassifier()

    classifier.classify(new RuntimeException("unknown")) shouldBe a[TransientFailure]
  }

  "BackoutTracker" should "detect poison message based on backout count" in {
    val tracker = BackoutTracker(threshold = 3)

    tracker.isPoisonMessage(createMessage(backoutCount = 0)) shouldBe false
    tracker.isPoisonMessage(createMessage(backoutCount = 2)) shouldBe false
    tracker.isPoisonMessage(createMessage(backoutCount = 3)) shouldBe true
    tracker.isPoisonMessage(createMessage(backoutCount = 5)) shouldBe true
  }

  it should "provide backout count from message" in {
    val tracker = BackoutTracker(threshold = 5)

    tracker.getBackoutCount(createMessage(backoutCount = 7)) shouldBe 7
  }

  it should "calculate remaining retries" in {
    val tracker = BackoutTracker(threshold = 5)

    tracker.remainingRetries(createMessage(backoutCount = 2)) shouldBe 3
    tracker.remainingRetries(createMessage(backoutCount = 5)) shouldBe 0
  }

  "QuarantineAction" should "represent different disposition options" in {
    QuarantineAction.MoveToDeadLetter.description should include ("dead letter")
    QuarantineAction.MoveToBackout.description should include ("backout")
    QuarantineAction.Discard.description should include ("discard")
    QuarantineAction.CustomQueue("MY.QUARANTINE").description should include ("MY.QUARANTINE")
  }

  "QuarantineDecision" should "combine action with reason" in {
    val decision = QuarantineDecision(
      action = QuarantineAction.MoveToDeadLetter,
      reason = "exceeded backout threshold",
      messageId = "aabb0001"
    )

    decision.action shouldBe QuarantineAction.MoveToDeadLetter
    decision.reason should include ("exceeded")
    decision.messageId shouldBe "aabb0001"
  }

  "PoisonMessageHandler" should "decide quarantine action for poison messages" in {
    val handler = DefaultPoisonMessageHandler(
      backoutThreshold = 3,
      defaultAction = QuarantineAction.MoveToDeadLetter
    )

    val message = createMessage(backoutCount = 5)
    val decision = handler.handlePoisonMessage(message)

    decision.action shouldBe QuarantineAction.MoveToDeadLetter
    decision.reason should include ("backout")
  }

  it should "support custom quarantine queue" in {
    val handler = DefaultPoisonMessageHandler(
      backoutThreshold = 3,
      defaultAction = QuarantineAction.CustomQueue("APP.POISON.QUEUE")
    )

    val message = createMessage(backoutCount = 3)
    val decision = handler.handlePoisonMessage(message)

    decision.action shouldBe QuarantineAction.CustomQueue("APP.POISON.QUEUE")
  }

  private def createMessage(backoutCount: Int): RawMQMessage = {
    RawMQMessage(
      messageId = Array(1, 2, 3),
      correlationId = Array.emptyByteArray,
      payload = "test".getBytes("UTF-8"),
      putTimestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = backoutCount
    )
  }
}
