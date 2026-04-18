package com.ibm.mq.spark.core.retry

import scala.util.Random

/**
 * Policy for determining retry behavior after failures.
 */
trait RetryPolicy {
  def shouldRetry(attempt: Int, failure: FailureType): Boolean
  def delayMs(attempt: Int): Long
  def delayWithJitter(attempt: Int): Long
}

/**
 * Default retry policy with exponential backoff and jitter.
 *
 * @param maxRetries maximum number of retry attempts
 * @param initialDelayMs initial delay in milliseconds
 * @param maxDelayMs maximum delay cap in milliseconds
 * @param backoffMultiplier multiplier for exponential growth
 * @param jitterFactor factor for random jitter (0.0 to 1.0)
 */
case class DefaultRetryPolicy(
    maxRetries: Int = 3,
    initialDelayMs: Long = 1000L,
    maxDelayMs: Long = 30000L,
    backoffMultiplier: Double = 2.0,
    jitterFactor: Double = 0.0
) extends RetryPolicy {

  require(maxRetries >= 0, "maxRetries must be non-negative")
  require(initialDelayMs > 0, "initialDelayMs must be positive")
  require(maxDelayMs >= initialDelayMs, "maxDelayMs must be >= initialDelayMs")
  require(backoffMultiplier >= 1.0, "backoffMultiplier must be >= 1.0")
  require(jitterFactor >= 0.0 && jitterFactor <= 1.0, "jitterFactor must be between 0.0 and 1.0")

  private val random = new Random()

  override def shouldRetry(attempt: Int, failure: FailureType): Boolean = {
    failure match {
      case _: PermanentFailure => false
      case _: TransientFailure => attempt <= maxRetries
    }
  }

  override def delayMs(attempt: Int): Long = {
    val exponentialDelay = initialDelayMs * math.pow(backoffMultiplier, attempt - 1).toLong
    math.min(exponentialDelay, maxDelayMs)
  }

  override def delayWithJitter(attempt: Int): Long = {
    val baseDelay = delayMs(attempt)
    if (jitterFactor == 0.0) {
      baseDelay
    } else {
      val jitterRange = (baseDelay * jitterFactor).toLong
      val jitter = (random.nextDouble() * 2 - 1) * jitterRange
      math.max(1L, baseDelay + jitter.toLong)
    }
  }
}

object RetryPolicy {

  val NoRetry: RetryPolicy = DefaultRetryPolicy(maxRetries = 0)

  val Default: RetryPolicy = DefaultRetryPolicy()

  def withMaxRetries(n: Int): RetryPolicy = DefaultRetryPolicy(maxRetries = n)
}
