package com.ibm.mq.spark.core.config

case class MQConsumerConfig(
    batchSize: Int = 100,
    pollTimeoutMs: Long = 5000L,
    maxRetries: Int = 3,
    backoffMultiplier: Double = 2.0,
    initialBackoffMs: Long = 1000L,
    maxBackoffMs: Long = 30000L,
    browseOnly: Boolean = false,
    backoutThreshold: Option[Int] = None,
    quarantineQueue: Option[String] = None
) {

  def validate(): List[String] = {
    val errors = List.newBuilder[String]

    if (batchSize <= 0) errors += "batchSize must be positive"
    if (batchSize > 100000) errors += "batchSize must not exceed 100000"
    if (pollTimeoutMs < 0) errors += "pollTimeoutMs must be non-negative"
    if (maxRetries < 0) errors += "maxRetries must be non-negative"
    if (backoffMultiplier < 1.0) errors += "backoffMultiplier must be at least 1.0"
    if (initialBackoffMs <= 0) errors += "initialBackoffMs must be positive"
    if (maxBackoffMs <= 0) errors += "maxBackoffMs must be positive"

    errors.result()
  }

  def backoffForAttempt(attempt: Int): Long = {
    val backoff = (initialBackoffMs * math.pow(backoffMultiplier, attempt)).toLong
    math.min(backoff, maxBackoffMs)
  }
}
