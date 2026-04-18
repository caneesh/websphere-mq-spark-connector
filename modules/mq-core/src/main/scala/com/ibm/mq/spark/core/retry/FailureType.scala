package com.ibm.mq.spark.core.retry

/**
 * Represents the classification of a failure for retry decisions.
 */
sealed trait FailureType {
  def message: String
  def isTransient: Boolean
  def isPermanent: Boolean = !isTransient
}

/**
 * A transient failure that may succeed on retry.
 * Examples: network timeouts, temporary unavailability, rate limiting.
 */
case class TransientFailure(message: String) extends FailureType {
  override def isTransient: Boolean = true
}

/**
 * A permanent failure that will not succeed on retry.
 * Examples: parse errors, validation failures, authorization denied.
 */
case class PermanentFailure(message: String) extends FailureType {
  override def isTransient: Boolean = false
}
