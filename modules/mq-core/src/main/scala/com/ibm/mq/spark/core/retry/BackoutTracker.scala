package com.ibm.mq.spark.core.retry

import com.ibm.mq.spark.core.message.RawMQMessage

/**
 * Tracks message backout count and detects poison messages.
 *
 * @param threshold backout count at which a message is considered poison
 */
case class BackoutTracker(threshold: Int) {

  require(threshold > 0, "threshold must be positive")

  /**
   * Determines if a message has exceeded the backout threshold.
   */
  def isPoisonMessage(message: RawMQMessage): Boolean = {
    message.backoutCount >= threshold
  }

  /**
   * Gets the backout count from a message.
   */
  def getBackoutCount(message: RawMQMessage): Int = {
    message.backoutCount
  }

  /**
   * Calculates remaining retries before the message becomes poison.
   */
  def remainingRetries(message: RawMQMessage): Int = {
    math.max(0, threshold - message.backoutCount)
  }
}
