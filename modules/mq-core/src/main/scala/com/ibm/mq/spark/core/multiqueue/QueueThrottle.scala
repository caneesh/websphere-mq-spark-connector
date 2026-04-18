package com.ibm.mq.spark.core.multiqueue

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

/**
 * Rate limiter for a single queue.
 * Uses token bucket algorithm with per-second refill.
 */
case class QueueThrottle(messagesPerSecond: Int) {

  private val lastRefillTime = new AtomicLong(System.currentTimeMillis())
  private val availableTokens = new AtomicLong(messagesPerSecond)
  private val acquiredCount = new LongAdder()
  private val startTime = System.currentTimeMillis()

  /**
   * Attempts to acquire a permit for one message.
   * Returns true if acquired, false if rate limited.
   */
  def tryAcquire(): Boolean = {
    refillTokens()
    val tokens = availableTokens.get()
    if (tokens > 0) {
      if (availableTokens.compareAndSet(tokens, tokens - 1)) {
        acquiredCount.increment()
        true
      } else {
        tryAcquire()
      }
    } else {
      false
    }
  }

  /**
   * Returns current rate in messages per second.
   */
  def currentRate: Double = {
    val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
    if (elapsed > 0) acquiredCount.sum() / elapsed else 0.0
  }

  private def refillTokens(): Unit = {
    val now = System.currentTimeMillis()
    val last = lastRefillTime.get()
    val elapsed = now - last

    if (elapsed >= 1000) {
      if (lastRefillTime.compareAndSet(last, now)) {
        availableTokens.set(messagesPerSecond)
      }
    }
  }
}

/**
 * Manages throttles for multiple queues.
 */
class MultiQueueThrottler {

  private val throttles = new ConcurrentHashMap[String, QueueThrottle]()

  /**
   * Sets rate limit for a queue.
   */
  def setLimit(queueName: String, messagesPerSecond: Int): Unit = {
    throttles.put(queueName, QueueThrottle(messagesPerSecond))
  }

  /**
   * Removes rate limit for a queue.
   */
  def removeLimit(queueName: String): Unit = {
    throttles.remove(queueName)
  }

  /**
   * Gets rate limit for a queue.
   */
  def getLimit(queueName: String): Option[Int] = {
    Option(throttles.get(queueName)).map(_.messagesPerSecond)
  }

  /**
   * Attempts to acquire permit for a queue.
   * Returns true if no limit or within limit, false if throttled.
   */
  def tryAcquire(queueName: String): Boolean = {
    Option(throttles.get(queueName)) match {
      case Some(throttle) => throttle.tryAcquire()
      case None => true
    }
  }

  /**
   * Gets current rate for a queue.
   */
  def getCurrentRate(queueName: String): Option[Double] = {
    Option(throttles.get(queueName)).map(_.currentRate)
  }

  /**
   * Clears all throttles.
   */
  def clear(): Unit = {
    throttles.clear()
  }
}

object MultiQueueThrottler {
  def apply(): MultiQueueThrottler = new MultiQueueThrottler()
}
