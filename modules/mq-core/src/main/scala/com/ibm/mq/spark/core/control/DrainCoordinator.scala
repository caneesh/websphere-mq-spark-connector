package com.ibm.mq.spark.core.control

import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, TimeUnit}

/**
 * Coordinates drain operations by tracking in-flight messages.
 */
class DrainCoordinator {

  private val inFlight = ConcurrentHashMap.newKeySet[String]()
  @volatile private var drainLatch: CountDownLatch = _

  /**
   * Returns count of in-flight messages.
   */
  def inFlightCount: Int = inFlight.size()

  /**
   * Tracks a message as in-flight.
   */
  def trackMessage(messageId: String): Unit = {
    inFlight.add(messageId)
  }

  /**
   * Marks a message as completed.
   */
  def completeMessage(messageId: String): Unit = {
    inFlight.remove(messageId)
    checkDrainComplete()
  }

  /**
   * Returns true if no messages are in-flight.
   */
  def isFullyDrained: Boolean = inFlight.isEmpty

  /**
   * Waits for all in-flight messages to complete.
   *
   * @param timeoutMs maximum time to wait
   * @return true if drained, false if timeout
   */
  def awaitDrain(timeoutMs: Long): Boolean = {
    if (isFullyDrained) return true

    drainLatch = new CountDownLatch(1)

    if (isFullyDrained) {
      return true
    }

    try {
      drainLatch.await(timeoutMs, TimeUnit.MILLISECONDS) || isFullyDrained
    } catch {
      case _: InterruptedException => false
    }
  }

  /**
   * Clears all tracked messages.
   */
  def clear(): Unit = {
    inFlight.clear()
  }

  private def checkDrainComplete(): Unit = {
    if (isFullyDrained && drainLatch != null) {
      drainLatch.countDown()
    }
  }
}

object DrainCoordinator {
  def apply(): DrainCoordinator = new DrainCoordinator()
}
