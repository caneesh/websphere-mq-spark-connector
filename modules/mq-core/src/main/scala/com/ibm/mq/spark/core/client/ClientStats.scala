package com.ibm.mq.spark.core.client

/**
 * Statistics for MQ client operations.
 *
 * Tracks message polling metrics for monitoring and debugging purposes.
 * This class is mutable for efficient updates during polling operations.
 */
class ClientStats {

  private var _messagesPolled: Long = 0L
  private var _pollCount: Long = 0L
  private var _lastPollTime: Long = 0L
  private var _totalPollDuration: Long = 0L
  private var _emptyPollCount: Long = 0L

  /**
   * Total number of messages polled.
   */
  def messagesPolled: Long = _messagesPolled

  /**
   * Total number of poll operations performed.
   */
  def pollCount: Long = _pollCount

  /**
   * Timestamp of the last poll operation (epoch millis).
   */
  def lastPollTime: Long = _lastPollTime

  /**
   * Total time spent in poll operations (millis).
   */
  def totalPollDuration: Long = _totalPollDuration

  /**
   * Number of poll operations that returned no messages.
   */
  def emptyPollCount: Long = _emptyPollCount

  /**
   * Records statistics for a poll operation.
   *
   * @param messageCount number of messages received in this poll
   * @param durationMillis time taken for the poll operation
   */
  def recordPoll(messageCount: Int, durationMillis: Long): Unit = {
    _messagesPolled += messageCount
    _pollCount += 1
    _lastPollTime = System.currentTimeMillis()
    _totalPollDuration += durationMillis
    if (messageCount == 0) _emptyPollCount += 1
  }

  /**
   * Resets all statistics to zero.
   */
  def reset(): Unit = {
    _messagesPolled = 0L
    _pollCount = 0L
    _lastPollTime = 0L
    _totalPollDuration = 0L
    _emptyPollCount = 0L
  }

  override def toString: String =
    s"ClientStats(messagesPolled=${_messagesPolled}, pollCount=${_pollCount}, " +
      s"lastPollTime=${_lastPollTime}, totalPollDuration=${_totalPollDuration}, emptyPollCount=${_emptyPollCount})"
}

object ClientStats {

  /**
   * Creates a new stats instance with all counters at zero.
   */
  def apply(): ClientStats = new ClientStats()
}
