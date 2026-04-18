package com.ibm.mq.spark.core.interfaces

trait ConnectorMetrics {

  def recordMessageReceived(queueName: String, bytes: Long): Unit

  def recordMessageCommitted(queueName: String): Unit

  def recordMessageRolledBack(queueName: String): Unit

  def recordParseSuccess(queueName: String, format: String, durationNanos: Long): Unit

  def recordParseFailure(queueName: String, format: String): Unit

  def recordRetry(queueName: String, attempt: Int): Unit

  def recordConnectionAttempt(queueManager: String, success: Boolean): Unit

  def recordPollDuration(queueName: String, durationMs: Long, messageCount: Int): Unit

  def getMessagesReceived(queueName: String): Long

  def getBytesReceived(queueName: String): Long
}
