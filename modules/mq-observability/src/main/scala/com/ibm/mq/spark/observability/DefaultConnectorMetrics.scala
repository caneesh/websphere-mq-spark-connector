package com.ibm.mq.spark.observability

import com.ibm.mq.spark.core.interfaces.ConnectorMetrics

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicLong, LongAdder}

/**
 * Default implementation of connector metrics.
 * Thread-safe and Prometheus-friendly.
 */
case class DefaultConnectorMetrics() extends ConnectorMetrics {

  private val messagesReceived = new ConcurrentHashMap[String, LongAdder]()
  private val bytesReceived = new ConcurrentHashMap[String, LongAdder]()
  private val messagesCommitted = new ConcurrentHashMap[String, LongAdder]()
  private val messagesRolledBack = new ConcurrentHashMap[String, LongAdder]()
  private val parseSuccesses = new ConcurrentHashMap[String, LongAdder]()
  private val parseFailures = new ConcurrentHashMap[String, LongAdder]()
  private val retries = new ConcurrentHashMap[String, LongAdder]()
  private val connectionSuccesses = new ConcurrentHashMap[String, LongAdder]()
  private val connectionFailures = new ConcurrentHashMap[String, LongAdder]()
  private val pollCounts = new ConcurrentHashMap[String, LongAdder]()
  private val pollDurations = new ConcurrentHashMap[String, LongAdder]()

  private val startTime = System.currentTimeMillis()

  override def recordMessageReceived(queueName: String, bytes: Long): Unit = {
    getOrCreate(messagesReceived, queueName).increment()
    getOrCreate(bytesReceived, queueName).add(bytes)
  }

  override def recordMessageCommitted(queueName: String): Unit = {
    getOrCreate(messagesCommitted, queueName).increment()
  }

  override def recordMessageRolledBack(queueName: String): Unit = {
    getOrCreate(messagesRolledBack, queueName).increment()
  }

  override def recordParseSuccess(queueName: String, format: String, durationNanos: Long): Unit = {
    getOrCreate(parseSuccesses, queueName).increment()
  }

  override def recordParseFailure(queueName: String, format: String): Unit = {
    getOrCreate(parseFailures, queueName).increment()
  }

  override def recordRetry(queueName: String, attempt: Int): Unit = {
    getOrCreate(retries, queueName).increment()
  }

  override def recordConnectionAttempt(queueManager: String, success: Boolean): Unit = {
    if (success) {
      getOrCreate(connectionSuccesses, queueManager).increment()
    } else {
      getOrCreate(connectionFailures, queueManager).increment()
    }
  }

  override def recordPollDuration(queueName: String, durationMs: Long, messageCount: Int): Unit = {
    getOrCreate(pollCounts, queueName).increment()
    getOrCreate(pollDurations, queueName).add(durationMs)
  }

  override def getMessagesReceived(queueName: String): Long = {
    getCount(messagesReceived, queueName)
  }

  override def getBytesReceived(queueName: String): Long = {
    getCount(bytesReceived, queueName)
  }

  def getCommittedCount(queueName: String): Long = {
    getCount(messagesCommitted, queueName)
  }

  def getRolledBackCount(queueName: String): Long = {
    getCount(messagesRolledBack, queueName)
  }

  def getParseSuccessCount(queueName: String): Long = {
    getCount(parseSuccesses, queueName)
  }

  def getParseFailureCount(queueName: String): Long = {
    getCount(parseFailures, queueName)
  }

  def getRetryCount(queueName: String): Long = {
    getCount(retries, queueName)
  }

  def getConnectionSuccessCount(queueManager: String): Long = {
    getCount(connectionSuccesses, queueManager)
  }

  def getConnectionFailureCount(queueManager: String): Long = {
    getCount(connectionFailures, queueManager)
  }

  def getPollCount(queueName: String): Long = {
    getCount(pollCounts, queueName)
  }

  def getTotalPollDuration(queueName: String): Long = {
    getCount(pollDurations, queueName)
  }

  def getMessageThroughput(queueName: String): Double = {
    val elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000.0
    if (elapsedSeconds > 0) {
      getMessagesReceived(queueName) / elapsedSeconds
    } else {
      0.0
    }
  }

  def toPrometheusFormat(): String = {
    val sb = new StringBuilder

    appendMetric(sb, "mq_messages_received", "Total messages received", messagesReceived)
    appendMetric(sb, "mq_bytes_received", "Total bytes received", bytesReceived)
    appendMetric(sb, "mq_messages_committed", "Total messages committed", messagesCommitted)
    appendMetric(sb, "mq_messages_rolledback", "Total messages rolled back", messagesRolledBack)
    appendMetric(sb, "mq_parse_success", "Total successful parses", parseSuccesses)
    appendMetric(sb, "mq_parse_failure", "Total parse failures", parseFailures)
    appendMetric(sb, "mq_retries", "Total retry attempts", retries)
    appendMetric(sb, "mq_connection_success", "Total successful connections", connectionSuccesses)
    appendMetric(sb, "mq_connection_failure", "Total failed connections", connectionFailures)
    appendMetric(sb, "mq_poll_count", "Total poll operations", pollCounts)
    appendMetric(sb, "mq_poll_duration_ms", "Total poll duration in milliseconds", pollDurations)

    sb.toString()
  }

  private def getOrCreate(map: ConcurrentHashMap[String, LongAdder], key: String): LongAdder = {
    map.computeIfAbsent(key, _ => new LongAdder())
  }

  private def getCount(map: ConcurrentHashMap[String, LongAdder], key: String): Long = {
    Option(map.get(key)).map(_.sum()).getOrElse(0L)
  }

  private def appendMetric(
      sb: StringBuilder,
      name: String,
      help: String,
      map: ConcurrentHashMap[String, LongAdder]
  ): Unit = {
    sb.append(s"# HELP $name $help\n")
    sb.append(s"# TYPE $name counter\n")
    map.forEach { (label, adder) =>
      sb.append(s"""$name{queue="$label"} ${adder.sum()}\n""")
    }
  }
}
