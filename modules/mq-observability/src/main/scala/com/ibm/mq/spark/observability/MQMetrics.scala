package com.ibm.mq.spark.observability

import io.micrometer.core.instrument.MeterRegistry

/**
 * Metrics collection interface for MQ connector operations.
 *
 * Implementations record operational metrics such as message counts,
 * processing times, error rates, and connection statistics.
 */
trait MQMetrics {

  /**
   * Returns the meter registry used for metric recording.
   *
   * @return the Micrometer meter registry
   */
  def registry: MeterRegistry

  /**
   * Records a message received from the queue.
   *
   * @param queueName the source queue name
   * @param sizeBytes the message size in bytes
   */
  def recordMessageReceived(queueName: String, sizeBytes: Long): Unit

  /**
   * Records a message sent to the queue.
   *
   * @param queueName the destination queue name
   * @param sizeBytes the message size in bytes
   */
  def recordMessageSent(queueName: String, sizeBytes: Long): Unit

  /**
   * Records a processing error.
   *
   * @param queueName the queue name
   * @param errorType the type of error
   */
  def recordError(queueName: String, errorType: String): Unit

  /**
   * Records the time taken to process a message.
   *
   * @param queueName the queue name
   * @param durationMs processing duration in milliseconds
   */
  def recordProcessingTime(queueName: String, durationMs: Long): Unit

  /**
   * Records a connection event.
   *
   * @param queueManager the queue manager name
   * @param connected true if connected, false if disconnected
   */
  def recordConnectionEvent(queueManager: String, connected: Boolean): Unit
}
