package com.ibm.mq.spark.core.client

import com.ibm.mq.spark.core.connection.MQConnectionConfig
import com.ibm.mq.spark.core.message.RawMQMessage

/**
 * Low-level transport abstraction for IBM MQ operations.
 *
 * This trait provides the foundation for interacting with MQ queue managers,
 * separating transport concerns from higher-level client logic.
 * Implementations may connect to actual MQ servers or provide mock behavior
 * for testing.
 */
trait MQTransport {

  /**
   * Establishes a connection to the MQ queue manager.
   *
   * @param config the connection configuration
   * @throws RuntimeException if connection fails
   */
  def connect(config: MQConnectionConfig): Unit

  /**
   * Disconnects from the MQ queue manager.
   */
  def disconnect(): Unit

  /**
   * Indicates whether the transport is currently connected.
   *
   * @return true if connected, false otherwise
   */
  def isConnected: Boolean

  /**
   * Receives a message from the queue.
   *
   * @param waitMillis maximum time to wait for a message
   * @return Some(message) if available within wait time, None otherwise
   */
  def receive(waitMillis: Long): Option[RawMQMessage]

  /**
   * Commits the current transaction, acknowledging all received messages.
   *
   * @throws RuntimeException if commit fails
   */
  def commit(): Unit

  /**
   * Rolls back the current transaction, returning messages to the queue.
   *
   * @throws RuntimeException if rollback fails
   */
  def rollback(): Unit
}
