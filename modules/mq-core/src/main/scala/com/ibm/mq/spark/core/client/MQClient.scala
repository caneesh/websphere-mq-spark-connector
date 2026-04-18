package com.ibm.mq.spark.core.client

import com.ibm.mq.spark.core.config.MQConnectorConfig
import com.ibm.mq.spark.core.message.MQMessage

import scala.util.Try

/**
 * Client interface for IBM MQ operations.
 *
 * Implementations provide connectivity to IBM MQ queue managers
 * and support for message retrieval and publishing operations.
 */
trait MQClient extends AutoCloseable {

  /**
   * Establishes a connection to the MQ queue manager.
   *
   * @param config the connector configuration
   * @return Success if connection established, Failure otherwise
   */
  def connect(config: MQConnectorConfig): Try[Unit]

  /**
   * Retrieves the next available message from the configured queue.
   *
   * @return Some(message) if available, None if queue is empty
   */
  def receive(): Option[MQMessage]

  /**
   * Sends a message to the configured queue.
   *
   * @param message the message to send
   * @return Success if sent, Failure otherwise
   */
  def send(message: MQMessage): Try[Unit]

  /**
   * Indicates whether the client is currently connected.
   *
   * @return true if connected, false otherwise
   */
  def isConnected: Boolean

  /**
   * Commits the current transaction.
   *
   * @return Success if committed, Failure otherwise
   */
  def commit(): Try[Unit]

  /**
   * Rolls back the current transaction.
   *
   * @return Success if rolled back, Failure otherwise
   */
  def rollback(): Try[Unit]
}
