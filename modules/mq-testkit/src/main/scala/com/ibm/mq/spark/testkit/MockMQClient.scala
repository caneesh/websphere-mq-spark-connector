package com.ibm.mq.spark.testkit

import com.ibm.mq.spark.core.client.MQClient
import com.ibm.mq.spark.core.config.MQConnectorConfig
import com.ibm.mq.spark.core.message.MQMessage

import scala.collection.mutable
import scala.util.{Success, Try}

/**
 * Mock implementation of MQClient for unit testing.
 *
 * Provides an in-memory queue implementation that tracks
 * all operations for verification in tests.
 */
class MockMQClient extends MQClient {

  private val messageQueue: mutable.Queue[MQMessage] = mutable.Queue.empty
  private val sentMessages: mutable.ListBuffer[MQMessage] = mutable.ListBuffer.empty
  private var connected: Boolean = false
  private var connectionConfig: Option[MQConnectorConfig] = None

  private var connectCount: Int = 0
  private var disconnectCount: Int = 0
  private var commitCount: Int = 0
  private var rollbackCount: Int = 0

  override def connect(config: MQConnectorConfig): Try[Unit] = {
    connectCount += 1
    connectionConfig = Some(config)
    connected = true
    Success(())
  }

  override def receive(): Option[MQMessage] = {
    if (messageQueue.nonEmpty) Some(messageQueue.dequeue())
    else None
  }

  override def send(message: MQMessage): Try[Unit] = {
    sentMessages += message
    Success(())
  }

  override def isConnected: Boolean = connected

  override def commit(): Try[Unit] = {
    commitCount += 1
    Success(())
  }

  override def rollback(): Try[Unit] = {
    rollbackCount += 1
    Success(())
  }

  override def close(): Unit = {
    disconnectCount += 1
    connected = false
    connectionConfig = None
  }

  /**
   * Enqueues messages for retrieval via receive().
   *
   * @param messages the messages to enqueue
   */
  def enqueueMessages(messages: MQMessage*): Unit = {
    messages.foreach(messageQueue.enqueue(_))
  }

  /**
   * Returns all messages sent via send().
   *
   * @return the sent messages
   */
  def getSentMessages: Seq[MQMessage] = sentMessages.toSeq

  /**
   * Returns the configuration used in the last connect() call.
   *
   * @return the connection configuration
   */
  def getConnectionConfig: Option[MQConnectorConfig] = connectionConfig

  /**
   * Returns the number of connect() calls.
   *
   * @return the connect count
   */
  def getConnectCount: Int = connectCount

  /**
   * Returns the number of close() calls.
   *
   * @return the disconnect count
   */
  def getDisconnectCount: Int = disconnectCount

  /**
   * Returns the number of commit() calls.
   *
   * @return the commit count
   */
  def getCommitCount: Int = commitCount

  /**
   * Returns the number of rollback() calls.
   *
   * @return the rollback count
   */
  def getRollbackCount: Int = rollbackCount

  /**
   * Resets all state for a fresh test.
   */
  def reset(): Unit = {
    messageQueue.clear()
    sentMessages.clear()
    connected = false
    connectionConfig = None
    connectCount = 0
    disconnectCount = 0
    commitCount = 0
    rollbackCount = 0
  }
}
