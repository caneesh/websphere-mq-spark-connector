package com.ibm.mq.spark.testkit

import com.ibm.mq.spark.core.config.MQConnectorConfig
import com.ibm.mq.spark.core.message.MQMessage
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

/**
 * Test support trait providing common utilities for MQ connector tests.
 *
 * Mix this trait into ScalaTest test classes to gain access to
 * test fixtures, configuration builders, and assertion helpers.
 */
trait MQTestSupport extends BeforeAndAfterEach with BeforeAndAfterAll { this: Suite =>

  /**
   * Creates a test configuration with default values.
   *
   * @param queueName optional queue name override
   * @return a test configuration
   */
  protected def createTestConfig(queueName: String = "TEST.QUEUE"): MQConnectorConfig = {
    MQConnectorConfig(
      queueManager = "TEST.QM",
      channel = "TEST.CHANNEL",
      connectionName = "localhost(1414)",
      queueName = queueName
    )
  }

  /**
   * Creates a test message with the given payload.
   *
   * @param payload the message content
   * @return a test message
   */
  protected def createTestMessage(payload: String): MQMessage = {
    MQMessage.fromString(payload)
  }

  /**
   * Creates a test message with binary payload.
   *
   * @param payload the message bytes
   * @return a test message
   */
  protected def createTestMessage(payload: Array[Byte]): MQMessage = {
    MQMessage(
      messageId = Array.emptyByteArray,
      correlationId = Array.emptyByteArray,
      payload = payload,
      timestamp = System.currentTimeMillis()
    )
  }

  /**
   * Generates a sequence of test messages.
   *
   * @param count the number of messages to generate
   * @param prefix the payload prefix
   * @return a sequence of test messages
   */
  protected def generateTestMessages(count: Int, prefix: String = "message"): Seq[MQMessage] = {
    (1 to count).map(i => createTestMessage(s"$prefix-$i"))
  }

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
  }
}
