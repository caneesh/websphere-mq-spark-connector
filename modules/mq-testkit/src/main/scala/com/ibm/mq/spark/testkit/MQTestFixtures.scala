package com.ibm.mq.spark.testkit

import com.ibm.mq.spark.core.client.{ClientStats, DefaultMQClient, MQTransport}
import com.ibm.mq.spark.core.connection.MQConnectionConfig
import com.ibm.mq.spark.core.message.RawMQMessage

import scala.collection.mutable.ListBuffer

/**
 * Mock MQ transport for testing without actual MQ connection.
 */
class MockMQTransport extends MQTransport {

  private val messageQueue = ListBuffer[RawMQMessage]()
  private var _connected: Boolean = false
  private var _failConnect: Option[String] = None
  private var _failCommit: Option[String] = None
  private var _failRollback: Option[String] = None

  var connectCount: Int = 0
  var disconnectCount: Int = 0
  var commitCount: Int = 0
  var rollbackCount: Int = 0
  var receiveCount: Int = 0

  override def connect(config: MQConnectionConfig): Unit = {
    _failConnect match {
      case Some(msg) =>
        _failConnect = None
        throw new RuntimeException(msg)
      case None =>
        _connected = true
        connectCount += 1
    }
  }

  override def disconnect(): Unit = {
    _connected = false
    disconnectCount += 1
  }

  override def isConnected: Boolean = _connected

  override def receive(waitMillis: Long): Option[RawMQMessage] = {
    receiveCount += 1
    synchronized {
      if (messageQueue.nonEmpty) {
        Some(messageQueue.remove(0))
      } else {
        None
      }
    }
  }

  override def commit(): Unit = {
    _failCommit match {
      case Some(msg) =>
        _failCommit = None
        throw new RuntimeException(msg)
      case None =>
        commitCount += 1
    }
  }

  override def rollback(): Unit = {
    _failRollback match {
      case Some(msg) =>
        _failRollback = None
        throw new RuntimeException(msg)
      case None =>
        rollbackCount += 1
    }
  }

  def enqueue(messages: RawMQMessage*): Unit = synchronized {
    messageQueue ++= messages
  }

  def clear(): Unit = synchronized {
    messageQueue.clear()
  }

  def queueSize: Int = synchronized {
    messageQueue.size
  }

  def failNextConnect(message: String): Unit = {
    _failConnect = Some(message)
  }

  def failNextCommit(message: String): Unit = {
    _failCommit = Some(message)
  }

  def failNextRollback(message: String): Unit = {
    _failRollback = Some(message)
  }

  def resetCounters(): Unit = {
    connectCount = 0
    disconnectCount = 0
    commitCount = 0
    rollbackCount = 0
    receiveCount = 0
  }
}

/**
 * Factory methods for creating test messages.
 */
object TestMessages {

  private var idCounter: Int = 0

  def json(content: Map[String, Any]): RawMQMessage = {
    val json = content.map { case (k, v) => s""""$k":"$v"""" }.mkString("{", ",", "}")
    createMessage(json.getBytes("UTF-8"), format = "JSON")
  }

  def xml(rootElement: String, content: Map[String, String]): RawMQMessage = {
    val elements = content.map { case (k, v) => s"<$k>$v</$k>" }.mkString
    val xml = s"<$rootElement>$elements</$rootElement>"
    createMessage(xml.getBytes("UTF-8"), format = "XML")
  }

  def text(content: String): RawMQMessage = {
    createMessage(content.getBytes("UTF-8"), format = "MQSTR")
  }

  def binary(data: Array[Byte]): RawMQMessage = {
    createMessage(data, format = "MQHRF2")
  }

  def withBackout(backoutCount: Int, content: String = "test"): RawMQMessage = {
    createMessage(content.getBytes("UTF-8"), backoutCount = backoutCount)
  }

  def large(sizeBytes: Int): RawMQMessage = {
    createMessage(Array.fill[Byte](sizeBytes)(0x41.toByte))
  }

  private def createMessage(
      payload: Array[Byte],
      format: String = "MQSTR",
      backoutCount: Int = 0
  ): RawMQMessage = {
    idCounter += 1
    val msgId = f"$idCounter%08x".getBytes("UTF-8")

    RawMQMessage(
      messageId = msgId,
      correlationId = Array.emptyByteArray,
      payload = payload,
      putTimestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = backoutCount,
      format = format
    )
  }

  def resetCounter(): Unit = {
    idCounter = 0
  }
}

/**
 * Test fixture for MQ client testing.
 */
case class MQClientTestFixture(
    transport: MockMQTransport = new MockMQTransport(),
    autoConnect: Boolean = true
) {

  val client: DefaultMQClient = new DefaultMQClient(transport)

  def setup(): Unit = {
    transport.resetCounters()
    TestMessages.resetCounter()
  }

  def teardown(): Unit = {
    if (client.isConnected) {
      client.close()
    }
    transport.clear()
  }
}
