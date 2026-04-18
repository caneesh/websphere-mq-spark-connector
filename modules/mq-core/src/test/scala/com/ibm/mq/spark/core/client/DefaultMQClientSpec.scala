package com.ibm.mq.spark.core.client

import com.ibm.mq.spark.core.config.{MQConnectorConfig, MQConsumerConfig, MQSecurityConfig}
import com.ibm.mq.spark.core.connection.{MQConnectionConfig, MQConnectionState}
import com.ibm.mq.spark.core.message.RawMQMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import scala.util.{Failure, Success}

class DefaultMQClientSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var mockTransport: MockMQTransport = _
  private var client: DefaultMQClient = _

  override def beforeEach(): Unit = {
    mockTransport = new MockMQTransport()
    client = new DefaultMQClient(mockTransport)
  }

  override def afterEach(): Unit = {
    if (client != null) client.close()
  }

  "DefaultMQClient" should "connect using provided configuration" in {
    val connConfig = MQConnectorConfig(
      queueManager = "QM1",
      channel = "DEV.APP.SVRCONN",
      connectionName = "localhost(1414)",
      queueName = "DEV.QUEUE.1"
    )

    val result = client.connect(connConfig)

    result.isSuccess shouldBe true
    client.isConnected shouldBe true
    client.connectionState shouldBe MQConnectionState.Connected
  }

  it should "report connection failure" in {
    mockTransport.failNextConnect("Connection refused")

    val connConfig = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CH1",
      connectionName = "localhost(1414)",
      queueName = "Q1"
    )

    val result = client.connect(connConfig)

    result.isFailure shouldBe true
    client.isConnected shouldBe false
  }

  it should "poll messages in batches" in {
    connectClient()

    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2"),
      createMessage("aabb03", "payload3")
    )

    val messages = client.poll(batchSize = 2, waitMillis = 1000)

    messages should have size 2
    messages.head.messageIdHex shouldBe "aabb01"
    messages(1).messageIdHex shouldBe "aabb02"
  }

  it should "return empty when queue is empty" in {
    connectClient()

    val messages = client.poll(batchSize = 10, waitMillis = 100)

    messages shouldBe empty
  }

  it should "capture all message metadata" in {
    connectClient()

    val rawMsg = RawMQMessage(
      messageId = Array(1, 2, 3),
      correlationId = Array(4, 5, 6),
      payload = "test".getBytes("UTF-8"),
      putTimestamp = 1234567890L,
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 5,
      expiry = 600,
      backoutCount = 2,
      replyToQueue = Some("REPLY.Q"),
      replyToQueueManager = Some("QM1"),
      format = "MQSTR",
      persistence = 1,
      messageType = 8,
      userId = Some("testuser"),
      applicationName = Some("TestApp"),
      putApplicationType = 28
    )
    mockTransport.enqueueMessages(rawMsg)

    val messages = client.poll(1, 1000)

    messages should have size 1
    val msg = messages.head
    msg.ccsid shouldBe 1208
    msg.encoding shouldBe 546
    msg.priority shouldBe 5
    msg.expiry shouldBe 600
    msg.backoutCount shouldBe 2
    msg.isRedelivered shouldBe true
    msg.replyToQueue shouldBe Some("REPLY.Q")
    msg.format shouldBe "MQSTR"
    msg.userId shouldBe Some("testuser")
  }

  it should "commit transaction" in {
    connectClient()
    mockTransport.enqueueMessages(createMessage("cc0001", "data"))

    client.poll(1, 1000)
    val result = client.commit()

    result.isSuccess shouldBe true
    mockTransport.commitCount shouldBe 1
  }

  it should "rollback transaction" in {
    connectClient()
    mockTransport.enqueueMessages(createMessage("cc0001", "data"))

    client.poll(1, 1000)
    val result = client.rollback()

    result.isSuccess shouldBe true
    mockTransport.rollbackCount shouldBe 1
  }

  it should "handle commit failure" in {
    connectClient()
    mockTransport.failNextCommit("Session expired")

    val result = client.commit()

    result.isFailure shouldBe true
    result.failed.get.getMessage should include("Session expired")
  }

  it should "track poll statistics" in {
    connectClient()
    mockTransport.enqueueMessages(
      createMessage("dd0001", "data1"),
      createMessage("dd0002", "data2")
    )

    client.poll(10, 1000)

    client.stats.messagesPolled shouldBe 2
    client.stats.pollCount shouldBe 1
    client.stats.lastPollTime should be > 0L
  }

  it should "disconnect cleanly" in {
    connectClient()

    client.close()

    client.isConnected shouldBe false
    mockTransport.disconnectCount shouldBe 1
  }

  private def connectClient(): Unit = {
    val config = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CH1",
      connectionName = "localhost(1414)",
      queueName = "Q1"
    )
    client.connect(config)
  }

  private def createMessage(msgId: String, payload: String): RawMQMessage = {
    RawMQMessage(
      messageId = hexStringToBytes(msgId),
      correlationId = Array.emptyByteArray,
      payload = payload.getBytes("UTF-8"),
      putTimestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = 0
    )
  }

  private def hexStringToBytes(hex: String): Array[Byte] = {
    hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  }
}

class MockMQTransport extends MQTransport {
  private var messages: List[RawMQMessage] = Nil
  private var connected: Boolean = false
  private var failConnect: Option[String] = None
  private var failCommit: Option[String] = None

  var commitCount: Int = 0
  var rollbackCount: Int = 0
  var disconnectCount: Int = 0

  def enqueueMessages(msgs: RawMQMessage*): Unit = {
    messages = messages ++ msgs.toList
  }

  def failNextConnect(reason: String): Unit = {
    failConnect = Some(reason)
  }

  def failNextCommit(reason: String): Unit = {
    failCommit = Some(reason)
  }

  override def connect(config: MQConnectionConfig): Unit = {
    failConnect match {
      case Some(reason) =>
        failConnect = None
        throw new RuntimeException(reason)
      case None =>
        connected = true
    }
  }

  override def disconnect(): Unit = {
    connected = false
    disconnectCount += 1
  }

  override def isConnected: Boolean = connected

  override def receive(waitMillis: Long): Option[RawMQMessage] = {
    messages match {
      case head :: tail =>
        messages = tail
        Some(head)
      case Nil => None
    }
  }

  override def commit(): Unit = {
    failCommit match {
      case Some(reason) =>
        failCommit = None
        throw new RuntimeException(reason)
      case None =>
        commitCount += 1
    }
  }

  override def rollback(): Unit = {
    rollbackCount += 1
  }
}
