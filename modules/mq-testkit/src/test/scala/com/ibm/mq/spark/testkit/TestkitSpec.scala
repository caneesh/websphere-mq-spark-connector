package com.ibm.mq.spark.testkit

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class TestkitSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var fixture: MQClientTestFixture = _

  override def beforeEach(): Unit = {
    fixture = MQClientTestFixture()
    fixture.setup()
  }

  override def afterEach(): Unit = {
    fixture.teardown()
  }

  "MockMQTransport" should "track connection state" in {
    val transport = fixture.transport

    transport.isConnected shouldBe false

    transport.connect(null)
    transport.isConnected shouldBe true
    transport.connectCount shouldBe 1

    transport.disconnect()
    transport.isConnected shouldBe false
    transport.disconnectCount shouldBe 1
  }

  it should "queue and dequeue messages" in {
    val transport = fixture.transport
    val msg = TestMessages.text("hello")

    transport.enqueue(msg)
    transport.queueSize shouldBe 1

    val received = transport.receive(1000)
    received shouldBe defined
    received.get.payloadAsString shouldBe "hello"
    transport.queueSize shouldBe 0
  }

  it should "support failure injection" in {
    val transport = fixture.transport
    transport.failNextConnect("Connection refused")

    an[RuntimeException] should be thrownBy transport.connect(null)
  }

  "TestMessages" should "create JSON messages" in {
    val msg = TestMessages.json(Map("key" -> "value"))

    msg.format shouldBe "JSON"
    msg.payloadAsString should include ("key")
    msg.payloadAsString should include ("value")
  }

  it should "create XML messages" in {
    val msg = TestMessages.xml("root", Map("item" -> "data"))

    msg.format shouldBe "XML"
    msg.payloadAsString should include ("<root>")
    msg.payloadAsString should include ("<item>data</item>")
  }

  it should "create text messages" in {
    val msg = TestMessages.text("plain text")

    msg.format shouldBe "MQSTR"
    msg.payloadAsString shouldBe "plain text"
  }

  it should "create messages with backout count" in {
    val msg = TestMessages.withBackout(5)

    msg.backoutCount shouldBe 5
    msg.isRedelivered shouldBe true
  }

  it should "create large messages" in {
    val msg = TestMessages.large(1000)

    msg.payloadSize shouldBe 1000
  }

  it should "generate unique message IDs" in {
    val msg1 = TestMessages.text("first")
    val msg2 = TestMessages.text("second")

    msg1.messageIdHex should not be msg2.messageIdHex
  }

  "MQClientTestFixture" should "provide configured client" in {
    fixture.client should not be null
    fixture.transport should not be null
  }

  it should "clean up on teardown" in {
    fixture.transport.enqueue(TestMessages.text("test"))
    fixture.teardown()

    fixture.transport.queueSize shouldBe 0
  }
}
