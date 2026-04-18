package com.ibm.mq.spark.core

import com.ibm.mq.spark.core.config.MQConnectorConfig
import com.ibm.mq.spark.core.message.MQMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MQCoreSpec extends AnyFlatSpec with Matchers {

  "MQConnectorConfig" should "validate required fields" in {
    val config = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CHANNEL1",
      connectionName = "localhost(1414)",
      queueName = "QUEUE1"
    )

    config.validate() shouldBe empty
  }

  it should "report validation errors for empty fields" in {
    val config = MQConnectorConfig(
      queueManager = "",
      channel = "",
      connectionName = "",
      queueName = ""
    )

    val errors = config.validate()
    errors should contain("queueManager is required")
    errors should contain("channel is required")
    errors should contain("connectionName is required")
    errors should contain("queueName is required")
  }

  it should "detect SSL configuration" in {
    val withSsl = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CHANNEL1",
      connectionName = "localhost(1414)",
      queueName = "QUEUE1",
      sslCipherSuite = Some("TLS_RSA_WITH_AES_256_CBC_SHA256")
    )

    val withoutSsl = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CHANNEL1",
      connectionName = "localhost(1414)",
      queueName = "QUEUE1"
    )

    withSsl.isSslEnabled shouldBe true
    withoutSsl.isSslEnabled shouldBe false
  }

  "MQMessage" should "create message from string" in {
    val message = MQMessage.fromString("test payload")

    message.payloadAsString shouldBe "test payload"
    message.payload should not be empty
  }

  it should "convert message ID to hex string" in {
    val message = MQMessage(
      messageId = Array(0x01, 0x02, 0x0a, 0xff.toByte),
      correlationId = Array.emptyByteArray,
      payload = Array.emptyByteArray,
      timestamp = System.currentTimeMillis()
    )

    message.messageIdHex shouldBe "01020aff"
  }

  it should "handle empty message ID" in {
    val message = MQMessage(
      messageId = Array.emptyByteArray,
      correlationId = Array.emptyByteArray,
      payload = Array.emptyByteArray,
      timestamp = System.currentTimeMillis()
    )

    message.messageIdHex shouldBe ""
  }

  "MQException" should "format error message with reason code" in {
    val exception = MQException.receiveFailed("queue empty", 2033)

    exception.toString should include("2033")
    exception.reasonCode shouldBe Some(2033)
  }

  it should "create configuration invalid exception" in {
    val exception = MQException.configurationInvalid(List("field1 missing", "field2 invalid"))

    exception.getMessage should include("field1 missing")
    exception.getMessage should include("field2 invalid")
  }
}
