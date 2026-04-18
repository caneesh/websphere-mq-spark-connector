package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.message.MQMessage
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MessageParserSpec extends AnyFlatSpec with Matchers {

  "JsonMessageParser" should "have the correct name" in {
    JsonMessageParser.name shouldBe "JsonMessageParser"
  }

  it should "support JSON format" in {
    JsonMessageParser.supportsFormat("JSON") shouldBe true
    JsonMessageParser.supportsFormat("json") shouldBe true
  }

  it should "support MQSTR format" in {
    JsonMessageParser.supportsFormat("MQSTR") shouldBe true
  }

  it should "support empty format" in {
    JsonMessageParser.supportsFormat("") shouldBe true
    JsonMessageParser.supportsFormat(null) shouldBe true
  }

  it should "parse payload from MQMessage" in {
    val message = MQMessage.fromString("""{"key": "value"}""")

    val result = JsonMessageParser.parse(message)

    result.isSuccess shouldBe true
    result.get should contain key "_raw"
  }

  it should "parse raw payload bytes" in {
    val payload = """{"name": "test"}""".getBytes("UTF-8")

    val result = JsonMessageParser.parsePayload(payload)

    result.isSuccess shouldBe true
    result.get("_raw") shouldBe """{"name": "test"}"""
  }

  it should "handle empty payload" in {
    val payload = "".getBytes("UTF-8")

    val result = JsonMessageParser.parsePayload(payload)

    result.isSuccess shouldBe true
  }
}
