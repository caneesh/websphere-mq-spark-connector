package com.ibm.mq.spark.core.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ParsedPayloadResultSpec extends AnyFlatSpec with Matchers {

  "ParsedPayloadResult" should "represent successful JSON parse" in {
    val result = ParsedPayloadResult.success(
      format = PayloadFormat.Json,
      parsedData = Map("key" -> "value"),
      originalSize = 20
    )

    result.isSuccess shouldBe true
    result.format shouldBe PayloadFormat.Json
    result.parsedData shouldBe Some(Map("key" -> "value"))
    result.error shouldBe None
  }

  it should "represent successful XML parse" in {
    val result = ParsedPayloadResult.success(
      format = PayloadFormat.Xml,
      parsedData = "<root>data</root>",
      originalSize = 17
    )

    result.isSuccess shouldBe true
    result.format shouldBe PayloadFormat.Xml
  }

  it should "represent parse failure" in {
    val result = ParsedPayloadResult.failure(
      format = PayloadFormat.Json,
      error = "Invalid JSON: unexpected token",
      originalPayload = "{invalid",
      originalSize = 8
    )

    result.isSuccess shouldBe false
    result.error shouldBe Some("Invalid JSON: unexpected token")
    result.originalPayload shouldBe Some("{invalid")
  }

  it should "support text format" in {
    val result = ParsedPayloadResult.success(
      format = PayloadFormat.Text,
      parsedData = "plain text message",
      originalSize = 18
    )

    result.format shouldBe PayloadFormat.Text
    result.parsedData shouldBe Some("plain text message")
  }

  it should "support binary format" in {
    val result = ParsedPayloadResult.binary(
      data = Array[Byte](1, 2, 3),
      originalSize = 3
    )

    result.format shouldBe PayloadFormat.Binary
    result.isSuccess shouldBe true
  }

  it should "track parsing duration" in {
    val startTime = System.nanoTime()
    Thread.sleep(10)
    val endTime = System.nanoTime()

    val result = ParsedPayloadResult.success(
      format = PayloadFormat.Json,
      parsedData = Map.empty,
      originalSize = 2,
      parsingDurationNanos = endTime - startTime
    )

    result.parsingDurationNanos should be > 0L
  }

  "PayloadFormat" should "detect format from content" in {
    PayloadFormat.detect("{\"key\":\"value\"}".getBytes) shouldBe PayloadFormat.Json
    PayloadFormat.detect("[1,2,3]".getBytes) shouldBe PayloadFormat.Json
    PayloadFormat.detect("<?xml version=\"1.0\"?><r/>".getBytes) shouldBe PayloadFormat.Xml
    PayloadFormat.detect("<root>data</root>".getBytes) shouldBe PayloadFormat.Xml
    PayloadFormat.detect("plain text".getBytes) shouldBe PayloadFormat.Text
    PayloadFormat.detect(Array[Byte](0, 1, 2)) shouldBe PayloadFormat.Binary
  }
}
