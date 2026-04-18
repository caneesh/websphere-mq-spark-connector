package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.PayloadFormat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PayloadParserSpec extends AnyFlatSpec with Matchers {

  "JsonPayloadParser" should "parse valid JSON object" in {
    val message = createMessage("""{"name": "test", "value": 123}""")
    val parser = JsonPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe true
    result.format shouldBe PayloadFormat.Json
    val data = result.parsedData.get.asInstanceOf[Map[String, Any]]
    data("name") shouldBe "test"
    data("value") shouldBe 123
  }

  it should "parse JSON array" in {
    val message = createMessage("""[1, 2, 3]""")
    val parser = JsonPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe true
    result.parsedData.get shouldBe a[Seq[_]]
  }

  it should "return failure for invalid JSON without crashing" in {
    val message = createMessage("""{"invalid": }""")
    val parser = JsonPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe false
    result.error should be(defined)
    result.originalPayload should be(defined)
  }

  it should "detect JSON format from message format field" in {
    val parser = JsonPayloadParser()

    parser.canParse(createMessageWithFormat("MQSTR")) shouldBe true
    parser.canParse(createMessageWithFormat("JSON")) shouldBe true
    parser.canParse(createMessageWithFormat("")) shouldBe true
  }

  "XmlPayloadParser" should "parse valid XML" in {
    val xml = """<root><item id="1">value</item></root>"""
    val message = createMessage(xml)
    val parser = XmlPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe true
    result.format shouldBe PayloadFormat.Xml
  }

  it should "extract element text and attributes" in {
    val xml = """<order id="123"><product>Widget</product></order>"""
    val message = createMessage(xml)
    val parser = XmlPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe true
    val data = result.parsedData.get.asInstanceOf[Map[String, Any]]
    data should contain key "order"
  }

  it should "return failure for invalid XML without crashing" in {
    val message = createMessage("""<invalid><unclosed>""")
    val parser = XmlPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe false
    result.error should be(defined)
  }

  it should "detect XML format" in {
    val parser = XmlPayloadParser()

    parser.canParse(createMessageWithFormat("XML")) shouldBe true
    parser.canParse(createMessage("<?xml version=\"1.0\"?>")) shouldBe true
    parser.canParse(createMessage("<root/>")) shouldBe true
  }

  "TextPayloadParser" should "parse payload as plain text" in {
    val message = createMessage("Hello World")
    val parser = TextPayloadParser()

    val result = parser.parse(message)

    result.isSuccess shouldBe true
    result.format shouldBe PayloadFormat.Text
    result.parsedData.get shouldBe "Hello World"
  }

  it should "handle different character encodings via CCSID" in {
    val parser = TextPayloadParser()
    val message = createMessageWithCcsid("test", 1208)

    val result = parser.parse(message)

    result.isSuccess shouldBe true
    result.parsedData.get shouldBe "test"
  }

  it should "always succeed for any input" in {
    val parser = TextPayloadParser()

    parser.parse(createMessage("")).isSuccess shouldBe true
    parser.parse(createMessage("random bytes")).isSuccess shouldBe true
  }

  "CompositePayloadParser" should "try parsers in order" in {
    val parser = CompositePayloadParser(Seq(
      JsonPayloadParser(),
      XmlPayloadParser(),
      TextPayloadParser()
    ))

    val jsonResult = parser.parse(createMessage("""{"key": "value"}"""))
    jsonResult.isSuccess shouldBe true
    jsonResult.format shouldBe PayloadFormat.Json

    val xmlResult = parser.parse(createMessage("<root/>"))
    xmlResult.isSuccess shouldBe true
    xmlResult.format shouldBe PayloadFormat.Xml
  }

  it should "fall back to next parser on failure" in {
    val parser = CompositePayloadParser(Seq(
      JsonPayloadParser(),
      TextPayloadParser()
    ))

    val result = parser.parse(createMessage("not json"))

    result.isSuccess shouldBe true
    result.format shouldBe PayloadFormat.Text
  }

  it should "return last failure if no parser succeeds" in {
    val parser = CompositePayloadParser(Seq(
      JsonPayloadParser(),
      XmlPayloadParser()
    ))

    val result = parser.parse(createMessage("not json or xml"))

    result.isSuccess shouldBe false
    result.error should be(defined)
  }

  it should "use format hint to select parser" in {
    val parser = CompositePayloadParser(Seq(
      JsonPayloadParser(),
      XmlPayloadParser(),
      TextPayloadParser()
    ))

    val message = createMessageWithFormat("XML")
    val result = parser.parse(message)

    result.isSuccess shouldBe true
  }

  it should "report original size correctly" in {
    val payload = "test payload"
    val parser = TextPayloadParser()

    val result = parser.parse(createMessage(payload))

    result.originalSize shouldBe payload.getBytes("UTF-8").length
  }

  it should "track parsing duration" in {
    val parser = JsonPayloadParser()
    val message = createMessage("""{"key": "value"}""")

    val result = parser.parse(message)

    result.parsingDurationNanos should be >= 0L
  }

  private def createMessage(payload: String): RawMQMessage = {
    RawMQMessage(
      messageId = Array(1, 2, 3),
      correlationId = Array.emptyByteArray,
      payload = payload.getBytes("UTF-8"),
      putTimestamp = System.currentTimeMillis(),
      queueName = "TEST.QUEUE",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = 0,
      format = ""
    )
  }

  private def createMessageWithFormat(format: String): RawMQMessage = {
    createMessage("test").copy(format = format)
  }

  private def createMessageWithCcsid(payload: String, ccsid: Int): RawMQMessage = {
    createMessage(payload).copy(ccsid = ccsid)
  }
}
