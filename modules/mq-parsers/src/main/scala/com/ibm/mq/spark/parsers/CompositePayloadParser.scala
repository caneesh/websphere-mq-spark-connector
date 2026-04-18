package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.interfaces.PayloadParser
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}

/**
 * A composite parser that tries multiple parsers in sequence.
 * Uses format hints when available, falls back to trying each parser.
 *
 * @param parsers the parsers to try, in order of preference
 */
case class CompositePayloadParser(parsers: Seq[PayloadParser]) extends PayloadParser {

  require(parsers.nonEmpty, "At least one parser is required")

  override def format: PayloadFormat = PayloadFormat.Unknown

  override def canParse(message: RawMQMessage): Boolean = {
    parsers.exists(_.canParse(message))
  }

  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    val preferredParsers = parsers.filter(_.canParse(message))
    val orderedParsers = if (preferredParsers.nonEmpty) preferredParsers else parsers

    var lastFailure: ParsedPayloadResult = null

    for (parser <- orderedParsers) {
      val result = parser.parse(message)
      if (result.isSuccess) {
        return result
      }
      lastFailure = result
    }

    if (lastFailure != null) {
      lastFailure
    } else {
      ParsedPayloadResult.failure(
        PayloadFormat.Unknown,
        "No parser could handle the message",
        new String(message.payload, "UTF-8"),
        message.payload.length
      )
    }
  }
}

object CompositePayloadParser {

  /**
   * Creates a default composite parser with JSON, XML, and Text parsers.
   */
  def default: CompositePayloadParser = CompositePayloadParser(Seq(
    JsonPayloadParser(),
    XmlPayloadParser(),
    TextPayloadParser()
  ))
}
