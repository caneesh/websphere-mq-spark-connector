package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.interfaces.PayloadParser
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}

import java.nio.charset.Charset

/**
 * Parser for plain text message payloads.
 * This parser always succeeds and serves as a fallback.
 */
case class TextPayloadParser() extends PayloadParser {

  override def format: PayloadFormat = PayloadFormat.Text

  override def canParse(message: RawMQMessage): Boolean = true

  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    val startTime = System.nanoTime()
    val payloadBytes = message.payload
    val originalSize = payloadBytes.length

    try {
      val charset = ccsidToCharset(message.ccsid)
      val text = new String(payloadBytes, charset)
      val duration = System.nanoTime() - startTime

      ParsedPayloadResult.success(PayloadFormat.Text, text, originalSize, duration)
    } catch {
      case e: Exception =>
        val duration = System.nanoTime() - startTime
        val text = new String(payloadBytes, "UTF-8")
        ParsedPayloadResult.success(PayloadFormat.Text, text, originalSize, duration)
    }
  }

  private def ccsidToCharset(ccsid: Int): Charset = {
    ccsid match {
      case 1208 => Charset.forName("UTF-8")
      case 819 => Charset.forName("ISO-8859-1")
      case 437 => Charset.forName("IBM437")
      case 500 => Charset.forName("IBM500")
      case 37 => Charset.forName("IBM037")
      case 1252 => Charset.forName("windows-1252")
      case _ => Charset.forName("UTF-8")
    }
  }
}
