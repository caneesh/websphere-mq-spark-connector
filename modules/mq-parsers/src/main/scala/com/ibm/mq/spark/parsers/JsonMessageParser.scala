package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.message.MQMessage

import scala.util.Try

/**
 * Parser for JSON-formatted MQ messages.
 *
 * Parses MQ message payloads containing JSON data into
 * a Map representation for further processing.
 */
object JsonMessageParser extends MessageParser[Map[String, Any]] {

  override val name: String = "JsonMessageParser"

  override def parse(message: MQMessage): Try[Map[String, Any]] = {
    parsePayload(message.payload)
  }

  override def parsePayload(payload: Array[Byte]): Try[Map[String, Any]] = {
    Try {
      val jsonString = new String(payload, "UTF-8")
      parseJsonString(jsonString)
    }
  }

  override def supportsFormat(format: String): Boolean = {
    format == null || format.isEmpty || format.equalsIgnoreCase("JSON") ||
      format.equalsIgnoreCase("MQSTR")
  }

  private def parseJsonString(json: String): Map[String, Any] = {
    Map("_raw" -> json)
  }
}
