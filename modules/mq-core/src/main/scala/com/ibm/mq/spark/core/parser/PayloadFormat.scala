package com.ibm.mq.spark.core.parser

sealed abstract class PayloadFormat(val name: String)

object PayloadFormat {
  case object Json extends PayloadFormat("JSON")
  case object Xml extends PayloadFormat("XML")
  case object Text extends PayloadFormat("TEXT")
  case object Binary extends PayloadFormat("BINARY")
  case object Unknown extends PayloadFormat("UNKNOWN")

  def detect(payload: Array[Byte]): PayloadFormat = {
    if (payload == null || payload.isEmpty) return Unknown

    val trimmed = trimLeadingWhitespace(payload)
    if (trimmed.isEmpty) return Text

    val firstByte = trimmed(0)

    if (firstByte == '{' || firstByte == '[') {
      Json
    } else if (firstByte == '<') {
      Xml
    } else if (isTextPayload(payload)) {
      Text
    } else {
      Binary
    }
  }

  private def trimLeadingWhitespace(bytes: Array[Byte]): Array[Byte] = {
    val start = bytes.indexWhere(b => !Character.isWhitespace(b.toChar))
    if (start < 0) Array.emptyByteArray
    else bytes.drop(start)
  }

  private def isTextPayload(bytes: Array[Byte]): Boolean = {
    bytes.forall { b =>
      val c = b & 0xFF
      c >= 0x20 && c < 0x7F || c == 0x09 || c == 0x0A || c == 0x0D
    }
  }
}
