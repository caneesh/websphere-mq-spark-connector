package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.interfaces.PayloadParser
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}

/**
 * Parser for JSON-formatted message payloads.
 * Uses a simple recursive descent parser to avoid external dependencies.
 */
case class JsonPayloadParser() extends PayloadParser {

  override def format: PayloadFormat = PayloadFormat.Json

  override def canParse(message: RawMQMessage): Boolean = {
    val fmt = Option(message.format).getOrElse("").toUpperCase
    fmt.isEmpty || fmt == "MQSTR" || fmt == "JSON" || fmt.contains("JSON") || looksLikeJson(message)
  }

  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    val startTime = System.nanoTime()
    val payloadBytes = message.payload
    val originalSize = payloadBytes.length

    try {
      val jsonString = new String(payloadBytes, "UTF-8")
      val parsed = SimpleJsonParser.parse(jsonString)
      val duration = System.nanoTime() - startTime

      parsed match {
        case Right(data) =>
          ParsedPayloadResult.success(PayloadFormat.Json, data, originalSize, duration)
        case Left(error) =>
          ParsedPayloadResult.failure(
            PayloadFormat.Json,
            error,
            jsonString,
            originalSize,
            duration
          )
      }
    } catch {
      case e: Exception =>
        val duration = System.nanoTime() - startTime
        ParsedPayloadResult.failure(
          PayloadFormat.Json,
          e.getMessage,
          new String(payloadBytes, "UTF-8"),
          originalSize,
          duration
        )
    }
  }

  private def looksLikeJson(message: RawMQMessage): Boolean = {
    if (message.payload.isEmpty) return false
    val trimmed = new String(message.payload, "UTF-8").trim
    (trimmed.startsWith("{") && trimmed.endsWith("}")) ||
      (trimmed.startsWith("[") && trimmed.endsWith("]"))
  }
}

/**
 * Simple JSON parser without external dependencies.
 */
private[parsers] object SimpleJsonParser {

  def parse(json: String): Either[String, Any] = {
    val trimmed = json.trim
    if (trimmed.isEmpty) {
      Right(Map.empty[String, Any])
    } else {
      try {
        val (value, _) = parseValue(trimmed, 0)
        Right(value)
      } catch {
        case e: Exception => Left(s"JSON parse error: ${e.getMessage}")
      }
    }
  }

  private def parseValue(json: String, pos: Int): (Any, Int) = {
    val p = skipWhitespace(json, pos)
    if (p >= json.length) throw new RuntimeException("Unexpected end of input")

    json.charAt(p) match {
      case '{' => parseObject(json, p)
      case '[' => parseArray(json, p)
      case '"' => parseString(json, p)
      case c if c == '-' || c.isDigit => parseNumber(json, p)
      case 't' => parseTrue(json, p)
      case 'f' => parseFalse(json, p)
      case 'n' => parseNull(json, p)
      case c => throw new RuntimeException(s"Unexpected character '$c' at position $p")
    }
  }

  private def parseObject(json: String, pos: Int): (Map[String, Any], Int) = {
    var p = pos + 1
    var result = Map.empty[String, Any]

    p = skipWhitespace(json, p)
    if (p < json.length && json.charAt(p) == '}') {
      return (result, p + 1)
    }

    while (p < json.length) {
      p = skipWhitespace(json, p)
      val (key, p1) = parseString(json, p)
      p = skipWhitespace(json, p1)
      if (json.charAt(p) != ':') throw new RuntimeException(s"Expected ':' at position $p")
      p = skipWhitespace(json, p + 1)
      val (value, p2) = parseValue(json, p)
      result = result + (key.asInstanceOf[String] -> value)
      p = skipWhitespace(json, p2)

      json.charAt(p) match {
        case ',' => p += 1
        case '}' => return (result, p + 1)
        case c => throw new RuntimeException(s"Expected ',' or '}}' at position $p, got '$c'")
      }
    }
    throw new RuntimeException("Unexpected end of object")
  }

  private def parseArray(json: String, pos: Int): (Seq[Any], Int) = {
    var p = pos + 1
    var result = Seq.empty[Any]

    p = skipWhitespace(json, p)
    if (p < json.length && json.charAt(p) == ']') {
      return (result, p + 1)
    }

    while (p < json.length) {
      p = skipWhitespace(json, p)
      val (value, p1) = parseValue(json, p)
      result = result :+ value
      p = skipWhitespace(json, p1)

      json.charAt(p) match {
        case ',' => p += 1
        case ']' => return (result, p + 1)
        case c => throw new RuntimeException(s"Expected ',' or ']' at position $p, got '$c'")
      }
    }
    throw new RuntimeException("Unexpected end of array")
  }

  private def parseString(json: String, pos: Int): (String, Int) = {
    if (json.charAt(pos) != '"') throw new RuntimeException(s"Expected quote at position $pos")
    var p = pos + 1
    val sb = new StringBuilder

    while (p < json.length) {
      val c = json.charAt(p)
      if (c == '"') {
        return (sb.toString, p + 1)
      } else if (c == '\\') {
        p += 1
        if (p >= json.length) throw new RuntimeException("Unexpected end of string")
        json.charAt(p) match {
          case '"' => sb.append('"')
          case '\\' => sb.append('\\')
          case '/' => sb.append('/')
          case 'b' => sb.append('\b')
          case 'f' => sb.append('\f')
          case 'n' => sb.append('\n')
          case 'r' => sb.append('\r')
          case 't' => sb.append('\t')
          case 'u' =>
            val hex = json.substring(p + 1, p + 5)
            sb.append(Integer.parseInt(hex, 16).toChar)
            p += 4
          case _ => throw new RuntimeException(s"Invalid escape sequence at position $p")
        }
        p += 1
      } else {
        sb.append(c)
        p += 1
      }
    }
    throw new RuntimeException("Unterminated string")
  }

  private def parseNumber(json: String, pos: Int): (Any, Int) = {
    var p = pos
    val sb = new StringBuilder

    if (json.charAt(p) == '-') {
      sb.append('-')
      p += 1
    }

    while (p < json.length && (json.charAt(p).isDigit || json.charAt(p) == '.' ||
           json.charAt(p) == 'e' || json.charAt(p) == 'E' ||
           json.charAt(p) == '+' || json.charAt(p) == '-')) {
      sb.append(json.charAt(p))
      p += 1
    }

    val numStr = sb.toString
    val num = if (numStr.contains('.') || numStr.contains('e') || numStr.contains('E')) {
      numStr.toDouble
    } else {
      val n = numStr.toLong
      if (n >= Int.MinValue && n <= Int.MaxValue) n.toInt else n
    }
    (num, p)
  }

  private def parseTrue(json: String, pos: Int): (Boolean, Int) = {
    if (json.substring(pos, pos + 4) == "true") (true, pos + 4)
    else throw new RuntimeException(s"Expected 'true' at position $pos")
  }

  private def parseFalse(json: String, pos: Int): (Boolean, Int) = {
    if (json.substring(pos, pos + 5) == "false") (false, pos + 5)
    else throw new RuntimeException(s"Expected 'false' at position $pos")
  }

  private def parseNull(json: String, pos: Int): (Any, Int) = {
    if (json.substring(pos, pos + 4) == "null") (null, pos + 4)
    else throw new RuntimeException(s"Expected 'null' at position $pos")
  }

  private def skipWhitespace(json: String, pos: Int): Int = {
    var p = pos
    while (p < json.length && json.charAt(p).isWhitespace) p += 1
    p
  }
}
