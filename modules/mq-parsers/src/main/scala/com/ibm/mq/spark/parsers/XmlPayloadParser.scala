package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.interfaces.PayloadParser
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}

import scala.xml.{Elem, Node, XML}

/**
 * Parser for XML-formatted message payloads.
 */
case class XmlPayloadParser() extends PayloadParser {

  override def format: PayloadFormat = PayloadFormat.Xml

  override def canParse(message: RawMQMessage): Boolean = {
    val fmt = Option(message.format).getOrElse("").toUpperCase
    fmt == "XML" || fmt.contains("XML") || looksLikeXml(message)
  }

  override def parse(message: RawMQMessage): ParsedPayloadResult = {
    val startTime = System.nanoTime()
    val payloadBytes = message.payload
    val originalSize = payloadBytes.length

    try {
      val xmlString = new String(payloadBytes, "UTF-8")
      val elem = XML.loadString(xmlString)
      val parsed = xmlToMap(elem)
      val duration = System.nanoTime() - startTime

      ParsedPayloadResult.success(PayloadFormat.Xml, parsed, originalSize, duration)
    } catch {
      case e: Exception =>
        val duration = System.nanoTime() - startTime
        ParsedPayloadResult.failure(
          PayloadFormat.Xml,
          e.getMessage,
          new String(payloadBytes, "UTF-8"),
          originalSize,
          duration
        )
    }
  }

  private def xmlToMap(elem: Elem): Map[String, Any] = {
    Map(elem.label -> nodeToValue(elem))
  }

  private def nodeToValue(node: Node): Any = {
    val attributes = node.attributes.asAttrMap
    val children = node.child.filter(n => n.isInstanceOf[Elem])

    if (children.isEmpty) {
      if (attributes.isEmpty) {
        node.text.trim
      } else {
        attributes + ("_text" -> node.text.trim)
      }
    } else {
      val childMap = children.groupBy(_.label).map { case (label, nodes) =>
        if (nodes.size == 1) {
          label -> nodeToValue(nodes.head)
        } else {
          label -> nodes.map(nodeToValue).toList
        }
      }
      if (attributes.isEmpty) {
        childMap
      } else {
        childMap ++ attributes.map { case (k, v) => s"@$k" -> v }
      }
    }
  }

  private def looksLikeXml(message: RawMQMessage): Boolean = {
    if (message.payload.isEmpty) return false
    val trimmed = new String(message.payload, "UTF-8").trim
    trimmed.startsWith("<?xml") || (trimmed.startsWith("<") && trimmed.endsWith(">"))
  }
}
