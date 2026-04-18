package com.ibm.mq.spark.source.schema

import com.ibm.mq.spark.core.message.RawMQMessage
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType

import java.nio.charset.Charset
import java.sql.Timestamp

/**
 * Converts MQ messages to Spark Rows based on a target schema.
 */
case class MQRowConverter(schema: StructType) {

  private val fieldNames = schema.fieldNames.toSet
  private val fieldExtractors = buildExtractors()

  /**
   * Converts a single message to a Spark Row.
   */
  def toRow(message: RawMQMessage): Row = {
    val values = fieldExtractors.map(_.apply(message)).toArray
    new GenericRowWithSchema(values, schema)
  }

  /**
   * Converts multiple messages to Spark Rows.
   */
  def toRows(messages: Seq[RawMQMessage]): Seq[Row] = {
    messages.map(toRow)
  }

  private def buildExtractors(): Seq[RawMQMessage => Any] = {
    schema.fields.map { field =>
      field.name match {
        case "messageId" => (m: RawMQMessage) => m.messageId
        case "correlationId" => (m: RawMQMessage) => m.correlationId
        case "payload" => (m: RawMQMessage) => m.payload
        case "payloadString" => (m: RawMQMessage) => decodePayload(m)
        case "timestamp" => (m: RawMQMessage) => new Timestamp(m.putTimestamp)
        case "queueName" => (m: RawMQMessage) => m.queueName
        case "ccsid" => (m: RawMQMessage) => m.ccsid
        case "encoding" => (m: RawMQMessage) => m.encoding
        case "priority" => (m: RawMQMessage) => m.priority
        case "expiry" => (m: RawMQMessage) => m.expiry
        case "backoutCount" => (m: RawMQMessage) => m.backoutCount
        case "replyToQueue" => (m: RawMQMessage) => m.replyToQueue.orNull
        case "replyToQueueManager" => (m: RawMQMessage) => m.replyToQueueManager.orNull
        case "format" => (m: RawMQMessage) => m.format
        case "persistence" => (m: RawMQMessage) => m.persistence
        case "messageType" => (m: RawMQMessage) => m.messageType
        case "userId" => (m: RawMQMessage) => m.userId.orNull
        case "applicationName" => (m: RawMQMessage) => m.applicationName.orNull
        case "putApplicationType" => (m: RawMQMessage) => m.putApplicationType
        case other => (_: RawMQMessage) => null
      }
    }
  }

  private def decodePayload(message: RawMQMessage): String = {
    try {
      val charset = ccsidToCharset(message.ccsid)
      new String(message.payload, charset)
    } catch {
      case _: Exception => null
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
