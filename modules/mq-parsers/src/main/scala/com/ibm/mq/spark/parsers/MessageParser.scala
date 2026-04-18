package com.ibm.mq.spark.parsers

import com.ibm.mq.spark.core.message.MQMessage

import scala.util.Try

/**
 * Trait for parsing MQ message payloads into structured data.
 *
 * Implementations handle specific message formats such as JSON,
 * XML, Avro, or custom binary protocols.
 *
 * @tparam T the output type of the parsed message
 */
trait MessageParser[T] {

  /**
   * Parses an MQ message into the target type.
   *
   * @param message the MQ message to parse
   * @return Success with the parsed value, or Failure with parse error
   */
  def parse(message: MQMessage): Try[T]

  /**
   * Parses the raw payload bytes into the target type.
   *
   * @param payload the message payload bytes
   * @return Success with the parsed value, or Failure with parse error
   */
  def parsePayload(payload: Array[Byte]): Try[T]

  /**
   * Indicates whether this parser can handle the given message format.
   *
   * @param format the MQ message format descriptor
   * @return true if this parser supports the format
   */
  def supportsFormat(format: String): Boolean

  /**
   * Returns the name of this parser for logging and metrics.
   *
   * @return the parser name
   */
  def name: String
}
