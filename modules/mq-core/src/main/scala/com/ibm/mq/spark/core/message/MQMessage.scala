package com.ibm.mq.spark.core.message

/**
 * Represents an IBM MQ message with payload and metadata.
 *
 * @param messageId unique identifier for the message
 * @param correlationId correlation identifier for request-reply patterns
 * @param payload the message body as bytes
 * @param timestamp message creation timestamp in milliseconds since epoch
 * @param properties additional message properties
 * @param format the message format descriptor
 * @param expiry message expiry in tenths of a second, -1 for unlimited
 */
case class MQMessage(
    messageId: Array[Byte],
    correlationId: Array[Byte],
    payload: Array[Byte],
    timestamp: Long,
    properties: Map[String, Any] = Map.empty,
    format: String = "",
    expiry: Int = -1
) {

  /**
   * Returns the payload as a UTF-8 string.
   *
   * @return the payload decoded as UTF-8
   */
  def payloadAsString: String = new String(payload, "UTF-8")

  /**
   * Returns the message ID as a hexadecimal string.
   *
   * @return hex representation of the message ID
   */
  def messageIdHex: String = bytesToHex(messageId)

  /**
   * Returns the correlation ID as a hexadecimal string.
   *
   * @return hex representation of the correlation ID
   */
  def correlationIdHex: String = bytesToHex(correlationId)

  private def bytesToHex(bytes: Array[Byte]): String = {
    if (bytes == null || bytes.isEmpty) ""
    else bytes.map("%02x".format(_)).mkString
  }
}

object MQMessage {

  /**
   * Creates an MQMessage with a string payload.
   *
   * @param payload the message content as a string
   * @return a new MQMessage instance
   */
  def fromString(payload: String): MQMessage = {
    val now = System.currentTimeMillis()
    MQMessage(
      messageId = Array.emptyByteArray,
      correlationId = Array.emptyByteArray,
      payload = payload.getBytes("UTF-8"),
      timestamp = now
    )
  }
}
