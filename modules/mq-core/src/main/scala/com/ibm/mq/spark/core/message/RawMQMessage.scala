package com.ibm.mq.spark.core.message

case class RawMQMessage(
    messageId: Array[Byte],
    correlationId: Array[Byte],
    payload: Array[Byte],
    putTimestamp: Long,
    queueName: String,
    ccsid: Int,
    encoding: Int,
    priority: Int,
    expiry: Int,
    backoutCount: Int,
    replyToQueue: Option[String] = None,
    replyToQueueManager: Option[String] = None,
    format: String = "",
    persistence: Int = 0,
    messageType: Int = 8,
    userId: Option[String] = None,
    applicationName: Option[String] = None,
    putApplicationType: Int = 0,
    groupId: Option[Array[Byte]] = None,
    messageSequenceNumber: Int = 1,
    offset: Int = 0
) {

  def messageIdHex: String = bytesToHex(messageId)

  def correlationIdHex: String = bytesToHex(correlationId)

  def payloadAsString: String = new String(payload, "UTF-8")

  def payloadSize: Int = payload.length

  def isRedelivered: Boolean = backoutCount > 0

  private def bytesToHex(bytes: Array[Byte]): String = {
    if (bytes == null || bytes.isEmpty) ""
    else bytes.map("%02x".format(_)).mkString
  }
}
