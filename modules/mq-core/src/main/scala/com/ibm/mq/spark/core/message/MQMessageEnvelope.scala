package com.ibm.mq.spark.core.message

case class MQMessageEnvelope(
    raw: RawMQMessage,
    receivedAt: Long,
    partitionId: Int,
    offset: Long,
    sourceId: String,
    headers: Map[String, String] = Map.empty
) {

  def processingLatencyMs: Long = receivedAt - raw.putTimestamp

  def payloadAsString: String = raw.payloadAsString

  def messageIdHex: String = raw.messageIdHex

  def correlationIdHex: String = raw.correlationIdHex

  def payloadSize: Int = raw.payloadSize

  def queueName: String = raw.queueName
}
