package com.ibm.mq.spark.core.checkpoint

case class ConnectorCheckpoint(
    queueName: String,
    queueManager: String,
    lastMessageId: Option[String],
    lastCorrelationId: Option[String],
    lastProcessedTimestamp: Long,
    messagesProcessed: Long,
    bytesProcessed: Long,
    partitionId: Int,
    createdAt: Long
) {

  def validate(): List[String] = {
    val errors = List.newBuilder[String]
    if (queueName.isEmpty) errors += "queueName is required"
    if (queueManager.isEmpty) errors += "queueManager is required"
    errors.result()
  }

  def toJson: String = {
    val msgId = lastMessageId.map(s => s""""$s"""").getOrElse("null")
    val corrId = lastCorrelationId.map(s => s""""$s"""").getOrElse("null")
    s"""{
       |"queueName":"$queueName",
       |"queueManager":"$queueManager",
       |"lastMessageId":$msgId,
       |"lastCorrelationId":$corrId,
       |"lastProcessedTimestamp":$lastProcessedTimestamp,
       |"messagesProcessed":$messagesProcessed,
       |"bytesProcessed":$bytesProcessed,
       |"partitionId":$partitionId,
       |"createdAt":$createdAt
       |}""".stripMargin.replaceAll("\n", "")
  }

  def merge(other: ConnectorCheckpoint): ConnectorCheckpoint = {
    if (other.lastProcessedTimestamp > this.lastProcessedTimestamp) other
    else this
  }
}

object ConnectorCheckpoint {

  def fromJson(json: String): ConnectorCheckpoint = {
    def extractString(key: String): String = {
      val pattern = s""""$key":"([^"]*)"""".r
      pattern.findFirstMatchIn(json).map(_.group(1)).getOrElse("")
    }

    def extractOptString(key: String): Option[String] = {
      val nullPattern = s""""$key":null""".r
      if (nullPattern.findFirstIn(json).isDefined) None
      else {
        val pattern = s""""$key":"([^"]*)"""".r
        pattern.findFirstMatchIn(json).map(_.group(1))
      }
    }

    def extractLong(key: String): Long = {
      val pattern = s""""$key":(-?\\d+)""".r
      pattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)
    }

    def extractInt(key: String): Int = {
      val pattern = s""""$key":(-?\\d+)""".r
      pattern.findFirstMatchIn(json).map(_.group(1).toInt).getOrElse(0)
    }

    ConnectorCheckpoint(
      queueName = extractString("queueName"),
      queueManager = extractString("queueManager"),
      lastMessageId = extractOptString("lastMessageId"),
      lastCorrelationId = extractOptString("lastCorrelationId"),
      lastProcessedTimestamp = extractLong("lastProcessedTimestamp"),
      messagesProcessed = extractLong("messagesProcessed"),
      bytesProcessed = extractLong("bytesProcessed"),
      partitionId = extractInt("partitionId"),
      createdAt = extractLong("createdAt")
    )
  }
}
