package com.ibm.mq.spark.core.audit

case class AuditEvent(
    eventType: AuditEventType,
    timestamp: Long,
    queueName: String,
    queueManager: String,
    messageId: Option[String],
    correlationId: Option[String],
    sourceId: String,
    details: Map[String, String]
) {

  private val sensitiveKeys = Set(
    "password", "sslKeyStorePassword", "sslTrustStorePassword",
    "secret", "token", "apiKey", "credential"
  )

  def redacted: AuditEvent = {
    val redactedDetails = details.map { case (k, v) =>
      if (sensitiveKeys.exists(sk => k.toLowerCase.contains(sk.toLowerCase))) {
        k -> "[REDACTED]"
      } else {
        k -> v
      }
    }
    copy(details = redactedDetails)
  }

  def toJson: String = {
    val msgId = messageId.map(s => s""""$s"""").getOrElse("null")
    val corrId = correlationId.map(s => s""""$s"""").getOrElse("null")
    val detailsJson = details.map { case (k, v) =>
      s""""$k":"${escapeJson(v)}""""
    }.mkString("{", ",", "}")

    s"""{
       |"eventType":"${eventType.name}",
       |"timestamp":$timestamp,
       |"queueName":"$queueName",
       |"queueManager":"$queueManager",
       |"messageId":$msgId,
       |"correlationId":$corrId,
       |"sourceId":"$sourceId",
       |"details":$detailsJson
       |}""".stripMargin.replaceAll("\n", "")
  }

  private def escapeJson(s: String): String = {
    s.replace("\\", "\\\\")
      .replace("\"", "\\\"")
      .replace("\n", "\\n")
      .replace("\r", "\\r")
      .replace("\t", "\\t")
  }
}

object AuditEvent {

  def error(
      queueName: String,
      queueManager: String,
      sourceId: String,
      error: Throwable,
      messageId: Option[String] = None
  ): AuditEvent = {
    AuditEvent(
      eventType = AuditEventType.Error,
      timestamp = System.currentTimeMillis(),
      queueName = queueName,
      queueManager = queueManager,
      messageId = messageId,
      correlationId = None,
      sourceId = sourceId,
      details = Map(
        "errorMessage" -> error.getMessage,
        "errorType" -> error.getClass.getName
      )
    )
  }
}
