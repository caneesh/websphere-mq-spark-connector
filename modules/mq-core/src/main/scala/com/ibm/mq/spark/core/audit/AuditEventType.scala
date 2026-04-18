package com.ibm.mq.spark.core.audit

sealed abstract class AuditEventType(val name: String)

object AuditEventType {
  case object MessageReceived extends AuditEventType("MESSAGE_RECEIVED")
  case object MessageCommitted extends AuditEventType("MESSAGE_COMMITTED")
  case object MessageRolledBack extends AuditEventType("MESSAGE_ROLLED_BACK")
  case object MessageQuarantined extends AuditEventType("MESSAGE_QUARANTINED")
  case object ParseError extends AuditEventType("PARSE_ERROR")
  case object ConnectionEstablished extends AuditEventType("CONNECTION_ESTABLISHED")
  case object ConnectionLost extends AuditEventType("CONNECTION_LOST")
  case object CheckpointSaved extends AuditEventType("CHECKPOINT_SAVED")
  case object CheckpointLoaded extends AuditEventType("CHECKPOINT_LOADED")
  case object Error extends AuditEventType("ERROR")
  case object RetryAttempt extends AuditEventType("RETRY_ATTEMPT")
}
