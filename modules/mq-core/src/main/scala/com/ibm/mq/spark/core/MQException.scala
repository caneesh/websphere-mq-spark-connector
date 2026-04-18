package com.ibm.mq.spark.core

/**
 * Base exception for IBM MQ connector errors.
 *
 * @param message the error message
 * @param cause optional underlying cause
 * @param reasonCode optional MQ reason code
 * @param completionCode optional MQ completion code
 */
class MQException(
    message: String,
    cause: Throwable = null,
    val reasonCode: Option[Int] = None,
    val completionCode: Option[Int] = None
) extends RuntimeException(message, cause) {

  override def toString: String = {
    val codeInfo = (reasonCode, completionCode) match {
      case (Some(rc), Some(cc)) => s" [reasonCode=$rc, completionCode=$cc]"
      case (Some(rc), None) => s" [reasonCode=$rc]"
      case _ => ""
    }
    s"MQException: $message$codeInfo"
  }
}

object MQException {

  /**
   * Creates an MQException for connection failures.
   *
   * @param message the error message
   * @param cause optional underlying cause
   * @return a new MQException
   */
  def connectionFailed(message: String, cause: Throwable = null): MQException =
    new MQException(s"Connection failed: $message", cause)

  /**
   * Creates an MQException for message retrieval failures.
   *
   * @param message the error message
   * @param reasonCode the MQ reason code
   * @return a new MQException
   */
  def receiveFailed(message: String, reasonCode: Int): MQException =
    new MQException(s"Receive failed: $message", null, Some(reasonCode))

  /**
   * Creates an MQException for message send failures.
   *
   * @param message the error message
   * @param reasonCode the MQ reason code
   * @return a new MQException
   */
  def sendFailed(message: String, reasonCode: Int): MQException =
    new MQException(s"Send failed: $message", null, Some(reasonCode))

  /**
   * Creates an MQException for configuration validation failures.
   *
   * @param errors the list of validation errors
   * @return a new MQException
   */
  def configurationInvalid(errors: List[String]): MQException =
    new MQException(s"Invalid configuration: ${errors.mkString(", ")}")

  /**
   * Creates an MQException for configuration errors.
   */
  def configurationError(message: String, cause: Throwable = null): MQException =
    new MQException(s"Configuration error: $message", cause)

  /**
   * Creates an MQException for authentication failures.
   */
  def authenticationFailed(message: String, cause: Throwable = null): MQException =
    new MQException(s"Authentication failed: $message", cause)

  /**
   * Creates an MQException for transport-level errors.
   */
  def transportError(message: String, cause: Throwable = null): MQException =
    new MQException(s"Transport error: $message", cause)

  /**
   * Creates an MQException for transaction rollback.
   */
  def transactionRolledBack(message: String, cause: Throwable = null): MQException =
    new MQException(s"Transaction rolled back: $message", cause)

  /**
   * Creates an MQException for parse failures.
   */
  def parseFailed(message: String, cause: Throwable = null): MQException =
    new MQException(s"Parse failed: $message", cause)
}
