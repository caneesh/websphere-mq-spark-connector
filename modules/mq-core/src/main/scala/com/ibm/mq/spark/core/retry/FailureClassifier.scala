package com.ibm.mq.spark.core.retry

import com.ibm.mq.spark.core.{MQException => ConnectorMQException}

/**
 * Classifies exceptions into transient or permanent failure types.
 *
 * Classification Categories:
 * - Transient: temporary issues that may resolve on retry
 *   - Network errors (connection, socket timeout)
 *   - MQ connection issues (queue manager unavailable)
 *   - Resource exhaustion (temporary)
 *
 * - Permanent: issues that won't resolve on retry
 *   - Configuration errors (bad queue name, auth failure)
 *   - Programming errors (null pointer, class cast)
 *   - Parse errors (malformed message content)
 *
 * MQ-Specific Classification:
 * - Connection errors: Transient (retry with backoff)
 * - Auth errors: Permanent (wrong credentials)
 * - Config errors: Permanent (bad queue name)
 * - Transport errors: Transient (connection lost)
 */
trait FailureClassifier {
  def classify(throwable: Throwable): FailureType
}

/**
 * Default implementation that classifies common exceptions.
 */
case class DefaultFailureClassifier() extends FailureClassifier {

  private val permanentExceptions: Set[Class[_]] = Set(
    classOf[IllegalArgumentException],
    classOf[NumberFormatException],
    classOf[NullPointerException],
    classOf[UnsupportedOperationException],
    classOf[ClassCastException],
    classOf[SecurityException]
  )

  private val transientExceptions: Set[Class[_]] = Set(
    classOf[java.net.SocketTimeoutException],
    classOf[java.net.ConnectException],
    classOf[java.io.IOException],
    classOf[java.util.concurrent.TimeoutException],
    classOf[java.net.SocketException]
  )

  override def classify(throwable: Throwable): FailureType = {
    val exceptionClass = throwable.getClass
    val message = Option(throwable.getMessage).getOrElse(exceptionClass.getSimpleName)

    throwable match {
      case mqEx: ConnectorMQException =>
        classifyMQException(mqEx)

      case _ if isPermanentException(exceptionClass) =>
        PermanentFailure(message)

      case _ if isTransientException(exceptionClass) =>
        TransientFailure(message)

      case _ =>
        TransientFailure(message)
    }
  }

  private def classifyMQException(ex: ConnectorMQException): FailureType = {
    val message = ex.getMessage
    val lower = message.toLowerCase

    if (lower.contains("authentication") || lower.contains("auth")) {
      PermanentFailure(s"Authentication error: $message")
    } else if (lower.contains("configuration") || lower.contains("config")) {
      PermanentFailure(s"Configuration error: $message")
    } else if (lower.contains("parse") || lower.contains("invalid format")) {
      PermanentFailure(s"Parse error: $message")
    } else {
      TransientFailure(message)
    }
  }

  private def isPermanentException(clazz: Class[_]): Boolean = {
    permanentExceptions.exists(_.isAssignableFrom(clazz))
  }

  private def isTransientException(clazz: Class[_]): Boolean = {
    transientExceptions.exists(_.isAssignableFrom(clazz))
  }
}
