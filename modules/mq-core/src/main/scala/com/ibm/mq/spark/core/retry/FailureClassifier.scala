package com.ibm.mq.spark.core.retry

/**
 * Classifies exceptions into transient or permanent failure types.
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
    classOf[ClassCastException]
  )

  private val transientExceptions: Set[Class[_]] = Set(
    classOf[java.net.SocketTimeoutException],
    classOf[java.net.ConnectException],
    classOf[java.io.IOException],
    classOf[java.util.concurrent.TimeoutException]
  )

  override def classify(throwable: Throwable): FailureType = {
    val exceptionClass = throwable.getClass
    val message = Option(throwable.getMessage).getOrElse(exceptionClass.getSimpleName)

    if (isPermanentException(exceptionClass)) {
      PermanentFailure(message)
    } else if (isTransientException(exceptionClass)) {
      TransientFailure(message)
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
