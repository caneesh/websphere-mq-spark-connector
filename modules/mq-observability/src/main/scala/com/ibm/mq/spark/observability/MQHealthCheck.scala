package com.ibm.mq.spark.observability

/**
 * Health check interface for MQ connector components.
 *
 * Implementations provide health status for connection management,
 * queue accessibility, and overall connector readiness.
 */
trait MQHealthCheck {

  /**
   * Performs a health check and returns the current status.
   *
   * @return the health check result
   */
  def check(): HealthCheckResult

  /**
   * Returns the name of this health check.
   *
   * @return the health check name
   */
  def name: String
}

/**
 * Result of a health check operation.
 *
 * @param status the health status
 * @param message optional status message
 * @param details additional diagnostic details
 * @param timestamp the time the check was performed
 */
case class HealthCheckResult(
    status: HealthStatus,
    message: Option[String] = None,
    details: Map[String, Any] = Map.empty,
    timestamp: Long = System.currentTimeMillis()
) {

  /**
   * Indicates whether the component is healthy.
   *
   * @return true if status is UP
   */
  def isHealthy: Boolean = status == HealthStatus.Up
}

/**
 * Enumeration of possible health statuses.
 */
sealed trait HealthStatus

object HealthStatus {

  case object Up extends HealthStatus {
    override def toString: String = "UP"
  }

  case object Down extends HealthStatus {
    override def toString: String = "DOWN"
  }

  case object Degraded extends HealthStatus {
    override def toString: String = "DEGRADED"
  }

  case object Unknown extends HealthStatus {
    override def toString: String = "UNKNOWN"
  }
}
