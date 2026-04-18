package com.ibm.mq.spark.observability

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ObservabilitySpec extends AnyFlatSpec with Matchers {

  "HealthStatus" should "provide correct string representations" in {
    HealthStatus.Up.toString shouldBe "UP"
    HealthStatus.Down.toString shouldBe "DOWN"
    HealthStatus.Degraded.toString shouldBe "DEGRADED"
    HealthStatus.Unknown.toString shouldBe "UNKNOWN"
  }

  "HealthCheckResult" should "indicate healthy when status is Up" in {
    val result = HealthCheckResult(status = HealthStatus.Up)

    result.isHealthy shouldBe true
  }

  it should "indicate unhealthy when status is Down" in {
    val result = HealthCheckResult(status = HealthStatus.Down)

    result.isHealthy shouldBe false
  }

  it should "indicate unhealthy when status is Degraded" in {
    val result = HealthCheckResult(status = HealthStatus.Degraded)

    result.isHealthy shouldBe false
  }

  it should "include message and details" in {
    val result = HealthCheckResult(
      status = HealthStatus.Up,
      message = Some("All systems operational"),
      details = Map("connections" -> 5, "queueDepth" -> 100)
    )

    result.message shouldBe Some("All systems operational")
    result.details should contain("connections" -> 5)
    result.details should contain("queueDepth" -> 100)
  }

  it should "set timestamp automatically" in {
    val before = System.currentTimeMillis()
    val result = HealthCheckResult(status = HealthStatus.Up)
    val after = System.currentTimeMillis()

    result.timestamp should be >= before
    result.timestamp should be <= after
  }
}
