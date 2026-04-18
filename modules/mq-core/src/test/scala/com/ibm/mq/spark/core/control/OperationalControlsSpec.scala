package com.ibm.mq.spark.core.control

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class OperationalControlsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  "ConnectorControl" should "start in running state" in {
    val control = ConnectorControl()

    control.state shouldBe ConnectorState.Running
    control.isRunning shouldBe true
  }

  it should "support pause and resume" in {
    val control = ConnectorControl()

    control.pause()
    control.state shouldBe ConnectorState.Paused
    control.isPaused shouldBe true

    control.resume()
    control.state shouldBe ConnectorState.Running
    control.isRunning shouldBe true
  }

  it should "track pause duration" in {
    val control = ConnectorControl()

    control.pause()
    Thread.sleep(100)
    control.resume()

    control.totalPauseDurationMs should be >= 100L
  }

  it should "support drain mode" in {
    val control = ConnectorControl()

    control.enterDrainMode()
    control.state shouldBe ConnectorState.Draining
    control.isDraining shouldBe true
  }

  it should "transition from draining to stopped" in {
    val control = ConnectorControl()

    control.enterDrainMode()
    control.completeDrain()

    control.state shouldBe ConnectorState.Stopped
    control.isStopped shouldBe true
  }

  it should "support graceful stop" in {
    val control = ConnectorControl()

    control.requestStop()
    control.isStopRequested shouldBe true

    control.gracefulStop()
    control.state shouldBe ConnectorState.Stopped
  }

  it should "notify listeners on state change" in {
    val control = ConnectorControl()
    var notified = false
    var lastState: ConnectorState = null

    control.addStateListener { state =>
      notified = true
      lastState = state
    }

    control.pause()

    notified shouldBe true
    lastState shouldBe ConnectorState.Paused
  }

  "BrowseModeControl" should "enable browse mode" in {
    val control = BrowseModeControl()

    control.enable()
    control.isEnabled shouldBe true
  }

  it should "disable browse mode" in {
    val control = BrowseModeControl()
    control.enable()

    control.disable()
    control.isEnabled shouldBe false
  }

  it should "track messages browsed" in {
    val control = BrowseModeControl()
    control.enable()

    control.recordBrowsed(10)
    control.recordBrowsed(5)

    control.totalBrowsed shouldBe 15
  }

  it should "provide hook for browse complete" in {
    val control = BrowseModeControl()
    var hookCalled = false

    control.onBrowseComplete { count =>
      hookCalled = true
      count shouldBe 25
    }

    control.enable()
    control.recordBrowsed(25)
    control.completeBrowse()

    hookCalled shouldBe true
  }

  "DrainCoordinator" should "track in-flight messages" in {
    val coordinator = DrainCoordinator()

    coordinator.trackMessage("msg1")
    coordinator.trackMessage("msg2")

    coordinator.inFlightCount shouldBe 2
  }

  it should "complete messages" in {
    val coordinator = DrainCoordinator()
    coordinator.trackMessage("msg1")
    coordinator.trackMessage("msg2")

    coordinator.completeMessage("msg1")

    coordinator.inFlightCount shouldBe 1
  }

  it should "report when fully drained" in {
    val coordinator = DrainCoordinator()
    coordinator.trackMessage("msg1")
    coordinator.completeMessage("msg1")

    coordinator.isFullyDrained shouldBe true
  }

  it should "wait for drain with timeout" in {
    val coordinator = DrainCoordinator()

    val result = coordinator.awaitDrain(timeoutMs = 100)

    result shouldBe true
  }

  it should "timeout if messages still in flight" in {
    val coordinator = DrainCoordinator()
    coordinator.trackMessage("msg1")

    val result = coordinator.awaitDrain(timeoutMs = 100)

    result shouldBe false
  }
}
