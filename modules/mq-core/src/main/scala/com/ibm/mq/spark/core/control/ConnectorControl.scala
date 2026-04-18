package com.ibm.mq.spark.core.control

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.collection.mutable.ListBuffer

/**
 * Controls operational state of the connector.
 * Thread-safe state management with listener support.
 */
class ConnectorControl {

  private val _state = new AtomicReference[ConnectorState](ConnectorState.Running)
  private val _stopRequested = new AtomicReference[Boolean](false)
  private val _pauseStartTime = new AtomicLong(0)
  private val _totalPauseDuration = new AtomicLong(0)
  private val listeners = ListBuffer[ConnectorState => Unit]()

  def state: ConnectorState = _state.get()

  def isRunning: Boolean = state == ConnectorState.Running

  def isPaused: Boolean = state == ConnectorState.Paused

  def isDraining: Boolean = state == ConnectorState.Draining

  def isStopped: Boolean = state == ConnectorState.Stopped

  def isStopRequested: Boolean = _stopRequested.get()

  def totalPauseDurationMs: Long = _totalPauseDuration.get()

  /**
   * Pauses message processing.
   */
  def pause(): Unit = {
    if (_state.compareAndSet(ConnectorState.Running, ConnectorState.Paused)) {
      _pauseStartTime.set(System.currentTimeMillis())
      notifyListeners(ConnectorState.Paused)
    }
  }

  /**
   * Resumes message processing.
   */
  def resume(): Unit = {
    if (_state.compareAndSet(ConnectorState.Paused, ConnectorState.Running)) {
      val pauseDuration = System.currentTimeMillis() - _pauseStartTime.get()
      _totalPauseDuration.addAndGet(pauseDuration)
      notifyListeners(ConnectorState.Running)
    }
  }

  /**
   * Enters drain mode - stop accepting new messages but process in-flight.
   */
  def enterDrainMode(): Unit = {
    val current = _state.get()
    if (current == ConnectorState.Running || current == ConnectorState.Paused) {
      if (_state.compareAndSet(current, ConnectorState.Draining)) {
        notifyListeners(ConnectorState.Draining)
      }
    }
  }

  /**
   * Completes drain and transitions to stopped state.
   */
  def completeDrain(): Unit = {
    if (_state.compareAndSet(ConnectorState.Draining, ConnectorState.Stopped)) {
      notifyListeners(ConnectorState.Stopped)
    }
  }

  /**
   * Requests a graceful stop.
   */
  def requestStop(): Unit = {
    _stopRequested.set(true)
  }

  /**
   * Performs graceful stop, transitioning to stopped state.
   */
  def gracefulStop(): Unit = {
    _stopRequested.set(true)
    val current = _state.get()
    if (current != ConnectorState.Stopped) {
      _state.set(ConnectorState.Stopped)
      notifyListeners(ConnectorState.Stopped)
    }
  }

  /**
   * Registers a state change listener.
   */
  def addStateListener(listener: ConnectorState => Unit): Unit = synchronized {
    listeners += listener
  }

  /**
   * Removes a state change listener.
   */
  def removeStateListener(listener: ConnectorState => Unit): Unit = synchronized {
    listeners -= listener
  }

  private def notifyListeners(state: ConnectorState): Unit = synchronized {
    listeners.foreach(l => scala.util.Try(l(state)))
  }
}

object ConnectorControl {
  def apply(): ConnectorControl = new ConnectorControl()
}

/**
 * Connector operational states.
 */
sealed trait ConnectorState

object ConnectorState {
  case object Running extends ConnectorState
  case object Paused extends ConnectorState
  case object Draining extends ConnectorState
  case object Stopped extends ConnectorState
}
