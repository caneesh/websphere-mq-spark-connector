package com.ibm.mq.spark.core.control

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.collection.mutable.ListBuffer

/**
 * Controls browse mode operations.
 * Browse mode reads messages without removing them from the queue.
 */
class BrowseModeControl {

  private val _enabled = new AtomicBoolean(false)
  private val _totalBrowsed = new AtomicLong(0)
  private val completionHooks = ListBuffer[Long => Unit]()

  def isEnabled: Boolean = _enabled.get()

  def totalBrowsed: Long = _totalBrowsed.get()

  /**
   * Enables browse mode.
   */
  def enable(): Unit = {
    _enabled.set(true)
  }

  /**
   * Disables browse mode.
   */
  def disable(): Unit = {
    _enabled.set(false)
  }

  /**
   * Records messages browsed.
   */
  def recordBrowsed(count: Int): Unit = {
    _totalBrowsed.addAndGet(count)
  }

  /**
   * Registers a hook called when browse completes.
   */
  def onBrowseComplete(hook: Long => Unit): Unit = synchronized {
    completionHooks += hook
  }

  /**
   * Signals browse completion and invokes hooks.
   */
  def completeBrowse(): Unit = synchronized {
    val total = _totalBrowsed.get()
    completionHooks.foreach(h => scala.util.Try(h(total)))
  }

  /**
   * Resets browse statistics.
   */
  def reset(): Unit = {
    _totalBrowsed.set(0)
  }
}

object BrowseModeControl {
  def apply(): BrowseModeControl = new BrowseModeControl()
}
