package com.ibm.mq.spark.observability

import com.ibm.mq.spark.core.audit.{AuditEvent, AuditEventType}
import com.ibm.mq.spark.core.interfaces.AuditSink

import java.io.PrintStream
import scala.collection.mutable.ListBuffer

/**
 * Console audit sink for development and debugging.
 */
case class ConsoleAuditSink(out: PrintStream = System.out) extends AuditSink {

  override def emit(event: AuditEvent): Unit = {
    out.println(formatEvent(event))
  }

  override def flush(): Unit = {
    out.flush()
  }

  override def close(): Unit = {}

  private def formatEvent(event: AuditEvent): String = {
    val ts = new java.sql.Timestamp(event.timestamp)
    val msgId = event.messageId.getOrElse("-")
    s"[$ts] ${event.eventType} queue=${event.queueName} qm=${event.queueManager} msgId=$msgId"
  }
}

/**
 * Structured log audit sink emitting JSON for log aggregation.
 */
case class StructuredLogAuditSink(out: PrintStream = System.out) extends AuditSink {

  override def emit(event: AuditEvent): Unit = {
    out.println(event.toJson)
  }

  override def flush(): Unit = {
    out.flush()
  }

  override def close(): Unit = {}
}

/**
 * In-memory audit sink for testing.
 */
case class InMemoryAuditSink() extends AuditSink {

  private val _events = ListBuffer[AuditEvent]()

  override def emit(event: AuditEvent): Unit = synchronized {
    _events += event
  }

  override def flush(): Unit = {}

  override def close(): Unit = {}

  def events: Seq[AuditEvent] = synchronized {
    _events.toSeq
  }

  def eventsOfType(eventType: AuditEventType): Seq[AuditEvent] = synchronized {
    _events.filter(_.eventType == eventType).toSeq
  }

  def clear(): Unit = synchronized {
    _events.clear()
  }

  def size: Int = synchronized {
    _events.size
  }
}

/**
 * Composite sink that sends events to multiple destinations.
 */
case class CompositeAuditSink(sinks: Seq[AuditSink]) extends AuditSink {

  override def emit(event: AuditEvent): Unit = {
    sinks.foreach(_.emit(event))
  }

  override def flush(): Unit = {
    sinks.foreach(_.flush())
  }

  override def close(): Unit = {
    sinks.foreach(_.close())
  }
}

/**
 * Filtering sink that only emits events matching criteria.
 */
case class FilteringAuditSink(
    delegate: AuditSink,
    predicate: AuditEvent => Boolean
) extends AuditSink {

  override def emit(event: AuditEvent): Unit = {
    if (predicate(event)) {
      delegate.emit(event)
    }
  }

  override def flush(): Unit = {
    delegate.flush()
  }

  override def close(): Unit = {
    delegate.close()
  }
}

object AuditSinks {

  def console(): AuditSink = ConsoleAuditSink()

  def structuredLog(): AuditSink = StructuredLogAuditSink()

  def inMemory(): InMemoryAuditSink = InMemoryAuditSink()

  def noop(): AuditSink = new AuditSink {
    override def emit(event: AuditEvent): Unit = {}
    override def flush(): Unit = {}
    override def close(): Unit = {}
  }

  def errorsOnly(delegate: AuditSink): AuditSink = FilteringAuditSink(
    delegate,
    event => event.eventType.name.contains("Error") || event.eventType.name.contains("Failure")
  )
}
