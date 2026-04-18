package com.ibm.mq.spark.core.multiqueue

import com.ibm.mq.spark.core.interfaces.PayloadParser

import java.util.concurrent.ConcurrentHashMap

/**
 * Registry for per-queue parser configurations.
 */
case class QueueParserRegistry(
    defaultParser: Option[PayloadParser] = None
) {

  private val parsers = new ConcurrentHashMap[String, PayloadParser]()

  /**
   * Registers a parser for a specific queue.
   */
  def register(queueName: String, parser: PayloadParser): Unit = {
    parsers.put(queueName, parser)
  }

  /**
   * Unregisters parser for a queue.
   */
  def unregister(queueName: String): Unit = {
    parsers.remove(queueName)
  }

  /**
   * Gets parser for a queue, falling back to default.
   */
  def getParser(queueName: String): Option[PayloadParser] = {
    Option(parsers.get(queueName)).orElse(defaultParser)
  }

  /**
   * Returns all registered queue names.
   */
  def registeredQueues: Set[String] = {
    import scala.collection.JavaConverters._
    parsers.keySet().asScala.toSet
  }

  /**
   * Clears all registered parsers.
   */
  def clear(): Unit = {
    parsers.clear()
  }
}

object QueueParserRegistry {

  def apply(): QueueParserRegistry = new QueueParserRegistry()

  def withDefault(parser: PayloadParser): QueueParserRegistry = {
    QueueParserRegistry(defaultParser = Some(parser))
  }
}
