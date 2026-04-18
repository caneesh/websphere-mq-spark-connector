package com.ibm.mq.spark.core.multiqueue

/**
 * Configuration for a single queue in multi-queue setup.
 */
case class QueueConfig(
    queueName: String,
    batchSize: Int = 1000,
    pollTimeoutMs: Long = 5000,
    parserType: String = "AUTO",
    throttleMessagesPerSecond: Option[Int] = None,
    maxRetries: Int = 3,
    backoutThreshold: Int = 5,
    priority: Int = 0,
    enabled: Boolean = true
) {

  def validate(): List[String] = {
    val errors = List.newBuilder[String]
    if (queueName.isEmpty) errors += "queueName is required"
    if (batchSize <= 0) errors += "batchSize must be positive"
    if (pollTimeoutMs <= 0) errors += "pollTimeoutMs must be positive"
    throttleMessagesPerSecond.foreach { rate =>
      if (rate <= 0) errors += "throttleMessagesPerSecond must be positive"
    }
    errors.result()
  }
}

/**
 * Configuration for multiple queues.
 */
case class MultiQueueConfig(queues: Seq[QueueConfig]) {

  private val configMap: Map[String, QueueConfig] = queues.map(q => q.queueName -> q).toMap

  def queueNames: Seq[String] = queues.map(_.queueName)

  def enabledQueues: Seq[QueueConfig] = queues.filter(_.enabled)

  def getConfig(queueName: String): Option[QueueConfig] = configMap.get(queueName)

  def validate(): List[String] = {
    val errors = List.newBuilder[String]

    val names = queues.map(_.queueName)
    val duplicates = names.diff(names.distinct)
    if (duplicates.nonEmpty) {
      errors += s"duplicate queue names found: ${duplicates.mkString(", ")}"
    }

    queues.foreach { q =>
      q.validate().foreach { err =>
        errors += s"${q.queueName}: $err"
      }
    }

    errors.result()
  }

  def withQueue(config: QueueConfig): MultiQueueConfig = {
    val updated = queues.filterNot(_.queueName == config.queueName) :+ config
    MultiQueueConfig(updated)
  }
}

object MultiQueueConfig {

  val Empty: MultiQueueConfig = MultiQueueConfig(Seq.empty)

  def single(queueName: String): MultiQueueConfig = {
    MultiQueueConfig(Seq(QueueConfig(queueName)))
  }

  def fromQueueNames(names: Seq[String]): MultiQueueConfig = {
    MultiQueueConfig(names.map(QueueConfig(_)))
  }
}
