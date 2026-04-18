package com.ibm.mq.spark.core.retry

/**
 * Represents the action to take for a poison message.
 */
sealed trait QuarantineAction {
  def description: String
}

object QuarantineAction {

  /**
   * Move message to the system dead letter queue.
   */
  case object MoveToDeadLetter extends QuarantineAction {
    override def description: String = "Move to dead letter queue"
  }

  /**
   * Move message to the configured backout queue.
   */
  case object MoveToBackout extends QuarantineAction {
    override def description: String = "Move to backout queue"
  }

  /**
   * Discard the message without preservation.
   */
  case object Discard extends QuarantineAction {
    override def description: String = "discard message"
  }

  /**
   * Move message to a custom quarantine queue.
   */
  case class CustomQueue(queueName: String) extends QuarantineAction {
    override def description: String = s"Move to custom queue: $queueName"
  }
}

/**
 * Decision about how to handle a poison message.
 *
 * @param action the quarantine action to take
 * @param reason explanation for why this action was chosen
 * @param messageId the ID of the affected message
 */
case class QuarantineDecision(
    action: QuarantineAction,
    reason: String,
    messageId: String
)
