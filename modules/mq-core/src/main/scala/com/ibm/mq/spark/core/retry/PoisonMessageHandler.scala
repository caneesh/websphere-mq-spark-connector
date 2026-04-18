package com.ibm.mq.spark.core.retry

import com.ibm.mq.spark.core.message.RawMQMessage

/**
 * Handles poison messages that have exceeded processing thresholds.
 */
trait PoisonMessageHandler {
  def handlePoisonMessage(message: RawMQMessage): QuarantineDecision
}

/**
 * Default implementation of poison message handling.
 *
 * @param backoutThreshold backout count threshold for poison detection
 * @param defaultAction action to take for poison messages
 */
case class DefaultPoisonMessageHandler(
    backoutThreshold: Int,
    defaultAction: QuarantineAction = QuarantineAction.MoveToDeadLetter
) extends PoisonMessageHandler {

  private val tracker = BackoutTracker(backoutThreshold)

  override def handlePoisonMessage(message: RawMQMessage): QuarantineDecision = {
    val backoutCount = tracker.getBackoutCount(message)
    val reason = s"Message exceeded backout threshold: $backoutCount >= $backoutThreshold"

    QuarantineDecision(
      action = defaultAction,
      reason = reason,
      messageId = message.messageIdHex
    )
  }
}
