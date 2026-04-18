package com.ibm.mq.spark.core.payload

/**
 * Strategy for handling payloads based on their size.
 */
case class LargePayloadStrategy(
    config: PayloadConfig,
    offloadHook: PayloadOffloadHook = NoOpOffloadHook
) {

  private val validator = PayloadSizeValidator.fromConfig(config)

  /**
   * Determines appropriate action for a payload.
   */
  def handle(payload: Array[Byte], metadata: Map[String, String]): PayloadDecision = {
    val validation = validator.validate(payload)

    if (!validation.isValid) {
      PayloadDecision(
        action = PayloadAction.Reject,
        reason = validation.rejection,
        sizeBytes = validation.sizeBytes
      )
    } else if (config.enableOffload && offloadHook.shouldOffload(validation.sizeBytes)) {
      val offloadResult = offloadHook.offload(payload, metadata)
      offloadResult match {
        case OffloadResult.External(ref, _) =>
          PayloadDecision(
            action = PayloadAction.Offloaded,
            reason = Some(s"Offloaded to: $ref"),
            sizeBytes = validation.sizeBytes
          )
        case OffloadResult.Failed(err) =>
          PayloadDecision(
            action = PayloadAction.Reject,
            reason = Some(s"Offload failed: $err"),
            sizeBytes = validation.sizeBytes
          )
        case OffloadResult.Local(_) =>
          processLocally(validation)
      }
    } else {
      processLocally(validation)
    }
  }

  private def processLocally(validation: ValidationResult): PayloadDecision = {
    if (validation.shouldWarn) {
      PayloadDecision(
        action = PayloadAction.ProcessWithWarning,
        reason = Some(s"Large payload: ${validation.sizeBytes} bytes"),
        sizeBytes = validation.sizeBytes
      )
    } else {
      PayloadDecision(
        action = PayloadAction.Process,
        reason = None,
        sizeBytes = validation.sizeBytes
      )
    }
  }
}

/**
 * Decision about how to handle a payload.
 */
case class PayloadDecision(
    action: PayloadAction,
    reason: Option[String],
    sizeBytes: Long
)

/**
 * Actions for payload handling.
 */
sealed trait PayloadAction

object PayloadAction {
  case object Process extends PayloadAction
  case object ProcessWithWarning extends PayloadAction
  case object Offloaded extends PayloadAction
  case object Reject extends PayloadAction
}
