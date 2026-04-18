package com.ibm.mq.spark.core.payload

/**
 * Validates payload sizes against configured limits.
 */
case class PayloadSizeValidator(
    maxBytes: Long,
    warnBytes: Long = Long.MaxValue
) {

  def validate(payload: Array[Byte]): ValidationResult = {
    val size = payload.length.toLong

    if (size > maxBytes) {
      ValidationResult(
        isValid = false,
        sizeBytes = size,
        shouldWarn = false,
        rejection = Some(s"Payload size ($size bytes) exceeds maximum allowed ($maxBytes bytes)")
      )
    } else if (size > warnBytes) {
      ValidationResult(
        isValid = true,
        sizeBytes = size,
        shouldWarn = true,
        rejection = None
      )
    } else {
      ValidationResult(
        isValid = true,
        sizeBytes = size,
        shouldWarn = false,
        rejection = None
      )
    }
  }
}

case class ValidationResult(
    isValid: Boolean,
    sizeBytes: Long,
    shouldWarn: Boolean,
    rejection: Option[String]
)

object PayloadSizeValidator {

  def fromConfig(config: PayloadConfig): PayloadSizeValidator = {
    PayloadSizeValidator(
      maxBytes = config.maxPayloadSizeBytes,
      warnBytes = config.warnThresholdBytes
    )
  }
}
