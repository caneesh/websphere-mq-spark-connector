package com.ibm.mq.spark.core.parser

case class ParsedPayloadResult(
    format: PayloadFormat,
    isSuccess: Boolean,
    parsedData: Option[Any],
    error: Option[String],
    originalPayload: Option[String],
    originalSize: Int,
    parsingDurationNanos: Long = 0L
)

object ParsedPayloadResult {

  def success(
      format: PayloadFormat,
      parsedData: Any,
      originalSize: Int,
      parsingDurationNanos: Long = 0L
  ): ParsedPayloadResult = {
    ParsedPayloadResult(
      format = format,
      isSuccess = true,
      parsedData = Some(parsedData),
      error = None,
      originalPayload = None,
      originalSize = originalSize,
      parsingDurationNanos = parsingDurationNanos
    )
  }

  def failure(
      format: PayloadFormat,
      error: String,
      originalPayload: String,
      originalSize: Int,
      parsingDurationNanos: Long = 0L
  ): ParsedPayloadResult = {
    ParsedPayloadResult(
      format = format,
      isSuccess = false,
      parsedData = None,
      error = Some(error),
      originalPayload = Some(originalPayload),
      originalSize = originalSize,
      parsingDurationNanos = parsingDurationNanos
    )
  }

  def binary(data: Array[Byte], originalSize: Int): ParsedPayloadResult = {
    ParsedPayloadResult(
      format = PayloadFormat.Binary,
      isSuccess = true,
      parsedData = Some(data),
      error = None,
      originalPayload = None,
      originalSize = originalSize
    )
  }
}
