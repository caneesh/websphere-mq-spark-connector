package com.ibm.mq.spark.core.payload

/**
 * Configuration for payload handling.
 *
 * @param maxPayloadSizeBytes maximum allowed payload size
 * @param warnThresholdBytes size threshold for warning logs
 * @param enableOffload whether to enable payload offloading
 * @param offloadThresholdBytes size threshold for offloading
 */
case class PayloadConfig(
    maxPayloadSizeBytes: Long = 100 * 1024 * 1024,  // 100 MB default
    warnThresholdBytes: Long = 10 * 1024 * 1024,   // 10 MB default
    enableOffload: Boolean = false,
    offloadThresholdBytes: Long = 50 * 1024 * 1024  // 50 MB default
) {

  def validate(): List[String] = {
    val errors = List.newBuilder[String]
    if (maxPayloadSizeBytes <= 0) errors += "maxPayloadSizeBytes must be positive"
    if (warnThresholdBytes <= 0) errors += "warnThresholdBytes must be positive"
    if (warnThresholdBytes > maxPayloadSizeBytes) {
      errors += "warnThresholdBytes should not exceed maxPayloadSizeBytes"
    }
    errors.result()
  }
}

object PayloadConfig {

  val Default: PayloadConfig = PayloadConfig()

  val Conservative: PayloadConfig = PayloadConfig(
    maxPayloadSizeBytes = 10 * 1024 * 1024,
    warnThresholdBytes = 1 * 1024 * 1024
  )

  val Permissive: PayloadConfig = PayloadConfig(
    maxPayloadSizeBytes = 500 * 1024 * 1024,
    warnThresholdBytes = 50 * 1024 * 1024
  )
}
