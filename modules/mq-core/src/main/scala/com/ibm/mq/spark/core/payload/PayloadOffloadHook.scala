package com.ibm.mq.spark.core.payload

/**
 * Hook for offloading large payloads to external storage.
 * This is a placeholder for future implementation.
 */
trait PayloadOffloadHook {

  /**
   * Determines if a payload should be offloaded based on size.
   */
  def shouldOffload(sizeBytes: Long): Boolean

  /**
   * Offloads payload to external storage.
   *
   * @param payload the payload bytes
   * @param metadata additional metadata about the message
   * @return result indicating where the payload was stored
   */
  def offload(payload: Array[Byte], metadata: Map[String, String]): OffloadResult
}

/**
 * Result of payload offload operation.
 */
sealed trait OffloadResult

object OffloadResult {

  /**
   * Payload kept in local memory.
   */
  case class Local(payload: Array[Byte]) extends OffloadResult

  /**
   * Payload stored in external location.
   */
  case class External(reference: String, storageType: String) extends OffloadResult

  /**
   * Offload failed.
   */
  case class Failed(error: String) extends OffloadResult
}

/**
 * No-op offload hook that never offloads.
 */
object NoOpOffloadHook extends PayloadOffloadHook {

  override def shouldOffload(sizeBytes: Long): Boolean = false

  override def offload(payload: Array[Byte], metadata: Map[String, String]): OffloadResult = {
    OffloadResult.Local(payload)
  }
}

/**
 * Size-based offload hook that offloads payloads exceeding threshold.
 */
case class SizeBasedOffloadHook(
    thresholdBytes: Long,
    delegate: PayloadOffloadHook
) extends PayloadOffloadHook {

  override def shouldOffload(sizeBytes: Long): Boolean = {
    sizeBytes > thresholdBytes
  }

  override def offload(payload: Array[Byte], metadata: Map[String, String]): OffloadResult = {
    if (shouldOffload(payload.length)) {
      delegate.offload(payload, metadata)
    } else {
      OffloadResult.Local(payload)
    }
  }
}
