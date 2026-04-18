package com.ibm.mq.spark.core.payload

/**
 * Handles large payloads with streaming/chunked processing
 * to reduce memory pressure.
 */
case class StreamingPayloadHandler(
    chunkSize: Int = 64 * 1024  // 64 KB chunks
) {

  /**
   * Estimates payload size (exact for byte arrays).
   */
  def estimateSize(payload: Array[Byte]): Long = {
    payload.length.toLong
  }

  /**
   * Returns an iterator of payload chunks.
   */
  def chunks(payload: Array[Byte]): Iterator[Array[Byte]] = {
    new Iterator[Array[Byte]] {
      private var offset = 0

      override def hasNext: Boolean = offset < payload.length

      override def next(): Array[Byte] = {
        val end = math.min(offset + chunkSize, payload.length)
        val chunk = java.util.Arrays.copyOfRange(payload, offset, end)
        offset = end
        chunk
      }
    }
  }

  /**
   * Processes payload in streaming fashion with a handler function.
   */
  def processStreaming[T](
      payload: Array[Byte],
      handler: Array[Byte] => T,
      combiner: (T, T) => T
  ): T = {
    chunks(payload).map(handler).reduce(combiner)
  }
}
