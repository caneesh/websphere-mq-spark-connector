package com.ibm.mq.spark.source.stream

import org.apache.spark.sql.connector.read.streaming.Offset

/**
 * Offset representation for MQ streaming source.
 * Tracks message count and timestamp for checkpoint/restart.
 */
case class MQOffset(messageCount: Long, timestamp: Long) extends Offset with Ordered[MQOffset] with Serializable {

  override def json(): String = {
    s"""{"messageCount":$messageCount,"timestamp":$timestamp}"""
  }

  override def compare(that: MQOffset): Int = {
    val cmp = this.messageCount.compareTo(that.messageCount)
    if (cmp != 0) cmp else this.timestamp.compareTo(that.timestamp)
  }
}

object MQOffset {

  val Initial: MQOffset = MQOffset(0, 0L)

  def fromJson(json: String): MQOffset = {
    val countPattern = """"messageCount":(\d+)""".r
    val tsPattern = """"timestamp":(\d+)""".r

    val count = countPattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)
    val ts = tsPattern.findFirstMatchIn(json).map(_.group(1).toLong).getOrElse(0L)

    MQOffset(count, ts)
  }
}
