package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.core.client.{MQTransport, RealMQTransport}
import com.ibm.mq.spark.core.connection.MQConnectionConfig
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.source.MQSourceOptions

/**
 * MQ Transport adapter for Spark source integration.
 *
 * This class wraps RealMQTransport and provides Serializable support
 * needed for Spark executor serialization. The actual transport is
 * created lazily on the executor side.
 *
 * Note: The transport instance itself is not serialized. Only the
 * configuration (options) is serialized, and the transport is
 * recreated on each executor.
 */
class MQJmsTransport(options: MQSourceOptions) extends MQTransport with Serializable {

  @transient private var delegate: RealMQTransport = _
  @transient private var currentConfig: MQConnectionConfig = _

  private def ensureDelegate(): Unit = {
    if (delegate == null) {
      delegate = new RealMQTransport(options.queueName)
    }
  }

  override def connect(config: MQConnectionConfig): Unit = {
    ensureDelegate()
    currentConfig = config
    delegate.connect(config)
  }

  override def disconnect(): Unit = {
    if (delegate != null) {
      delegate.disconnect()
      delegate = null
    }
  }

  override def isConnected: Boolean = {
    delegate != null && delegate.isConnected
  }

  override def receive(waitMillis: Long): Option[RawMQMessage] = {
    if (delegate == null || !delegate.isConnected) {
      throw new IllegalStateException("Transport not connected. Call connect() first.")
    }
    delegate.receive(waitMillis)
  }

  override def commit(): Unit = {
    if (delegate == null || !delegate.isConnected) {
      throw new IllegalStateException("Cannot commit: transport not connected")
    }
    delegate.commit()
  }

  override def rollback(): Unit = {
    if (delegate != null && delegate.isConnected) {
      delegate.rollback()
    }
  }
}
