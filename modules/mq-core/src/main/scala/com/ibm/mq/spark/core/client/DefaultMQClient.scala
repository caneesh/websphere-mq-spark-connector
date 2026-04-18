package com.ibm.mq.spark.core.client

import com.ibm.mq.spark.core.config.{MQConnectorConfig, MQSecurityConfig}
import com.ibm.mq.spark.core.connection.{MQConnectionConfig, MQConnectionState}
import com.ibm.mq.spark.core.message.{MQMessage, RawMQMessage}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
 * Default implementation of the MQ client.
 *
 * Provides transactional message consumption with batch polling,
 * commit/rollback support, and statistics tracking.
 *
 * @param transport the underlying transport layer
 */
class DefaultMQClient(transport: MQTransport) extends MQClient {

  @volatile private var _connectionState: MQConnectionState = MQConnectionState.Disconnected
  @volatile private var currentConfig: Option[MQConnectorConfig] = None
  private val _stats: ClientStats = ClientStats()

  /**
   * Current connection state.
   */
  def connectionState: MQConnectionState = _connectionState

  /**
   * Client statistics.
   */
  def stats: ClientStats = _stats

  override def connect(config: MQConnectorConfig): Try[Unit] = {
    _connectionState = MQConnectionState.Connecting
    currentConfig = Some(config)

    Try {
      val connectionConfig = buildConnectionConfig(config)
      transport.connect(connectionConfig)
      _connectionState = MQConnectionState.Connected
    }.recoverWith {
      case e: Exception =>
        _connectionState = MQConnectionState.Failed(e.getMessage)
        Failure(e)
    }
  }

  override def isConnected: Boolean = transport.isConnected

  override def receive(): Option[MQMessage] = {
    if (!isConnected) return None

    transport.receive(1000L).map { raw =>
      MQMessage(
        payload = raw.payload,
        messageId = raw.messageId,
        correlationId = raw.correlationId,
        timestamp = raw.putTimestamp,
        properties = Map.empty
      )
    }
  }

  /**
   * Polls messages in batches.
   *
   * @param batchSize maximum number of messages to retrieve
   * @param waitMillis maximum time to wait for the first message
   * @return sequence of raw messages retrieved
   */
  def poll(batchSize: Int, waitMillis: Long): Seq[RawMQMessage] = {
    if (!isConnected) return Seq.empty

    val startTime = System.currentTimeMillis()
    val messages = ListBuffer[RawMQMessage]()
    var continue = true
    var remainingWait = waitMillis

    while (continue && messages.size < batchSize) {
      val wait = if (messages.isEmpty) remainingWait else 0L
      transport.receive(wait) match {
        case Some(msg) =>
          messages += msg
          remainingWait = math.max(0, waitMillis - (System.currentTimeMillis() - startTime))
        case None =>
          continue = false
      }
    }

    val duration = System.currentTimeMillis() - startTime
    _stats.recordPoll(messages.size, duration)

    messages.toSeq
  }

  override def send(message: MQMessage): Try[Unit] = {
    Failure(new UnsupportedOperationException("Send not yet implemented"))
  }

  override def commit(): Try[Unit] = {
    Try(transport.commit())
  }

  override def rollback(): Try[Unit] = {
    Try(transport.rollback())
  }

  override def close(): Unit = {
    if (isConnected) {
      transport.disconnect()
    }
    _connectionState = MQConnectionState.Disconnected
  }

  private def buildConnectionConfig(config: MQConnectorConfig): MQConnectionConfig = {
    MQConnectionConfig.from(config, MQSecurityConfig())
  }
}
