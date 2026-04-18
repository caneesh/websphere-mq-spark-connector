package com.ibm.mq.spark.source

import com.ibm.mq.spark.core.config.MQConnectorConfig

/**
 * Configuration options for the MQ Spark data source.
 *
 * Parses Spark DataSource options and creates the underlying
 * MQConnectorConfig for MQ client operations.
 *
 * @param queueManager the MQ queue manager name
 * @param channel the MQ channel name
 * @param connectionName the host and port in format "host(port)"
 * @param queueName the target queue name
 * @param user optional username for authentication
 * @param password optional password for authentication
 * @param sslCipherSuite optional SSL cipher suite
 * @param batchSize number of messages to fetch per batch
 * @param pollTimeoutMs timeout for polling messages in milliseconds
 */
case class MQSourceOptions(
    queueManager: String,
    channel: String,
    connectionName: String,
    queueName: String,
    user: Option[String] = None,
    password: Option[String] = None,
    sslCipherSuite: Option[String] = None,
    batchSize: Int = 1000,
    pollTimeoutMs: Long = 5000L
) {

  /**
   * Converts to the core MQConnectorConfig.
   *
   * @return the connector configuration
   */
  def toConnectorConfig: MQConnectorConfig = MQConnectorConfig(
    queueManager = queueManager,
    channel = channel,
    connectionName = connectionName,
    queueName = queueName,
    user = user,
    password = password,
    sslCipherSuite = sslCipherSuite
  )
}

object MQSourceOptions {

  val QUEUE_MANAGER = "queueManager"
  val CHANNEL = "channel"
  val CONNECTION_NAME = "connectionName"
  val QUEUE_NAME = "queueName"
  val USER = "user"
  val PASSWORD = "password"
  val SSL_CIPHER_SUITE = "sslCipherSuite"
  val BATCH_SIZE = "batchSize"
  val POLL_TIMEOUT_MS = "pollTimeoutMs"

  /**
   * Creates MQSourceOptions from a map of string options.
   *
   * @param options the options map
   * @return the parsed options
   * @throws IllegalArgumentException if required options are missing
   */
  def fromMap(options: Map[String, String]): MQSourceOptions = {
    def require(key: String): String =
      options.getOrElse(key, throw new IllegalArgumentException(s"Option '$key' is required"))

    MQSourceOptions(
      queueManager = require(QUEUE_MANAGER),
      channel = require(CHANNEL),
      connectionName = require(CONNECTION_NAME),
      queueName = require(QUEUE_NAME),
      user = options.get(USER),
      password = options.get(PASSWORD),
      sslCipherSuite = options.get(SSL_CIPHER_SUITE),
      batchSize = options.get(BATCH_SIZE).map(_.toInt).getOrElse(1000),
      pollTimeoutMs = options.get(POLL_TIMEOUT_MS).map(_.toLong).getOrElse(5000L)
    )
  }
}
