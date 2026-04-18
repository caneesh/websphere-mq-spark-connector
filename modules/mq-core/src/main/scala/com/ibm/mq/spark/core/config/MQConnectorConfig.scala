package com.ibm.mq.spark.core.config

/**
 * Configuration for IBM MQ connector connections.
 *
 * @param queueManager the name of the MQ queue manager
 * @param channel the MQ channel name
 * @param connectionName the host and port in format "host(port)"
 * @param queueName the target queue name
 * @param user optional username for authentication
 * @param password optional password for authentication
 * @param sslCipherSuite optional SSL cipher suite
 * @param sslKeyStore optional path to SSL key store
 * @param sslKeyStorePassword optional SSL key store password
 * @param connectTimeout connection timeout in milliseconds
 * @param readTimeout read timeout in milliseconds
 */
case class MQConnectorConfig(
    queueManager: String,
    channel: String,
    connectionName: String,
    queueName: String,
    user: Option[String] = None,
    password: Option[String] = None,
    sslCipherSuite: Option[String] = None,
    sslKeyStore: Option[String] = None,
    sslKeyStorePassword: Option[String] = None,
    connectTimeout: Long = 30000L,
    readTimeout: Long = 30000L
) {

  /**
   * Validates the configuration.
   *
   * @return a list of validation errors, empty if valid
   */
  def validate(): List[String] = {
    val errors = List.newBuilder[String]

    if (queueManager.isEmpty) errors += "queueManager is required"
    if (channel.isEmpty) errors += "channel is required"
    if (connectionName.isEmpty) errors += "connectionName is required"
    if (queueName.isEmpty) errors += "queueName is required"
    if (connectTimeout <= 0) errors += "connectTimeout must be positive"
    if (readTimeout <= 0) errors += "readTimeout must be positive"

    errors.result()
  }

  /**
   * Indicates whether SSL is configured.
   *
   * @return true if SSL cipher suite is specified
   */
  def isSslEnabled: Boolean = sslCipherSuite.isDefined
}
