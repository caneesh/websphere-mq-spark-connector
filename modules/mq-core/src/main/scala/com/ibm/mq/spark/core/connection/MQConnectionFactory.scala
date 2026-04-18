package com.ibm.mq.spark.core.connection

object MQConnectionFactory {

  val XMSC_WMQ_HOST_NAME = "XMSC_WMQ_HOST_NAME"
  val XMSC_WMQ_PORT = "XMSC_WMQ_PORT"
  val XMSC_WMQ_CHANNEL = "XMSC_WMQ_CHANNEL"
  val XMSC_WMQ_QUEUE_MANAGER = "XMSC_WMQ_QUEUE_MANAGER"
  val XMSC_WMQ_CONNECTION_NAME_LIST = "XMSC_WMQ_CONNECTION_NAME_LIST"
  val XMSC_WMQ_SSL_CIPHER_SUITE = "XMSC_WMQ_SSL_CIPHER_SUITE"
  val XMSC_WMQ_SSL_KEY_REPOSITORY = "XMSC_WMQ_SSL_KEY_REPOSITORY"
  val XMSC_USERID = "XMSC_USERID"
  val XMSC_PASSWORD = "XMSC_PASSWORD"
  val XMSC_WMQ_CCDT_URL = "XMSC_WMQ_CCDT_URL"
  val XMSC_WMQ_CONNECTION_MODE = "XMSC_WMQ_CONNECTION_MODE"

  def buildProperties(config: MQConnectionConfig): Map[String, String] = {
    val props = scala.collection.mutable.Map[String, String]()

    props += XMSC_WMQ_QUEUE_MANAGER -> config.queueManager

    if (config.usesCcdt) {
      config.ccdtFilePath.foreach { path =>
        props += XMSC_WMQ_CCDT_URL -> s"file://$path"
      }
    } else {
      props += XMSC_WMQ_HOST_NAME -> config.host
      props += XMSC_WMQ_PORT -> config.port.toString
      props += XMSC_WMQ_CHANNEL -> config.channel

      if (config.hosts.size > 1) {
        props += XMSC_WMQ_CONNECTION_NAME_LIST -> config.connectionString
      }
    }

    config.user.foreach(u => props += XMSC_USERID -> u)
    config.password.foreach(p => props += XMSC_PASSWORD -> p)

    config.sslCipherSuite.foreach { cipher =>
      props += XMSC_WMQ_SSL_CIPHER_SUITE -> cipher
    }

    props += XMSC_WMQ_CONNECTION_MODE -> "1"

    props.toMap
  }

  def buildJavaProperties(config: MQConnectionConfig): java.util.Properties = {
    val props = new java.util.Properties()
    buildProperties(config).foreach { case (k, v) => props.setProperty(k, v) }
    props
  }
}
