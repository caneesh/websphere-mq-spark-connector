package com.ibm.mq.spark.core.connection

import com.ibm.mq.spark.core.config.{MQConnectorConfig, MQSecurityConfig}

import scala.util.{Failure, Success, Try}

case class MQConnectionConfig(
    queueManager: String,
    channel: String,
    host: String,
    port: Int,
    hosts: Seq[String],
    ports: Seq[Int],
    user: Option[String],
    password: Option[String],
    usesCcdt: Boolean,
    ccdtFilePath: Option[String],
    sslCipherSuite: Option[String] = None,
    sslKeyStorePath: Option[String] = None,
    sslKeyStorePassword: Option[String] = None,
    sslTrustStorePath: Option[String] = None,
    sslTrustStorePassword: Option[String] = None,
    connectTimeout: Long = 30000L,
    readTimeout: Long = 30000L
) {

  def validate(): List[String] = {
    val errors = List.newBuilder[String]

    if (queueManager.isEmpty) errors += "queueManager is required"
    if (!usesCcdt) {
      if (channel.isEmpty) errors += "channel is required when not using CCDT"
      if (hosts.isEmpty) errors += "at least one host is required when not using CCDT"
    }
    if (usesCcdt && ccdtFilePath.isEmpty) {
      errors += "ccdtFilePath is required when using CCDT"
    }

    errors.result()
  }

  def connectionString: String = {
    if (hosts.size == 1) {
      s"${hosts.head}(${ports.head})"
    } else {
      hosts.zip(ports).map { case (h, p) => s"$h($p)" }.mkString(",")
    }
  }
}

object MQConnectionConfig {

  def from(connectorConfig: MQConnectorConfig, securityConfig: MQSecurityConfig): MQConnectionConfig = {
    val (hosts, ports) = if (securityConfig.usesCcdt) {
      (Seq.empty[String], Seq.empty[Int])
    } else {
      parseConnectionName(connectorConfig.connectionName) match {
        case Success((h, p)) => (h, p)
        case Failure(_) => (Seq.empty[String], Seq.empty[Int])
      }
    }

    MQConnectionConfig(
      queueManager = connectorConfig.queueManager,
      channel = connectorConfig.channel,
      host = hosts.headOption.getOrElse(""),
      port = ports.headOption.getOrElse(1414),
      hosts = hosts,
      ports = ports,
      user = securityConfig.user.orElse(connectorConfig.user),
      password = securityConfig.password.orElse(connectorConfig.password),
      usesCcdt = securityConfig.usesCcdt,
      ccdtFilePath = securityConfig.ccdtFilePath,
      sslCipherSuite = securityConfig.sslCipherSuite.orElse(connectorConfig.sslCipherSuite),
      sslKeyStorePath = securityConfig.sslKeyStorePath.orElse(connectorConfig.sslKeyStore),
      sslKeyStorePassword = securityConfig.sslKeyStorePassword.orElse(connectorConfig.sslKeyStorePassword),
      sslTrustStorePath = securityConfig.sslTrustStorePath,
      sslTrustStorePassword = securityConfig.sslTrustStorePassword,
      connectTimeout = connectorConfig.connectTimeout,
      readTimeout = connectorConfig.readTimeout
    )
  }

  def parseConnectionName(connectionName: String): Try[(Seq[String], Seq[Int])] = {
    Try {
      if (connectionName.isEmpty) {
        (Seq.empty[String], Seq.empty[Int])
      } else {
        val pattern = """([^(]+)\((\d+)\)""".r
        val parts = connectionName.split(",").map(_.trim)

        val parsed = parts.map {
          case pattern(host, port) => (host, port.toInt)
          case other => throw new IllegalArgumentException(s"Invalid connection name format: $other")
        }

        (parsed.map(_._1).toSeq, parsed.map(_._2).toSeq)
      }
    }
  }
}
