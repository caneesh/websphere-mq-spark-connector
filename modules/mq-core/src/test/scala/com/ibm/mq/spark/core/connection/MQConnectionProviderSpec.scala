package com.ibm.mq.spark.core.connection

import com.ibm.mq.spark.core.config.{MQConnectorConfig, MQSecurityConfig}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MQConnectionProviderSpec extends AnyFlatSpec with Matchers {

  "MQConnectionConfig" should "build from connector and security configs" in {
    val connectorConfig = MQConnectorConfig(
      queueManager = "QM1",
      channel = "DEV.APP.SVRCONN",
      connectionName = "localhost(1414)",
      queueName = "DEV.QUEUE.1"
    )

    val securityConfig = MQSecurityConfig(
      user = Some("app"),
      password = Some("passw0rd")
    )

    val connectionConfig = MQConnectionConfig.from(connectorConfig, securityConfig)

    connectionConfig.queueManager shouldBe "QM1"
    connectionConfig.channel shouldBe "DEV.APP.SVRCONN"
    connectionConfig.host shouldBe "localhost"
    connectionConfig.port shouldBe 1414
    connectionConfig.user shouldBe Some("app")
  }

  it should "parse multiple hosts from connection name" in {
    val config = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CH1",
      connectionName = "host1(1414),host2(1415),host3(1416)",
      queueName = "Q1"
    )

    val connectionConfig = MQConnectionConfig.from(config, MQSecurityConfig())

    connectionConfig.hosts should contain allOf ("host1", "host2", "host3")
    connectionConfig.ports should contain allOf (1414, 1415, 1416)
  }

  it should "validate connection name format" in {
    val config = MQConnectorConfig(
      queueManager = "QM1",
      channel = "CH1",
      connectionName = "invalid-format",
      queueName = "Q1"
    )

    val result = MQConnectionConfig.parseConnectionName(config.connectionName)
    result.isFailure shouldBe true
  }

  it should "support CCDT configuration" in {
    val securityConfig = MQSecurityConfig(
      ccdtFilePath = Some("/path/to/ccdt.tab")
    )

    val connectorConfig = MQConnectorConfig(
      queueManager = "QM1",
      channel = "",
      connectionName = "",
      queueName = "Q1"
    )

    val connectionConfig = MQConnectionConfig.from(connectorConfig, securityConfig)

    connectionConfig.usesCcdt shouldBe true
    connectionConfig.ccdtFilePath shouldBe Some("/path/to/ccdt.tab")
  }

  "MQConnectionFactory" should "create MQ connection properties" in {
    val connectionConfig = MQConnectionConfig(
      queueManager = "QM1",
      channel = "DEV.APP.SVRCONN",
      host = "localhost",
      port = 1414,
      hosts = Seq("localhost"),
      ports = Seq(1414),
      user = Some("app"),
      password = Some("passw0rd"),
      usesCcdt = false,
      ccdtFilePath = None,
      sslCipherSuite = None,
      connectTimeout = 30000,
      readTimeout = 30000
    )

    val props = MQConnectionFactory.buildProperties(connectionConfig)

    props should contain key "XMSC_WMQ_HOST_NAME"
    props should contain key "XMSC_WMQ_PORT"
    props should contain key "XMSC_WMQ_CHANNEL"
    props should contain key "XMSC_WMQ_QUEUE_MANAGER"
    props("XMSC_WMQ_HOST_NAME") shouldBe "localhost"
    props("XMSC_WMQ_PORT") shouldBe "1414"
  }

  it should "include SSL properties when configured" in {
    val connectionConfig = MQConnectionConfig(
      queueManager = "QM1",
      channel = "SSL.CHANNEL",
      host = "secure.example.com",
      port = 1414,
      hosts = Seq("secure.example.com"),
      ports = Seq(1414),
      user = None,
      password = None,
      usesCcdt = false,
      ccdtFilePath = None,
      sslCipherSuite = Some("TLS_RSA_WITH_AES_256_CBC_SHA256"),
      sslKeyStorePath = Some("/path/to/keystore.jks"),
      sslTrustStorePath = Some("/path/to/truststore.jks"),
      connectTimeout = 30000,
      readTimeout = 30000
    )

    val props = MQConnectionFactory.buildProperties(connectionConfig)

    props should contain key "XMSC_WMQ_SSL_CIPHER_SUITE"
    props("XMSC_WMQ_SSL_CIPHER_SUITE") shouldBe "TLS_RSA_WITH_AES_256_CBC_SHA256"
  }

  "MQConnectionState" should "track connection lifecycle" in {
    var state: MQConnectionState = MQConnectionState.Disconnected

    state.isConnected shouldBe false
    state.canConnect shouldBe true

    state = MQConnectionState.Connecting
    state.isConnected shouldBe false
    state.canConnect shouldBe false

    state = MQConnectionState.Connected
    state.isConnected shouldBe true
    state.canConnect shouldBe false

    state = MQConnectionState.Reconnecting
    state.isConnected shouldBe false
    state.canConnect shouldBe false

    state = MQConnectionState.Failed("Connection refused")
    state.isConnected shouldBe false
    state.canConnect shouldBe true
  }
}
