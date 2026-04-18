package com.ibm.mq.spark.core.client

import com.ibm.mq.spark.core.{MQException => ConnectorMQException}
import com.ibm.mq.spark.core.connection.MQConnectionConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for RealMQTransport.
 *
 * Note: These tests verify the transport behavior without a real MQ server.
 * Integration tests against a real MQ instance should be run separately
 * using Testcontainers or a dedicated MQ environment.
 */
class RealMQTransportSpec extends AnyFlatSpec with Matchers {

  "RealMQTransport" should "validate configuration before connecting" in {
    val transport = new RealMQTransport("TEST.QUEUE")
    val invalidConfig = MQConnectionConfig(
      queueManager = "", // missing
      channel = "TEST.CHANNEL",
      host = "localhost",
      port = 1414,
      hosts = Seq("localhost"),
      ports = Seq(1414),
      user = None,
      password = None,
      usesCcdt = false,
      ccdtFilePath = None
    )

    val exception = intercept[ConnectorMQException] {
      transport.connect(invalidConfig)
    }
    exception.getMessage should include("Invalid configuration")
    exception.getMessage should include("queueManager is required")
  }

  it should "validate channel is required when not using CCDT" in {
    val transport = new RealMQTransport("TEST.QUEUE")
    val invalidConfig = MQConnectionConfig(
      queueManager = "QM1",
      channel = "", // missing
      host = "localhost",
      port = 1414,
      hosts = Seq("localhost"),
      ports = Seq(1414),
      user = None,
      password = None,
      usesCcdt = false,
      ccdtFilePath = None
    )

    val exception = intercept[ConnectorMQException] {
      transport.connect(invalidConfig)
    }
    exception.getMessage should include("channel is required")
  }

  it should "report not connected initially" in {
    val transport = new RealMQTransport("TEST.QUEUE")
    transport.isConnected shouldBe false
  }

  it should "throw when receiving without connection" in {
    val transport = new RealMQTransport("TEST.QUEUE")

    val exception = intercept[ConnectorMQException] {
      transport.receive(1000)
    }
    exception.getMessage should include("Not connected")
  }

  it should "throw when committing without connection" in {
    val transport = new RealMQTransport("TEST.QUEUE")

    val exception = intercept[ConnectorMQException] {
      transport.commit()
    }
    exception.getMessage should include("Not connected")
  }

  it should "not throw when rolling back without connection" in {
    val transport = new RealMQTransport("TEST.QUEUE")
    noException should be thrownBy transport.rollback()
  }

  it should "not throw when disconnecting without connection" in {
    val transport = new RealMQTransport("TEST.QUEUE")
    noException should be thrownBy transport.disconnect()
  }

  "MQException factory methods" should "create appropriate exception types" in {
    val connEx = ConnectorMQException.connectionFailed("test")
    connEx.getMessage should include("Connection failed")

    val authEx = ConnectorMQException.authenticationFailed("test")
    authEx.getMessage should include("Authentication failed")

    val configEx = ConnectorMQException.configurationError("test")
    configEx.getMessage should include("Configuration error")

    val transportEx = ConnectorMQException.transportError("test")
    transportEx.getMessage should include("Transport error")

    val txEx = ConnectorMQException.transactionRolledBack("test")
    txEx.getMessage should include("Transaction rolled back")

    val parseEx = ConnectorMQException.parseFailed("test")
    parseEx.getMessage should include("Parse failed")
  }
}

/**
 * Integration test configuration for RealMQTransport.
 *
 * To run integration tests against a real MQ server:
 *
 * 1. Using Docker/Testcontainers:
 *    docker run -d -p 1414:1414 -p 9443:9443 \
 *      -e LICENSE=accept \
 *      -e MQ_QMGR_NAME=QM1 \
 *      ibmcom/mq:latest
 *
 * 2. Set environment variables:
 *    MQ_HOST=localhost
 *    MQ_PORT=1414
 *    MQ_QUEUE_MANAGER=QM1
 *    MQ_CHANNEL=DEV.APP.SVRCONN
 *    MQ_QUEUE=DEV.QUEUE.1
 *    MQ_USER=app
 *    MQ_PASSWORD=passw0rd
 *
 * 3. Run with integration profile:
 *    mvn test -Pintegration
 */
object RealMQTransportIntegrationTest {

  def createTestConfig(): MQConnectionConfig = {
    val host = sys.env.getOrElse("MQ_HOST", "localhost")
    val port = sys.env.getOrElse("MQ_PORT", "1414").toInt
    val qm = sys.env.getOrElse("MQ_QUEUE_MANAGER", "QM1")
    val channel = sys.env.getOrElse("MQ_CHANNEL", "DEV.APP.SVRCONN")
    val user = sys.env.get("MQ_USER")
    val password = sys.env.get("MQ_PASSWORD")

    MQConnectionConfig(
      queueManager = qm,
      channel = channel,
      host = host,
      port = port,
      hosts = Seq(host),
      ports = Seq(port),
      user = user,
      password = password,
      usesCcdt = false,
      ccdtFilePath = None
    )
  }

  def isIntegrationEnabled: Boolean = {
    sys.env.contains("MQ_HOST") || sys.props.contains("integration")
  }
}
