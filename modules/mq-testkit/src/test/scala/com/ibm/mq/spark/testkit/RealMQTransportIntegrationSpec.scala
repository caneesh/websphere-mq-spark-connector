package com.ibm.mq.spark.testkit

import com.ibm.mq.spark.core.client.RealMQTransport
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Integration tests for RealMQTransport against a real MQ server.
 *
 * These tests are skipped by default. To run them:
 *
 * 1. Start an MQ container:
 *    docker run -d --name mq-test \
 *      -e LICENSE=accept \
 *      -e MQ_QMGR_NAME=QM1 \
 *      -e MQ_APP_PASSWORD=passw0rd \
 *      -p 1414:1414 -p 9443:9443 \
 *      ibmcom/mq:latest
 *
 * 2. Run with environment variables:
 *    MQ_INTEGRATION_ENABLED=true MQ_USER=app MQ_PASSWORD=passw0rd mvn test
 *
 * 3. Or run with Maven profile:
 *    mvn test -Pintegration
 */
class RealMQTransportIntegrationSpec extends AnyFlatSpec with Matchers with MQContainerSupport {

  "RealMQTransport" should "connect to real MQ server" in {
    assumeIntegrationEnabled()

    val transport = new RealMQTransport(mqQueue)
    val config = createTestConnectionConfig()

    noException should be thrownBy {
      transport.connect(config)
      transport.isConnected shouldBe true
      transport.disconnect()
    }
  }

  it should "receive messages from queue" in {
    assumeIntegrationEnabled()

    val transport = new RealMQTransport(mqQueue)
    val config = createTestConnectionConfig()

    transport.connect(config)
    try {
      val message = transport.receive(1000)
    } finally {
      transport.rollback()
      transport.disconnect()
    }
  }

  it should "commit transaction after receiving" in {
    assumeIntegrationEnabled()

    val transport = new RealMQTransport(mqQueue)
    val config = createTestConnectionConfig()

    transport.connect(config)
    try {
      val message = transport.receive(1000)
      if (message.isDefined) {
        transport.commit()
      }
    } finally {
      transport.disconnect()
    }
  }

  it should "rollback transaction to redeliver messages" in {
    assumeIntegrationEnabled()

    val transport = new RealMQTransport(mqQueue)
    val config = createTestConnectionConfig()

    transport.connect(config)
    try {
      val message = transport.receive(1000)
      transport.rollback()

      if (message.isDefined) {
        val redelivered = transport.receive(1000)
        redelivered shouldBe defined
      }
    } finally {
      transport.rollback()
      transport.disconnect()
    }
  }

  it should "extract message metadata" in {
    assumeIntegrationEnabled()

    val transport = new RealMQTransport(mqQueue)
    val config = createTestConnectionConfig()

    transport.connect(config)
    try {
      val message = transport.receive(1000)
      message.foreach { msg =>
        msg.messageId should not be empty
        msg.queueName shouldBe mqQueue
        msg.ccsid should be > 0
        msg.putTimestamp should be > 0L
      }
    } finally {
      transport.rollback()
      transport.disconnect()
    }
  }

  it should "handle empty queue gracefully" in {
    assumeIntegrationEnabled()

    val emptyQueue = sys.env.getOrElse("MQ_EMPTY_QUEUE", "DEV.QUEUE.2")
    val transport = new RealMQTransport(emptyQueue)
    val config = createTestConnectionConfig()

    transport.connect(config)
    try {
      val message = transport.receive(100)
      message shouldBe None
    } finally {
      transport.disconnect()
    }
  }
}
