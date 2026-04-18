package com.ibm.mq.spark.testkit

import com.ibm.mq.spark.core.connection.MQConnectionConfig
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.LoggerFactory

/**
 * Test support trait for integration tests against a real MQ server.
 *
 * Usage:
 * {{{
 * class MyIntegrationSpec extends AnyFlatSpec with MQContainerSupport {
 *   "My test" should "connect to MQ" in {
 *     assumeIntegrationEnabled()
 *     val config = createTestConnectionConfig()
 *     // use config to connect
 *   }
 * }
 * }}}
 *
 * Configuration via environment variables:
 * - MQ_HOST: MQ server hostname (default: localhost)
 * - MQ_PORT: MQ server port (default: 1414)
 * - MQ_QUEUE_MANAGER: Queue manager name (default: QM1)
 * - MQ_CHANNEL: Channel name (default: DEV.APP.SVRCONN)
 * - MQ_QUEUE: Test queue name (default: DEV.QUEUE.1)
 * - MQ_USER: Authentication user (optional)
 * - MQ_PASSWORD: Authentication password (optional)
 * - MQ_INTEGRATION_ENABLED: Set to "true" to enable integration tests
 *
 * Running with Docker:
 * {{{
 * docker run -d --name mq-test \
 *   -e LICENSE=accept \
 *   -e MQ_QMGR_NAME=QM1 \
 *   -e MQ_APP_PASSWORD=passw0rd \
 *   -p 1414:1414 -p 9443:9443 \
 *   ibmcom/mq:latest
 *
 * export MQ_INTEGRATION_ENABLED=true
 * export MQ_USER=app
 * export MQ_PASSWORD=passw0rd
 * mvn test -Pintegration
 * }}}
 */
trait MQContainerSupport extends BeforeAndAfterAll { self: Suite =>

  private val log = LoggerFactory.getLogger(getClass)

  protected lazy val mqHost: String = sys.env.getOrElse("MQ_HOST", "localhost")
  protected lazy val mqPort: Int = sys.env.getOrElse("MQ_PORT", "1414").toInt
  protected lazy val mqQueueManager: String = sys.env.getOrElse("MQ_QUEUE_MANAGER", "QM1")
  protected lazy val mqChannel: String = sys.env.getOrElse("MQ_CHANNEL", "DEV.APP.SVRCONN")
  protected lazy val mqQueue: String = sys.env.getOrElse("MQ_QUEUE", "DEV.QUEUE.1")
  protected lazy val mqUser: Option[String] = sys.env.get("MQ_USER")
  protected lazy val mqPassword: Option[String] = sys.env.get("MQ_PASSWORD")

  protected def isIntegrationEnabled: Boolean = {
    sys.env.get("MQ_INTEGRATION_ENABLED").exists(_.equalsIgnoreCase("true")) ||
    sys.props.get("integration").exists(_.equalsIgnoreCase("true"))
  }

  protected def assumeIntegrationEnabled(): Unit = {
    if (!isIntegrationEnabled) {
      cancel("Integration tests disabled. Set MQ_INTEGRATION_ENABLED=true to enable.")
    }
  }

  protected def createTestConnectionConfig(): MQConnectionConfig = {
    MQConnectionConfig(
      queueManager = mqQueueManager,
      channel = mqChannel,
      host = mqHost,
      port = mqPort,
      hosts = Seq(mqHost),
      ports = Seq(mqPort),
      user = mqUser,
      password = mqPassword,
      usesCcdt = false,
      ccdtFilePath = None
    )
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (isIntegrationEnabled) {
      log.info(s"Integration tests enabled. MQ target: $mqHost:$mqPort, QM: $mqQueueManager, Queue: $mqQueue")
    }
  }
}

/**
 * Container configuration for IBM MQ Docker image.
 */
object MQContainerConfig {
  val DefaultImage = "ibmcom/mq:latest"
  val DefaultPort = 1414
  val DefaultWebConsolePort = 9443
  val DefaultQueueManager = "QM1"
  val DefaultChannel = "DEV.APP.SVRCONN"
  val DefaultQueue = "DEV.QUEUE.1"
  val DefaultUser = "app"
  val DefaultPassword = "passw0rd"

  def dockerRunCommand(
      queueManager: String = DefaultQueueManager,
      password: String = DefaultPassword,
      hostPort: Int = DefaultPort,
      webPort: Int = DefaultWebConsolePort
  ): String = {
    s"""docker run -d --name mq-test-container \\
       |  -e LICENSE=accept \\
       |  -e MQ_QMGR_NAME=$queueManager \\
       |  -e MQ_APP_PASSWORD=$password \\
       |  -p $hostPort:1414 \\
       |  -p $webPort:9443 \\
       |  $DefaultImage""".stripMargin
  }
}
