package com.ibm.mq.spark.core.client

import com.ibm.mq.{MQException => IBMMQException, _}
import com.ibm.mq.constants.CMQC
import com.ibm.mq.spark.core.connection.MQConnectionConfig
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.{MQException => ConnectorMQException}
import org.slf4j.LoggerFactory

import java.util.Hashtable
import scala.util.{Failure, Success, Try}

/**
 * Real IBM MQ transport implementation using IBM MQ base Java classes.
 *
 * Design decision: Using IBM MQ base classes instead of JMS because:
 * - Direct access to MQ-specific metadata (MQMD fields)
 * - Native syncpoint (transaction) support
 * - Better control over get options and message handling
 * - No JMS abstraction overhead
 *
 * Thread safety: This transport is NOT thread-safe. Each Spark partition
 * should use its own transport instance.
 *
 * @param queueName the name of the queue to consume from
 */
class RealMQTransport(queueName: String) extends MQTransport {

  private val log = LoggerFactory.getLogger(getClass)

  @volatile private var queueManager: MQQueueManager = _
  @volatile private var queue: MQQueue = _
  @volatile private var config: MQConnectionConfig = _
  @volatile private var _connected: Boolean = false

  override def connect(connectionConfig: MQConnectionConfig): Unit = {
    if (_connected) {
      log.warn("Already connected, ignoring connect request")
      return
    }

    this.config = connectionConfig
    val errors = connectionConfig.validate()
    if (errors.nonEmpty) {
      throw ConnectorMQException.configurationError(s"Invalid configuration: ${errors.mkString(", ")}")
    }

    Try {
      val props = buildConnectionProperties(connectionConfig)
      log.info(s"Connecting to queue manager ${connectionConfig.queueManager}")

      queueManager = new MQQueueManager(connectionConfig.queueManager, props)

      val openOptions = CMQC.MQOO_INPUT_SHARED | CMQC.MQOO_FAIL_IF_QUIESCING
      queue = queueManager.accessQueue(queueName, openOptions)

      _connected = true
      log.info(s"Connected to queue $queueName on ${connectionConfig.queueManager}")
    } match {
      case Success(_) => // already logged
      case Failure(e: IBMMQException) =>
        _connected = false
        throw classifyMQException(e, "connect")
      case Failure(e) =>
        _connected = false
        throw ConnectorMQException.connectionFailed(s"Failed to connect: ${e.getMessage}", e)
    }
  }

  override def disconnect(): Unit = {
    Try {
      if (queue != null) {
        queue.close()
        queue = null
      }
      if (queueManager != null) {
        queueManager.disconnect()
        queueManager = null
      }
      _connected = false
      log.info("Disconnected from MQ")
    } match {
      case Success(_) => // ok
      case Failure(e: IBMMQException) =>
        log.warn(s"Error during disconnect: ${e.getMessage}")
        _connected = false
      case Failure(e) =>
        log.warn(s"Unexpected error during disconnect: ${e.getMessage}")
        _connected = false
    }
  }

  override def isConnected: Boolean = _connected && queueManager != null && queueManager.isConnected

  override def receive(waitMillis: Long): Option[RawMQMessage] = {
    if (!isConnected) {
      throw ConnectorMQException.connectionFailed("Not connected to MQ")
    }

    Try {
      val message = new MQMessage()
      val gmo = new MQGetMessageOptions()

      gmo.options = CMQC.MQGMO_WAIT |
        CMQC.MQGMO_SYNCPOINT |
        CMQC.MQGMO_FAIL_IF_QUIESCING |
        CMQC.MQGMO_CONVERT

      gmo.waitInterval = if (waitMillis > Int.MaxValue) Int.MaxValue else waitMillis.toInt

      queue.get(message, gmo)

      Some(extractRawMessage(message))
    } match {
      case Success(msg) => msg
      case Failure(e: IBMMQException) if e.getReason == CMQC.MQRC_NO_MSG_AVAILABLE =>
        None
      case Failure(e: IBMMQException) =>
        throw classifyMQException(e, "receive")
      case Failure(e) =>
        throw ConnectorMQException.transportError(s"Failed to receive message: ${e.getMessage}", e)
    }
  }

  override def commit(): Unit = {
    if (!isConnected) {
      throw ConnectorMQException.connectionFailed("Not connected to MQ")
    }

    Try {
      queueManager.commit()
      log.debug("Transaction committed")
    } match {
      case Success(_) => // ok
      case Failure(e: IBMMQException) =>
        throw classifyMQException(e, "commit")
      case Failure(e) =>
        throw ConnectorMQException.transportError(s"Failed to commit: ${e.getMessage}", e)
    }
  }

  override def rollback(): Unit = {
    if (!isConnected) {
      log.warn("Cannot rollback: not connected")
      return
    }

    Try {
      queueManager.backout()
      log.debug("Transaction rolled back")
    } match {
      case Success(_) => // ok
      case Failure(e: IBMMQException) =>
        log.warn(s"Rollback failed: ${e.getMessage}")
        throw classifyMQException(e, "rollback")
      case Failure(e) =>
        log.warn(s"Unexpected rollback error: ${e.getMessage}")
    }
  }

  private def buildConnectionProperties(config: MQConnectionConfig): Hashtable[String, Any] = {
    val props = new Hashtable[String, Any]()

    if (config.usesCcdt) {
      config.ccdtFilePath.foreach { path =>
        props.put(CMQC.CCSID_PROPERTY, 1208)
        System.setProperty("MQCHLLIB", path.substring(0, path.lastIndexOf('/')))
        System.setProperty("MQCHLTAB", path.substring(path.lastIndexOf('/') + 1))
      }
    } else {
      props.put(CMQC.HOST_NAME_PROPERTY, config.host)
      props.put(CMQC.PORT_PROPERTY, config.port.asInstanceOf[java.lang.Integer])
      props.put(CMQC.CHANNEL_PROPERTY, config.channel)
    }

    props.put(CMQC.TRANSPORT_PROPERTY, CMQC.TRANSPORT_MQSERIES_CLIENT)

    config.user.filter(_.nonEmpty).foreach { user =>
      props.put(CMQC.USER_ID_PROPERTY, user)
    }
    config.password.filter(_.nonEmpty).foreach { pwd =>
      props.put(CMQC.PASSWORD_PROPERTY, pwd)
    }

    config.sslCipherSuite.foreach { cipher =>
      props.put("sslCipherSuite", cipher)
    }

    config.sslKeyStorePath.foreach { path =>
      System.setProperty("javax.net.ssl.keyStore", path)
    }
    config.sslKeyStorePassword.foreach { pwd =>
      System.setProperty("javax.net.ssl.keyStorePassword", pwd)
    }
    config.sslTrustStorePath.foreach { path =>
      System.setProperty("javax.net.ssl.trustStore", path)
    }
    config.sslTrustStorePassword.foreach { pwd =>
      System.setProperty("javax.net.ssl.trustStorePassword", pwd)
    }

    props.put(CMQC.CONNECT_OPTIONS_PROPERTY,
      CMQC.MQCNO_RECONNECT_Q_MGR.asInstanceOf[java.lang.Integer])

    props
  }

  private def extractRawMessage(msg: MQMessage): RawMQMessage = {
    val payload = new Array[Byte](msg.getDataLength)
    msg.readFully(payload)

    RawMQMessage(
      messageId = copyBytes(msg.messageId),
      correlationId = copyBytes(msg.correlationId),
      payload = payload,
      putTimestamp = convertMQTimestamp(msg.putDateTime),
      queueName = queueName,
      ccsid = msg.characterSet,
      encoding = msg.encoding,
      priority = msg.priority,
      expiry = msg.expiry,
      backoutCount = msg.backoutCount,
      replyToQueue = Option(msg.replyToQueueName).filter(_.trim.nonEmpty),
      replyToQueueManager = Option(msg.replyToQueueManagerName).filter(_.trim.nonEmpty),
      format = Option(msg.format).getOrElse("").trim,
      persistence = msg.persistence,
      messageType = msg.messageType,
      userId = Option(msg.userId).filter(_.trim.nonEmpty),
      applicationName = Option(msg.putApplicationName).filter(_.trim.nonEmpty),
      putApplicationType = msg.putApplicationType,
      groupId = if (msg.groupId != null && msg.groupId.exists(_ != 0)) Some(copyBytes(msg.groupId)) else None,
      messageSequenceNumber = msg.messageSequenceNumber,
      offset = msg.offset
    )
  }

  private def copyBytes(src: Array[Byte]): Array[Byte] = {
    if (src == null) Array.emptyByteArray
    else {
      val dest = new Array[Byte](src.length)
      System.arraycopy(src, 0, dest, 0, src.length)
      dest
    }
  }

  private def convertMQTimestamp(mqDateTime: java.util.GregorianCalendar): Long = {
    if (mqDateTime == null) System.currentTimeMillis()
    else mqDateTime.getTimeInMillis
  }

  private def classifyMQException(e: IBMMQException, operation: String): ConnectorMQException = {
    val reason = e.getReason

    reason match {
      case CMQC.MQRC_NOT_AUTHORIZED | CMQC.MQRC_SECURITY_ERROR =>
        ConnectorMQException.authenticationFailed(s"Authentication failed during $operation: ${e.getMessage}", e)

      case CMQC.MQRC_Q_MGR_NOT_AVAILABLE | CMQC.MQRC_CONNECTION_BROKEN |
           CMQC.MQRC_HOST_NOT_AVAILABLE | CMQC.MQRC_CHANNEL_NOT_AVAILABLE =>
        ConnectorMQException.connectionFailed(s"Connection error during $operation: ${e.getMessage}", e)

      case CMQC.MQRC_UNKNOWN_OBJECT_NAME | CMQC.MQRC_Q_MGR_NAME_ERROR =>
        ConnectorMQException.configurationError(s"Configuration error during $operation: ${e.getMessage}", e)

      case CMQC.MQRC_BACKED_OUT =>
        ConnectorMQException.transactionRolledBack(s"Transaction backed out during $operation: ${e.getMessage}", e)

      case _ =>
        ConnectorMQException.transportError(s"MQ error during $operation (reason=$reason): ${e.getMessage}", e)
    }
  }
}

object RealMQTransport {

  def apply(queueName: String): RealMQTransport = new RealMQTransport(queueName)
}
