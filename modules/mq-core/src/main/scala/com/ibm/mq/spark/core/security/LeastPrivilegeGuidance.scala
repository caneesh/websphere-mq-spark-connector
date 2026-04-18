package com.ibm.mq.spark.core.security

/**
 * Provides guidance for configuring IBM MQ with least privilege access.
 *
 * These are recommendations - actual MQ security configuration depends
 * on your environment and security requirements.
 */
object LeastPrivilegeGuidance {

  /**
   * Returns MQ authority guidance for a queue consumer application.
   */
  def forQueueConsumer(queueName: String): String = {
    s"""
       |# Least Privilege for Queue Consumer
       |# Grant minimal permissions to read from $queueName
       |
       |# 1. Create an MQ user for the application
       |# runmqsc QM1
       |
       |# 2. Grant GET access to the queue
       |SET AUTHREC PROFILE('$queueName') OBJTYPE(QUEUE) PRINCIPAL('app_user') AUTHADD(GET, INQ, BROWSE)
       |
       |# 3. Grant connect access to the queue manager
       |SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('app_user') AUTHADD(CONNECT, INQ)
       |
       |# 4. Grant access to the channel
       |# SET CHLAUTH(DEV.APP.SVRCONN) TYPE(USERMAP) CLNTUSER('app_user') USERSRC(CHANNEL)
       |
       |# Note: Avoid granting PUT, PASSALL, PASSID, SETALL, SETID permissions unless required
       |""".stripMargin
  }

  /**
   * Returns MQ authority guidance for a queue producer application.
   */
  def forQueueProducer(queueName: String): String = {
    s"""
       |# Least Privilege for Queue Producer
       |# Grant minimal permissions to write to $queueName
       |
       |# 1. Create an MQ user for the application
       |# runmqsc QM1
       |
       |# 2. Grant PUT access to the queue
       |SET AUTHREC PROFILE('$queueName') OBJTYPE(QUEUE) PRINCIPAL('app_user') AUTHADD(PUT, INQ)
       |
       |# 3. Grant connect access to the queue manager
       |SET AUTHREC OBJTYPE(QMGR) PRINCIPAL('app_user') AUTHADD(CONNECT, INQ)
       |
       |# Note: Avoid granting GET, BROWSE unless required
       |""".stripMargin
  }

  /**
   * Returns guidance for channel access configuration.
   */
  def forChannelAccess(channelName: String): String = {
    s"""
       |# Channel Access Configuration for $channelName
       |
       |# 1. Enable channel authentication
       |ALTER QMGR CHLAUTH(ENABLED)
       |
       |# 2. Block default access (deny all by default)
       |SET CHLAUTH('$channelName') TYPE(ADDRESSMAP) ADDRESS('*') USERSRC(NOACCESS)
       |
       |# 3. Allow specific users/addresses
       |SET CHLAUTH('$channelName') TYPE(ADDRESSMAP) ADDRESS('10.0.0.*') USERSRC(CHANNEL)
       |
       |# 4. Map authenticated users
       |SET CHLAUTH('$channelName') TYPE(USERMAP) CLNTUSER('app_user') USERSRC(CHANNEL) CHCKCLNT(REQUIRED)
       |
       |# 5. Require TLS for production
       |ALTER CHANNEL('$channelName') CHLTYPE(SVRCONN) SSLCIPH(TLS_RSA_WITH_AES_256_CBC_SHA256)
       |""".stripMargin
  }

  /**
   * Returns general security best practices.
   */
  def generalBestPractices: String = {
    """
      |# IBM MQ Security Best Practices
      |
      |## Authentication
      |- Always use connection authentication (CONNAUTH)
      |- Use TLS for all production connections
      |- Rotate passwords/certificates regularly
      |- Never embed credentials in code
      |
      |## Authorization
      |- Apply least privilege principle
      |- Use application-specific MQ users
      |- Avoid using mqm group for applications
      |- Audit authority changes
      |
      |## Network Security
      |- Use TLS 1.2 or higher
      |- Configure appropriate cipher suites
      |- Use certificate authentication where possible
      |- Restrict network access with CHLAUTH rules
      |
      |## Monitoring
      |- Enable authority events
      |- Monitor failed connection attempts
      |- Review channel status regularly
      |""".stripMargin
  }
}
