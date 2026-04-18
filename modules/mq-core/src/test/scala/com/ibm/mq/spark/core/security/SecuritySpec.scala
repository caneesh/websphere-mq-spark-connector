package com.ibm.mq.spark.core.security

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SecuritySpec extends AnyFlatSpec with Matchers {

  "SecretRedactor" should "redact password fields" in {
    val config = Map(
      "password" -> "secret123",
      "username" -> "user1"
    )

    val redacted = SecretRedactor.redactMap(config)

    redacted("password") shouldBe "[REDACTED]"
    redacted("username") shouldBe "user1"
  }

  it should "redact keystore and truststore passwords" in {
    val config = Map(
      "sslKeyStorePassword" -> "keypass",
      "sslTrustStorePassword" -> "trustpass",
      "queueName" -> "MY.QUEUE"
    )

    val redacted = SecretRedactor.redactMap(config)

    redacted("sslKeyStorePassword") shouldBe "[REDACTED]"
    redacted("sslTrustStorePassword") shouldBe "[REDACTED]"
    redacted("queueName") shouldBe "MY.QUEUE"
  }

  it should "redact tokens and API keys" in {
    val config = Map(
      "apiKey" -> "key12345",
      "authToken" -> "token999",
      "secretKey" -> "mysecret"
    )

    val redacted = SecretRedactor.redactMap(config)

    redacted("apiKey") shouldBe "[REDACTED]"
    redacted("authToken") shouldBe "[REDACTED]"
    redacted("secretKey") shouldBe "[REDACTED]"
  }

  it should "be case insensitive" in {
    val config = Map(
      "PASSWORD" -> "secret",
      "Password" -> "secret2",
      "api_key" -> "key"
    )

    val redacted = SecretRedactor.redactMap(config)

    redacted("PASSWORD") shouldBe "[REDACTED]"
    redacted("Password") shouldBe "[REDACTED]"
    redacted("api_key") shouldBe "[REDACTED]"
  }

  it should "redact strings containing sensitive patterns" in {
    val text = "connecting with password=secret123 to host"

    val redacted = SecretRedactor.redactString(text)

    redacted should include ("[REDACTED]")
    redacted should not include ("secret123")
  }

  "TlsConfig" should "validate keystore path exists" in {
    val config = TlsConfig(
      keyStorePath = Some("/nonexistent/keystore.jks"),
      keyStorePassword = Some("password")
    )

    val errors = config.validate()

    errors should not be empty
    errors.exists(_.contains("keystore")) shouldBe true
  }

  it should "require password when keystore specified" in {
    val config = TlsConfig(
      keyStorePath = Some("/tmp/keystore.jks"),
      keyStorePassword = None
    )

    val errors = config.validate()

    errors.exists(_.contains("password")) shouldBe true
  }

  it should "validate truststore similarly" in {
    val config = TlsConfig(
      trustStorePath = Some("/tmp/truststore.jks"),
      trustStorePassword = None
    )

    val errors = config.validate()

    errors.exists(_.contains("password")) shouldBe true
  }

  it should "accept valid cipher suites" in {
    val config = TlsConfig(
      cipherSuite = Some("TLS_RSA_WITH_AES_128_CBC_SHA256")
    )

    config.cipherSuite shouldBe defined
  }

  "SecureConfigLoader" should "mask sensitive values in toString" in {
    val loader = SecureConfigLoader(Map(
      "queueManager" -> "QM1",
      "password" -> "secret"
    ))

    val str = loader.toString

    str should not include ("secret")
    str should include ("REDACTED")
  }

  it should "provide safe access to sensitive values" in {
    val loader = SecureConfigLoader(Map(
      "password" -> "mysecret"
    ))

    loader.getSensitive("password") shouldBe Some("mysecret")
  }

  it should "return None for missing values" in {
    val loader = SecureConfigLoader(Map.empty)

    loader.getSensitive("password") shouldBe None
    loader.get("missing") shouldBe None
  }

  "LeastPrivilegeGuidance" should "provide connection permission guidance" in {
    val guidance = LeastPrivilegeGuidance.forQueueConsumer("APP.QUEUE")

    guidance should include ("GET")
    guidance should include ("APP.QUEUE")
  }

  it should "provide guidance for producer" in {
    val guidance = LeastPrivilegeGuidance.forQueueProducer("APP.QUEUE")

    guidance should include ("PUT")
  }

  it should "provide channel access guidance" in {
    val guidance = LeastPrivilegeGuidance.forChannelAccess("DEV.APP.SVRCONN")

    guidance should include ("DEV.APP.SVRCONN")
  }
}
