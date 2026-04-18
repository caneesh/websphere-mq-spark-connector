package com.ibm.mq.spark.core.config

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MQSecurityConfigSpec extends AnyFlatSpec with Matchers {

  "MQSecurityConfig" should "be created with TLS settings" in {
    val config = MQSecurityConfig(
      sslCipherSuite = Some("TLS_RSA_WITH_AES_256_CBC_SHA256"),
      sslKeyStorePath = Some("/path/to/keystore.jks"),
      sslKeyStorePassword = Some("keypass"),
      sslTrustStorePath = Some("/path/to/truststore.jks"),
      sslTrustStorePassword = Some("trustpass")
    )

    config.isTlsEnabled shouldBe true
    config.sslCipherSuite shouldBe Some("TLS_RSA_WITH_AES_256_CBC_SHA256")
  }

  it should "indicate TLS disabled when no cipher suite" in {
    val config = MQSecurityConfig()
    config.isTlsEnabled shouldBe false
  }

  it should "validate keystore requires password" in {
    val config = MQSecurityConfig(
      sslKeyStorePath = Some("/path/to/keystore.jks"),
      sslKeyStorePassword = None
    )

    val errors = config.validate()
    errors should contain("sslKeyStorePassword is required when sslKeyStorePath is set")
  }

  it should "validate truststore requires password" in {
    val config = MQSecurityConfig(
      sslTrustStorePath = Some("/path/to/truststore.jks"),
      sslTrustStorePassword = None
    )

    val errors = config.validate()
    errors should contain("sslTrustStorePassword is required when sslTrustStorePath is set")
  }

  it should "redact sensitive fields in toString" in {
    val config = MQSecurityConfig(
      sslKeyStorePassword = Some("secret123"),
      sslTrustStorePassword = Some("secret456"),
      user = Some("admin"),
      password = Some("adminpass")
    )

    val str = config.toString
    str should not include "secret123"
    str should not include "secret456"
    str should not include "adminpass"
    str should include ("[REDACTED]")
  }

  it should "support CCDT file path" in {
    val config = MQSecurityConfig(
      ccdtFilePath = Some("/path/to/ccdt.tab")
    )

    config.ccdtFilePath shouldBe Some("/path/to/ccdt.tab")
    config.usesCcdt shouldBe true
  }
}
