package com.ibm.mq.spark.core.security

import java.io.File

/**
 * TLS/SSL configuration for secure MQ connections.
 *
 * @param keyStorePath path to Java keystore file
 * @param keyStorePassword password for keystore
 * @param keyStoreType keystore type (JKS, PKCS12)
 * @param trustStorePath path to truststore file
 * @param trustStorePassword password for truststore
 * @param trustStoreType truststore type
 * @param cipherSuite specific cipher suite to use
 * @param sslProtocol SSL/TLS protocol version
 */
case class TlsConfig(
    keyStorePath: Option[String] = None,
    keyStorePassword: Option[String] = None,
    keyStoreType: String = "JKS",
    trustStorePath: Option[String] = None,
    trustStorePassword: Option[String] = None,
    trustStoreType: String = "JKS",
    cipherSuite: Option[String] = None,
    sslProtocol: String = "TLSv1.2"
) {

  /**
   * Validates the TLS configuration.
   *
   * @return list of validation errors, empty if valid
   */
  def validate(): List[String] = {
    val errors = List.newBuilder[String]

    keyStorePath.foreach { path =>
      if (!new File(path).exists()) {
        errors += s"Keystore file not found: $path"
      }
      if (keyStorePassword.isEmpty) {
        errors += "Keystore password is required when keystore path is specified"
      }
    }

    trustStorePath.foreach { path =>
      if (!new File(path).exists()) {
        errors += s"Truststore file not found: $path"
      }
      if (trustStorePassword.isEmpty) {
        errors += "Truststore password is required when truststore path is specified"
      }
    }

    errors.result()
  }

  /**
   * Indicates whether TLS is enabled.
   */
  def isEnabled: Boolean = {
    cipherSuite.isDefined || keyStorePath.isDefined || trustStorePath.isDefined
  }

  /**
   * Returns a string representation with sensitive values redacted.
   */
  override def toString: String = {
    val ks = keyStorePath.getOrElse("none")
    val ts = trustStorePath.getOrElse("none")
    val cipher = cipherSuite.getOrElse("default")
    s"TlsConfig(keyStore=$ks, trustStore=$ts, cipher=$cipher, protocol=$sslProtocol)"
  }
}

object TlsConfig {

  /**
   * Creates TLS config from properties map.
   */
  def fromMap(props: Map[String, String]): TlsConfig = {
    TlsConfig(
      keyStorePath = props.get("sslKeyStore"),
      keyStorePassword = props.get("sslKeyStorePassword"),
      keyStoreType = props.getOrElse("sslKeyStoreType", "JKS"),
      trustStorePath = props.get("sslTrustStore"),
      trustStorePassword = props.get("sslTrustStorePassword"),
      trustStoreType = props.getOrElse("sslTrustStoreType", "JKS"),
      cipherSuite = props.get("sslCipherSuite"),
      sslProtocol = props.getOrElse("sslProtocol", "TLSv1.2")
    )
  }

  val Disabled: TlsConfig = TlsConfig()
}
