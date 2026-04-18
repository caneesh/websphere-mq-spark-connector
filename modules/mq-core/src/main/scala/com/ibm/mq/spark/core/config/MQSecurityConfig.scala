package com.ibm.mq.spark.core.config

case class MQSecurityConfig(
    sslCipherSuite: Option[String] = None,
    sslKeyStorePath: Option[String] = None,
    sslKeyStorePassword: Option[String] = None,
    sslTrustStorePath: Option[String] = None,
    sslTrustStorePassword: Option[String] = None,
    sslKeyStoreType: String = "JKS",
    sslTrustStoreType: String = "JKS",
    ccdtFilePath: Option[String] = None,
    user: Option[String] = None,
    password: Option[String] = None
) {

  def isTlsEnabled: Boolean = sslCipherSuite.isDefined

  def usesCcdt: Boolean = ccdtFilePath.isDefined

  def validate(): List[String] = {
    val errors = List.newBuilder[String]

    if (sslKeyStorePath.isDefined && sslKeyStorePassword.isEmpty) {
      errors += "sslKeyStorePassword is required when sslKeyStorePath is set"
    }

    if (sslTrustStorePath.isDefined && sslTrustStorePassword.isEmpty) {
      errors += "sslTrustStorePassword is required when sslTrustStorePath is set"
    }

    errors.result()
  }

  override def toString: String = {
    val sb = new StringBuilder("MQSecurityConfig(")
    sb.append(s"sslCipherSuite=$sslCipherSuite, ")
    sb.append(s"sslKeyStorePath=$sslKeyStorePath, ")
    sb.append(s"sslKeyStorePassword=${redactOption(sslKeyStorePassword)}, ")
    sb.append(s"sslTrustStorePath=$sslTrustStorePath, ")
    sb.append(s"sslTrustStorePassword=${redactOption(sslTrustStorePassword)}, ")
    sb.append(s"sslKeyStoreType=$sslKeyStoreType, ")
    sb.append(s"sslTrustStoreType=$sslTrustStoreType, ")
    sb.append(s"ccdtFilePath=$ccdtFilePath, ")
    sb.append(s"user=$user, ")
    sb.append(s"password=${redactOption(password)}")
    sb.append(")")
    sb.toString()
  }

  private def redactOption(opt: Option[String]): String = {
    opt.map(_ => "[REDACTED]").getOrElse("None")
  }
}
