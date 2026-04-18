package com.ibm.mq.spark.core.security

/**
 * Secure configuration loader that handles sensitive values safely.
 *
 * - Redacts sensitive values in toString/logs
 * - Provides explicit access for sensitive values
 * - Supports environment variable substitution
 */
case class SecureConfigLoader(config: Map[String, String]) {

  private val envPattern = """\$\{env:([^}]+)\}""".r

  /**
   * Gets a non-sensitive configuration value.
   */
  def get(key: String): Option[String] = {
    config.get(key).map(resolveValue)
  }

  /**
   * Gets a sensitive configuration value (passwords, tokens, etc.).
   * Use sparingly and never log the result.
   */
  def getSensitive(key: String): Option[String] = {
    config.get(key).map(resolveValue)
  }

  /**
   * Gets a required value, throwing if missing.
   */
  def require(key: String): String = {
    get(key).getOrElse(throw new IllegalArgumentException(s"Required config '$key' is missing"))
  }

  /**
   * Gets a value with a default.
   */
  def getOrElse(key: String, default: String): String = {
    get(key).getOrElse(default)
  }

  /**
   * Gets a boolean value.
   */
  def getBoolean(key: String, default: Boolean = false): Boolean = {
    get(key).map(_.toLowerCase).map {
      case "true" | "yes" | "1" => true
      case _ => false
    }.getOrElse(default)
  }

  /**
   * Gets an integer value.
   */
  def getInt(key: String, default: Int = 0): Int = {
    get(key).flatMap(s => scala.util.Try(s.toInt).toOption).getOrElse(default)
  }

  /**
   * Gets a long value.
   */
  def getLong(key: String, default: Long = 0L): Long = {
    get(key).flatMap(s => scala.util.Try(s.toLong).toOption).getOrElse(default)
  }

  /**
   * Returns all keys in the config.
   */
  def keys: Set[String] = config.keySet

  /**
   * Returns a redacted view of the configuration for logging.
   */
  def redactedConfig: Map[String, String] = {
    SecretRedactor.redactMap(config)
  }

  /**
   * Safe toString that redacts sensitive values.
   */
  override def toString: String = {
    val redacted = redactedConfig
    s"SecureConfigLoader(${redacted.map { case (k, v) => s"$k=$v" }.mkString(", ")})"
  }

  private def resolveValue(value: String): String = {
    envPattern.replaceAllIn(value, m => {
      val envVar = m.group(1)
      sys.env.getOrElse(envVar, s"$${env:$envVar}")
    })
  }
}

object SecureConfigLoader {

  /**
   * Creates a loader from Spark options.
   */
  def fromSparkOptions(options: java.util.Map[String, String]): SecureConfigLoader = {
    import scala.collection.JavaConverters._
    SecureConfigLoader(options.asScala.toMap)
  }

  /**
   * Creates a loader from system properties with a prefix.
   */
  def fromSystemProperties(prefix: String): SecureConfigLoader = {
    val props = System.getProperties
    val filtered = props.stringPropertyNames().toArray.collect {
      case key: String if key.startsWith(prefix) =>
        key.substring(prefix.length) -> props.getProperty(key)
    }.toMap
    SecureConfigLoader(filtered)
  }
}
