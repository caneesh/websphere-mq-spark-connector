package com.ibm.mq.spark.core.security

/**
 * Utility for redacting sensitive information from logs and configs.
 */
object SecretRedactor {

  private val sensitivePatterns = Seq(
    "password",
    "secret",
    "token",
    "apikey",
    "api_key",
    "credential",
    "private",
    "auth"
  )

  private val RedactedValue = "[REDACTED]"

  private val passwordValuePattern = """(password\s*[=:]\s*)([^\s,}]+)""".r

  /**
   * Redacts sensitive values in a configuration map.
   */
  def redactMap(config: Map[String, String]): Map[String, String] = {
    config.map { case (key, value) =>
      if (isSensitiveKey(key)) {
        key -> RedactedValue
      } else {
        key -> value
      }
    }
  }

  /**
   * Redacts sensitive patterns in a string.
   */
  def redactString(text: String): String = {
    var result = text
    result = passwordValuePattern.replaceAllIn(result, m => s"${m.group(1)}$RedactedValue")
    sensitivePatterns.foreach { pattern =>
      val regex = s"(?i)($pattern\\s*[=:]\\s*)([^\\s,}]+)".r
      result = regex.replaceAllIn(result, m => s"${m.group(1)}$RedactedValue")
    }
    result
  }

  /**
   * Checks if a key is sensitive based on naming patterns.
   */
  def isSensitiveKey(key: String): Boolean = {
    val lowerKey = key.toLowerCase
    sensitivePatterns.exists(pattern => lowerKey.contains(pattern))
  }

  /**
   * Masks a value for logging, showing only partial content.
   */
  def mask(value: String, showChars: Int = 2): String = {
    if (value == null || value.isEmpty) {
      RedactedValue
    } else if (value.length <= showChars * 2) {
      "*" * value.length
    } else {
      value.take(showChars) + "*" * (value.length - showChars * 2) + value.takeRight(showChars)
    }
  }
}
