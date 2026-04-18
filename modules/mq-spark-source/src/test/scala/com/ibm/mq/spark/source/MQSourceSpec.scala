package com.ibm.mq.spark.source

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MQSourceSpec extends AnyFlatSpec with Matchers {

  "MQDataSource" should "provide the correct short name" in {
    val dataSource = new MQDataSource()

    dataSource.shortName() shouldBe "ibm-mq"
  }

  it should "infer the default schema" in {
    val dataSource = new MQDataSource()

    val schema = dataSource.inferSchema(null)

    schema.fieldNames should contain("messageId")
    schema.fieldNames should contain("correlationId")
    schema.fieldNames should contain("payload")
    schema.fieldNames should contain("payloadString")
    schema.fieldNames should contain("timestamp")
  }

  it should "support external metadata" in {
    val dataSource = new MQDataSource()

    dataSource.supportsExternalMetadata() shouldBe true
  }

  "MQSourceOptions" should "parse required options from map" in {
    val options = Map(
      "queueManager" -> "QM1",
      "channel" -> "CHANNEL1",
      "connectionName" -> "localhost(1414)",
      "queueName" -> "QUEUE1"
    )

    val sourceOptions = MQSourceOptions.fromMap(options)

    sourceOptions.queueManager shouldBe "QM1"
    sourceOptions.channel shouldBe "CHANNEL1"
    sourceOptions.connectionName shouldBe "localhost(1414)"
    sourceOptions.queueName shouldBe "QUEUE1"
  }

  it should "parse optional options with defaults" in {
    val options = Map(
      "queueManager" -> "QM1",
      "channel" -> "CHANNEL1",
      "connectionName" -> "localhost(1414)",
      "queueName" -> "QUEUE1"
    )

    val sourceOptions = MQSourceOptions.fromMap(options)

    sourceOptions.batchSize shouldBe 1000
    sourceOptions.pollTimeoutMs shouldBe 5000L
    sourceOptions.user shouldBe None
    sourceOptions.password shouldBe None
  }

  it should "parse custom batch size and poll timeout" in {
    val options = Map(
      "queueManager" -> "QM1",
      "channel" -> "CHANNEL1",
      "connectionName" -> "localhost(1414)",
      "queueName" -> "QUEUE1",
      "batchSize" -> "500",
      "pollTimeoutMs" -> "10000"
    )

    val sourceOptions = MQSourceOptions.fromMap(options)

    sourceOptions.batchSize shouldBe 500
    sourceOptions.pollTimeoutMs shouldBe 10000L
  }

  it should "throw exception for missing required options" in {
    val options = Map(
      "queueManager" -> "QM1"
    )

    an[IllegalArgumentException] should be thrownBy {
      MQSourceOptions.fromMap(options)
    }
  }

  it should "convert to connector config" in {
    val sourceOptions = MQSourceOptions(
      queueManager = "QM1",
      channel = "CHANNEL1",
      connectionName = "localhost(1414)",
      queueName = "QUEUE1",
      user = Some("admin"),
      sslCipherSuite = Some("TLS_RSA_WITH_AES_256_CBC_SHA256")
    )

    val config = sourceOptions.toConnectorConfig

    config.queueManager shouldBe "QM1"
    config.channel shouldBe "CHANNEL1"
    config.connectionName shouldBe "localhost(1414)"
    config.queueName shouldBe "QUEUE1"
    config.user shouldBe Some("admin")
    config.sslCipherSuite shouldBe Some("TLS_RSA_WITH_AES_256_CBC_SHA256")
  }
}
