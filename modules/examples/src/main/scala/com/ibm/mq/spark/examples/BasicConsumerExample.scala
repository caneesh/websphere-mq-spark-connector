package com.ibm.mq.spark.examples

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Basic example demonstrating IBM MQ message consumption with Spark.
 *
 * This example shows how to configure a Spark session and read
 * messages from an IBM MQ queue into a DataFrame.
 */
object BasicConsumerExample {

  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Starting BasicConsumerExample")

    val spark = SparkSession.builder()
      .appName("MQ Consumer Example")
      .master("local[*]")
      .getOrCreate()

    try {
      run(spark)
    } finally {
      spark.stop()
      logger.info("BasicConsumerExample completed")
    }
  }

  private def run(spark: SparkSession): Unit = {
    val df = spark.read
      .format("com.ibm.mq.spark.source.MQDataSource")
      .option("queueManager", sys.env.getOrElse("MQ_QUEUE_MANAGER", "QM1"))
      .option("channel", sys.env.getOrElse("MQ_CHANNEL", "DEV.APP.SVRCONN"))
      .option("connectionName", sys.env.getOrElse("MQ_CONNECTION", "localhost(1414)"))
      .option("queueName", sys.env.getOrElse("MQ_QUEUE", "DEV.QUEUE.1"))
      .load()

    logger.info(s"Schema: ${df.schema.treeString}")

    df.show(10, truncate = false)
  }
}
