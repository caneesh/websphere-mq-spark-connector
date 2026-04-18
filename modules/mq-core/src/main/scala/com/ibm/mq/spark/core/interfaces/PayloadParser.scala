package com.ibm.mq.spark.core.interfaces

import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.core.parser.{ParsedPayloadResult, PayloadFormat}

trait PayloadParser {

  def parse(message: RawMQMessage): ParsedPayloadResult

  def canParse(message: RawMQMessage): Boolean

  def format: PayloadFormat
}
