package com.ibm.mq.spark.core.interfaces

import com.ibm.mq.spark.core.checkpoint.ConnectorCheckpoint

import scala.util.Try

trait CheckpointStore {

  def save(checkpoint: ConnectorCheckpoint): Try[Unit]

  def load(queueName: String, partitionId: Int): Try[Option[ConnectorCheckpoint]]

  def delete(queueName: String, partitionId: Int): Try[Unit]

  def list(): Try[Seq[ConnectorCheckpoint]]
}
