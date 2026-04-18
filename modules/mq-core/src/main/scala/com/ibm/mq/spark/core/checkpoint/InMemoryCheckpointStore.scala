package com.ibm.mq.spark.core.checkpoint

import com.ibm.mq.spark.core.interfaces.CheckpointStore

import scala.collection.concurrent.TrieMap
import scala.util.{Success, Try}

/**
 * In-memory checkpoint store for testing and development.
 *
 * WARNING: Checkpoints are lost on restart.
 * Use only for testing or ephemeral workloads.
 */
case class InMemoryCheckpointStore() extends CheckpointStore {

  private val checkpoints = TrieMap.empty[(String, Int), ConnectorCheckpoint]

  override def save(checkpoint: ConnectorCheckpoint): Try[Unit] = {
    checkpoints.put((checkpoint.queueName, checkpoint.partitionId), checkpoint)
    Success(())
  }

  override def load(queueName: String, partitionId: Int): Try[Option[ConnectorCheckpoint]] = {
    Success(checkpoints.get((queueName, partitionId)))
  }

  override def delete(queueName: String, partitionId: Int): Try[Unit] = {
    checkpoints.remove((queueName, partitionId))
    Success(())
  }

  override def list(): Try[Seq[ConnectorCheckpoint]] = {
    Success(checkpoints.values.toSeq)
  }

  def clear(): Unit = {
    checkpoints.clear()
  }

  def size: Int = checkpoints.size
}
