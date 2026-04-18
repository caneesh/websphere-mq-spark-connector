package com.ibm.mq.spark.core.checkpoint

import com.ibm.mq.spark.core.interfaces.CheckpointStore

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
 * File-based checkpoint store implementation.
 *
 * Stores checkpoints as JSON files in a directory structure.
 * Each checkpoint is stored in: {baseDir}/{queueName}/{partitionId}.json
 *
 * RESTART SEMANTICS:
 * - At-least-once delivery: messages may be reprocessed after restart
 * - Checkpoint saved after commit, not before
 * - If crash occurs between MQ commit and checkpoint save, messages are reprocessed
 * - For exactly-once, use a transactional backend (DB with 2PC)
 *
 * @param baseDir the base directory for checkpoint files
 */
case class FileCheckpointStore(baseDir: String) extends CheckpointStore {

  private val basePath = Paths.get(baseDir)

  if (!Files.exists(basePath)) {
    Files.createDirectories(basePath)
  }

  override def save(checkpoint: ConnectorCheckpoint): Try[Unit] = {
    Try {
      val queueDir = basePath.resolve(sanitizeName(checkpoint.queueName))
      if (!Files.exists(queueDir)) {
        Files.createDirectories(queueDir)
      }

      val checkpointFile = queueDir.resolve(s"${checkpoint.partitionId}.json")
      val writer = new PrintWriter(checkpointFile.toFile)
      try {
        writer.write(checkpoint.toJson)
      } finally {
        writer.close()
      }
    }
  }

  override def load(queueName: String, partitionId: Int): Try[Option[ConnectorCheckpoint]] = {
    Try {
      val checkpointFile = basePath
        .resolve(sanitizeName(queueName))
        .resolve(s"$partitionId.json")

      if (Files.exists(checkpointFile)) {
        val source = Source.fromFile(checkpointFile.toFile)
        try {
          val json = source.mkString
          Some(ConnectorCheckpoint.fromJson(json))
        } finally {
          source.close()
        }
      } else {
        None
      }
    }
  }

  override def delete(queueName: String, partitionId: Int): Try[Unit] = {
    Try {
      val checkpointFile = basePath
        .resolve(sanitizeName(queueName))
        .resolve(s"$partitionId.json")

      if (Files.exists(checkpointFile)) {
        Files.delete(checkpointFile)
      }
    }
  }

  override def list(): Try[Seq[ConnectorCheckpoint]] = {
    Try {
      val checkpoints = Seq.newBuilder[ConnectorCheckpoint]

      if (Files.exists(basePath)) {
        Files.list(basePath).forEach { queueDir =>
          if (Files.isDirectory(queueDir)) {
            Files.list(queueDir).forEach { checkpointFile =>
              if (checkpointFile.toString.endsWith(".json")) {
                val source = Source.fromFile(checkpointFile.toFile)
                try {
                  val json = source.mkString
                  checkpoints += ConnectorCheckpoint.fromJson(json)
                } finally {
                  source.close()
                }
              }
            }
          }
        }
      }

      checkpoints.result()
    }
  }

  private def sanitizeName(name: String): String = {
    name.replace("/", "_").replace("\\", "_").replace(":", "_")
  }
}
