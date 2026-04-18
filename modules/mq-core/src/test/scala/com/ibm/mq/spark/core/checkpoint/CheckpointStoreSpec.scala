package com.ibm.mq.spark.core.checkpoint

import com.ibm.mq.spark.core.interfaces.CheckpointStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.{Files, Path}

class CheckpointStoreSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var tempDir: Path = _
  private var store: CheckpointStore = _

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("mq-checkpoint-test")
    store = FileCheckpointStore(tempDir.toString)
  }

  override def afterEach(): Unit = {
    deleteRecursively(tempDir.toFile)
  }

  "FileCheckpointStore" should "save and load checkpoint" in {
    val checkpoint = createCheckpoint("TEST.QUEUE", 0)

    store.save(checkpoint).isSuccess shouldBe true

    val loaded = store.load("TEST.QUEUE", 0)
    loaded.isSuccess shouldBe true
    loaded.get shouldBe defined
    loaded.get.get.queueName shouldBe "TEST.QUEUE"
  }

  it should "return None for non-existent checkpoint" in {
    val loaded = store.load("NONEXISTENT.QUEUE", 0)

    loaded.isSuccess shouldBe true
    loaded.get shouldBe None
  }

  it should "update existing checkpoint" in {
    val initial = createCheckpoint("TEST.QUEUE", 0, messagesProcessed = 100)
    val updated = createCheckpoint("TEST.QUEUE", 0, messagesProcessed = 200)

    store.save(initial)
    store.save(updated)

    val loaded = store.load("TEST.QUEUE", 0)
    loaded.get.get.messagesProcessed shouldBe 200
  }

  it should "handle multiple partitions" in {
    val cp1 = createCheckpoint("TEST.QUEUE", 0)
    val cp2 = createCheckpoint("TEST.QUEUE", 1)

    store.save(cp1)
    store.save(cp2)

    store.load("TEST.QUEUE", 0).get.get.partitionId shouldBe 0
    store.load("TEST.QUEUE", 1).get.get.partitionId shouldBe 1
  }

  it should "handle multiple queues" in {
    val cp1 = createCheckpoint("QUEUE.A", 0)
    val cp2 = createCheckpoint("QUEUE.B", 0)

    store.save(cp1)
    store.save(cp2)

    store.load("QUEUE.A", 0).get.get.queueName shouldBe "QUEUE.A"
    store.load("QUEUE.B", 0).get.get.queueName shouldBe "QUEUE.B"
  }

  it should "delete checkpoint" in {
    val checkpoint = createCheckpoint("TEST.QUEUE", 0)
    store.save(checkpoint)

    store.delete("TEST.QUEUE", 0).isSuccess shouldBe true
    store.load("TEST.QUEUE", 0).get shouldBe None
  }

  it should "list all checkpoints" in {
    store.save(createCheckpoint("QUEUE.A", 0))
    store.save(createCheckpoint("QUEUE.A", 1))
    store.save(createCheckpoint("QUEUE.B", 0))

    val all = store.list()

    all.isSuccess shouldBe true
    all.get should have size 3
  }

  it should "persist checkpoint across store instances" in {
    val checkpoint = createCheckpoint("PERSIST.QUEUE", 0)
    store.save(checkpoint)

    val newStore = FileCheckpointStore(tempDir.toString)
    val loaded = newStore.load("PERSIST.QUEUE", 0)

    loaded.get shouldBe defined
    loaded.get.get.queueName shouldBe "PERSIST.QUEUE"
  }

  it should "handle special characters in queue names" in {
    val checkpoint = createCheckpoint("MY.QUEUE.WITH/SPECIAL", 0)

    store.save(checkpoint).isSuccess shouldBe true
    store.load("MY.QUEUE.WITH/SPECIAL", 0).get shouldBe defined
  }

  "InMemoryCheckpointStore" should "work for testing" in {
    val memStore = InMemoryCheckpointStore()

    memStore.save(createCheckpoint("TEST.QUEUE", 0))
    memStore.load("TEST.QUEUE", 0).get shouldBe defined
  }

  "CheckpointStore" should "be extensible for other backends" in {
    val customStore: CheckpointStore = new CheckpointStore {
      override def save(checkpoint: ConnectorCheckpoint) = scala.util.Success(())
      override def load(queueName: String, partitionId: Int) = scala.util.Success(None)
      override def delete(queueName: String, partitionId: Int) = scala.util.Success(())
      override def list() = scala.util.Success(Seq.empty)
    }

    customStore shouldBe a[CheckpointStore]
  }

  private def createCheckpoint(
      queueName: String,
      partitionId: Int,
      messagesProcessed: Long = 50
  ): ConnectorCheckpoint = {
    ConnectorCheckpoint(
      queueName = queueName,
      queueManager = "QM1",
      lastMessageId = Some("aabb0001"),
      lastCorrelationId = None,
      lastProcessedTimestamp = System.currentTimeMillis(),
      messagesProcessed = messagesProcessed,
      bytesProcessed = 1024,
      partitionId = partitionId,
      createdAt = System.currentTimeMillis()
    )
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
