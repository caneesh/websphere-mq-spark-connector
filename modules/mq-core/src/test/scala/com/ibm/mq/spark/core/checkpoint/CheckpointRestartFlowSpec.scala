package com.ibm.mq.spark.core.checkpoint

import com.ibm.mq.spark.core.interfaces.CheckpointStore
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.io.File
import java.nio.file.{Files, Path}

/**
 * Tests for checkpoint restart flow behavior.
 *
 * These tests verify that:
 * 1. Checkpoints can be persisted and reloaded across process restarts
 * 2. Processing can resume from checkpoint state
 * 3. Partial processing before checkpoint is handled correctly
 */
class CheckpointRestartFlowSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var tempDir: Path = _
  private var store: CheckpointStore = _

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("mq-checkpoint-restart-test")
    store = FileCheckpointStore(tempDir.toString)
  }

  override def afterEach(): Unit = {
    deleteRecursively(tempDir.toFile)
  }

  "Checkpoint restart flow" should "persist and load checkpoint state across store instances" in {
    val checkpoint = createCheckpoint(
      queueName = "RESTART.QUEUE",
      partitionId = 0,
      messagesProcessed = 1000,
      bytesProcessed = 50000,
      lastMessageId = Some("aabb0001")
    )

    store.save(checkpoint).isSuccess shouldBe true

    val newStore = FileCheckpointStore(tempDir.toString)
    val loaded = newStore.load("RESTART.QUEUE", 0).get

    loaded shouldBe defined
    loaded.get.messagesProcessed shouldBe 1000
    loaded.get.bytesProcessed shouldBe 50000
    loaded.get.lastMessageId shouldBe Some("aabb0001")
  }

  it should "allow resuming from last checkpoint after simulated restart" in {
    var totalProcessed = 0L

    store.save(createCheckpoint(
      queueName = "RESUME.QUEUE",
      partitionId = 0,
      messagesProcessed = 500
    ))

    val restartStore = FileCheckpointStore(tempDir.toString)
    val checkpoint = restartStore.load("RESUME.QUEUE", 0).get

    checkpoint shouldBe defined
    totalProcessed = checkpoint.get.messagesProcessed

    totalProcessed += 500
    restartStore.save(createCheckpoint(
      queueName = "RESUME.QUEUE",
      partitionId = 0,
      messagesProcessed = totalProcessed
    ))

    val finalStore = FileCheckpointStore(tempDir.toString)
    val finalCheckpoint = finalStore.load("RESUME.QUEUE", 0).get

    finalCheckpoint.get.messagesProcessed shouldBe 1000
  }

  it should "return None for fresh start with no previous checkpoint" in {
    val loaded = store.load("NEW.QUEUE", 0).get

    loaded shouldBe None
  }

  it should "maintain checkpoint for each partition independently" in {
    store.save(createCheckpoint("MULTI.QUEUE", 0, messagesProcessed = 100))
    store.save(createCheckpoint("MULTI.QUEUE", 1, messagesProcessed = 200))
    store.save(createCheckpoint("MULTI.QUEUE", 2, messagesProcessed = 300))

    val restartStore = FileCheckpointStore(tempDir.toString)

    restartStore.load("MULTI.QUEUE", 0).get.get.messagesProcessed shouldBe 100
    restartStore.load("MULTI.QUEUE", 1).get.get.messagesProcessed shouldBe 200
    restartStore.load("MULTI.QUEUE", 2).get.get.messagesProcessed shouldBe 300
  }

  it should "overwrite checkpoint on subsequent saves" in {
    store.save(createCheckpoint("UPDATE.QUEUE", 0, messagesProcessed = 100))
    store.save(createCheckpoint("UPDATE.QUEUE", 0, messagesProcessed = 200))
    store.save(createCheckpoint("UPDATE.QUEUE", 0, messagesProcessed = 300))

    val restartStore = FileCheckpointStore(tempDir.toString)
    val loaded = restartStore.load("UPDATE.QUEUE", 0).get

    loaded.get.messagesProcessed shouldBe 300
  }

  it should "preserve lastMessageId for duplicate detection" in {
    store.save(createCheckpoint(
      queueName = "DEDUP.QUEUE",
      partitionId = 0,
      messagesProcessed = 100,
      lastMessageId = Some("msg-uuid-12345")
    ))

    val restartStore = FileCheckpointStore(tempDir.toString)
    val checkpoint = restartStore.load("DEDUP.QUEUE", 0).get

    checkpoint.get.lastMessageId shouldBe Some("msg-uuid-12345")
  }

  it should "preserve correlationId for request-reply tracking" in {
    store.save(createCheckpoint(
      queueName = "CORREL.QUEUE",
      partitionId = 0,
      messagesProcessed = 50,
      lastCorrelationId = Some("correl-abc-789")
    ))

    val restartStore = FileCheckpointStore(tempDir.toString)
    val checkpoint = restartStore.load("CORREL.QUEUE", 0).get

    checkpoint.get.lastCorrelationId shouldBe Some("correl-abc-789")
  }

  "InMemoryCheckpointStore" should "provide same restart semantics for testing" in {
    val memStore = InMemoryCheckpointStore()

    memStore.save(createCheckpoint("MEM.QUEUE", 0, messagesProcessed = 100))

    val loaded = memStore.load("MEM.QUEUE", 0).get
    loaded shouldBe defined
    loaded.get.messagesProcessed shouldBe 100

    memStore.save(createCheckpoint("MEM.QUEUE", 0, messagesProcessed = 200))

    val updated = memStore.load("MEM.QUEUE", 0).get
    updated.get.messagesProcessed shouldBe 200
  }

  "Checkpoint serialization" should "preserve all fields through JSON round-trip" in {
    val original = createCheckpoint(
      queueName = "SERIAL.QUEUE",
      partitionId = 3,
      messagesProcessed = 12345,
      bytesProcessed = 987654,
      lastMessageId = Some("msg-id-hex"),
      lastCorrelationId = Some("correl-id-hex")
    )

    store.save(original)

    val restartStore = FileCheckpointStore(tempDir.toString)
    val loaded = restartStore.load("SERIAL.QUEUE", 3).get.get

    loaded.queueName shouldBe "SERIAL.QUEUE"
    loaded.queueManager shouldBe "QM1"
    loaded.partitionId shouldBe 3
    loaded.messagesProcessed shouldBe 12345
    loaded.bytesProcessed shouldBe 987654
    loaded.lastMessageId shouldBe Some("msg-id-hex")
    loaded.lastCorrelationId shouldBe Some("correl-id-hex")
  }

  "Restart after failure scenarios" should "handle checkpoint saved then process crash (replay expected)" in {
    store.save(createCheckpoint("FAIL.QUEUE", 0, messagesProcessed = 100))

    val restartStore = FileCheckpointStore(tempDir.toString)
    val checkpoint = restartStore.load("FAIL.QUEUE", 0).get

    checkpoint.get.messagesProcessed shouldBe 100
  }

  it should "handle no checkpoint when process crashes before save (full replay)" in {
    val restartStore = FileCheckpointStore(tempDir.toString)
    val checkpoint = restartStore.load("CRASH.QUEUE", 0).get

    checkpoint shouldBe None
  }

  private def createCheckpoint(
      queueName: String,
      partitionId: Int,
      messagesProcessed: Long = 0,
      bytesProcessed: Long = 0,
      lastMessageId: Option[String] = None,
      lastCorrelationId: Option[String] = None
  ): ConnectorCheckpoint = {
    ConnectorCheckpoint(
      queueName = queueName,
      queueManager = "QM1",
      lastMessageId = lastMessageId,
      lastCorrelationId = lastCorrelationId,
      lastProcessedTimestamp = System.currentTimeMillis(),
      messagesProcessed = messagesProcessed,
      bytesProcessed = bytesProcessed,
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
