package com.ibm.mq.spark.source.batch

import com.ibm.mq.spark.core.client.{ClientStats, DefaultMQClient, MQTransport}
import com.ibm.mq.spark.core.connection.MQConnectionConfig
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.source.MQSourceOptions
import com.ibm.mq.spark.source.schema.MQSchemaProvider
import org.apache.spark.sql.connector.read.InputPartition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class MQBatchReaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var mockTransport: MockBatchTransport = _

  override def beforeEach(): Unit = {
    mockTransport = new MockBatchTransport()
  }

  "MQScanBuilder" should "create a scan with proper options" in {
    val options = createOptions()
    val builder = MQScanBuilder(options, MQSchemaProvider.canonicalSchema)

    builder should not be null
    val scan = builder.build()
    scan should not be null
  }

  "MQBatchScan" should "plan partitions based on configuration" in {
    val options = createOptions()
    val scan = MQBatchScan(options, MQSchemaProvider.canonicalSchema)

    val partitions = scan.planInputPartitions()

    partitions should not be empty
    partitions.length shouldBe 1
  }

  it should "create partition reader factory" in {
    val options = createOptions()
    val scan = MQBatchScan(options, MQSchemaProvider.canonicalSchema)

    val factory = scan.createReaderFactory()

    factory should not be null
  }

  "MQPartitionReader" should "read messages and convert to rows" in {
    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2")
    )

    val partition = MQInputPartition(0, createOptions())
    val reader = createReader(partition)

    reader.next() shouldBe true
    val row1 = reader.get()
    row1 should not be null

    reader.next() shouldBe true
    val row2 = reader.get()
    row2 should not be null

    reader.next() shouldBe false
    reader.close()
  }

  it should "handle empty queue" in {
    val partition = MQInputPartition(0, createOptions())
    val reader = createReader(partition)

    reader.next() shouldBe false
    reader.close()
  }

  it should "respect batch size limit" in {
    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2"),
      createMessage("aabb03", "payload3"),
      createMessage("aabb04", "payload4"),
      createMessage("aabb05", "payload5")
    )

    val options = createOptions(batchSize = 3)
    val partition = MQInputPartition(0, options)
    val reader = createReader(partition)

    var count = 0
    while (reader.next()) {
      reader.get()
      count += 1
    }

    count shouldBe 3
    reader.close()
  }

  it should "convert messages to proper schema" in {
    val msg = createMessage("aabb01", "test payload")
    mockTransport.enqueueMessages(msg)

    val partition = MQInputPartition(0, createOptions())
    val reader = createReader(partition)

    reader.next() shouldBe true
    val row = reader.get()

    row.numFields shouldBe MQSchemaProvider.canonicalSchema.length
    reader.close()
  }

  "MQInputPartition" should "be serializable" in {
    val partition = MQInputPartition(0, createOptions())

    import java.io._
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(partition)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[MQInputPartition]

    deserialized.partitionId shouldBe 0
    deserialized.options.queueName shouldBe "DEV.QUEUE.1"
  }

  "MQPartitionReader transaction boundary" should "not commit until all messages are read" in {
    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2")
    )

    val partition = MQInputPartition(0, createOptions())
    val reader = createReader(partition)

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.next() shouldBe false
    mockTransport.commitCount shouldBe 1

    reader.close()
    mockTransport.commitCount shouldBe 1
    mockTransport.rollbackCount shouldBe 0
  }

  it should "rollback on close if reading did not complete" in {
    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2"),
      createMessage("aabb03", "payload3")
    )

    val options = createOptions(batchSize = 10)
    val partition = MQInputPartition(0, options)
    val reader = createReader(partition)

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.close()
    mockTransport.commitCount shouldBe 0
    mockTransport.rollbackCount shouldBe 1
  }

  it should "commit after reaching batch size limit" in {
    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2"),
      createMessage("aabb03", "payload3"),
      createMessage("aabb04", "payload4"),
      createMessage("aabb05", "payload5")
    )

    val options = createOptions(batchSize = 3)
    val partition = MQInputPartition(0, options)
    val reader = createReader(partition)

    var count = 0
    while (reader.next()) {
      count += 1
    }

    count shouldBe 3
    mockTransport.commitCount shouldBe 1

    reader.close()
    mockTransport.commitCount shouldBe 1
    mockTransport.rollbackCount shouldBe 0
  }

  it should "commit after exhausting queue before batch limit" in {
    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2")
    )

    val options = createOptions(batchSize = 10)
    val partition = MQInputPartition(0, options)
    val reader = createReader(partition)

    var count = 0
    while (reader.next()) {
      count += 1
    }

    count shouldBe 2
    mockTransport.commitCount shouldBe 1

    reader.close()
    mockTransport.rollbackCount shouldBe 0
  }

  it should "not commit if no messages were read" in {
    val partition = MQInputPartition(0, createOptions())
    val reader = createReader(partition)

    reader.next() shouldBe false
    mockTransport.commitCount shouldBe 1

    reader.close()
    mockTransport.rollbackCount shouldBe 0
  }

  it should "save checkpoint after successful commit when checkpoint store provided" in {
    import com.ibm.mq.spark.core.checkpoint.{ConnectorCheckpoint, InMemoryCheckpointStore}

    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2")
    )

    val checkpointStore = new InMemoryCheckpointStore()
    val partition = MQInputPartition(0, createOptions())
    val reader = new MQPartitionReader(
      partition,
      MQSchemaProvider.canonicalSchema,
      () => new DefaultMQClient(mockTransport),
      Some(checkpointStore)
    )

    while (reader.next()) {}
    reader.close()

    val checkpoint = checkpointStore.load("DEV.QUEUE.1", 0).get
    checkpoint shouldBe defined
    checkpoint.get.messagesProcessed shouldBe 2
    checkpoint.get.lastMessageId shouldBe defined
  }

  it should "not save checkpoint when rollback happens" in {
    import com.ibm.mq.spark.core.checkpoint.InMemoryCheckpointStore

    mockTransport.enqueueMessages(
      createMessage("aabb01", "payload1"),
      createMessage("aabb02", "payload2"),
      createMessage("aabb03", "payload3")
    )

    val checkpointStore = new InMemoryCheckpointStore()
    val options = createOptions(batchSize = 10)
    val partition = MQInputPartition(0, options)
    val reader = new MQPartitionReader(
      partition,
      MQSchemaProvider.canonicalSchema,
      () => new DefaultMQClient(mockTransport),
      Some(checkpointStore)
    )

    reader.next() shouldBe true
    reader.next() shouldBe true
    reader.close()

    val checkpoint = checkpointStore.load("DEV.QUEUE.1", 0).get
    checkpoint shouldBe empty
    mockTransport.rollbackCount shouldBe 1
  }

  private def createOptions(batchSize: Int = 1000): MQSourceOptions = {
    MQSourceOptions(
      queueManager = "QM1",
      channel = "DEV.APP.SVRCONN",
      connectionName = "localhost(1414)",
      queueName = "DEV.QUEUE.1",
      batchSize = batchSize,
      pollTimeoutMs = 1000
    )
  }

  private def createReader(partition: MQInputPartition): MQPartitionReader = {
    new MQPartitionReader(
      partition,
      MQSchemaProvider.canonicalSchema,
      () => new DefaultMQClient(mockTransport)
    )
  }

  private def createMessage(msgId: String, payload: String): RawMQMessage = {
    RawMQMessage(
      messageId = hexStringToBytes(msgId),
      correlationId = Array.emptyByteArray,
      payload = payload.getBytes("UTF-8"),
      putTimestamp = System.currentTimeMillis(),
      queueName = "DEV.QUEUE.1",
      ccsid = 1208,
      encoding = 546,
      priority = 4,
      expiry = -1,
      backoutCount = 0,
      format = "MQSTR"
    )
  }

  private def hexStringToBytes(hex: String): Array[Byte] = {
    hex.grouped(2).map(Integer.parseInt(_, 16).toByte).toArray
  }
}

class MockBatchTransport extends MQTransport {
  private var messages: List[RawMQMessage] = Nil
  private var connected: Boolean = false
  private var _commitCount: Int = 0
  private var _rollbackCount: Int = 0

  def commitCount: Int = _commitCount
  def rollbackCount: Int = _rollbackCount

  def enqueueMessages(msgs: RawMQMessage*): Unit = {
    messages = messages ++ msgs.toList
  }

  def reset(): Unit = {
    messages = Nil
    _commitCount = 0
    _rollbackCount = 0
  }

  override def connect(config: MQConnectionConfig): Unit = {
    connected = true
  }

  override def disconnect(): Unit = {
    connected = false
  }

  override def isConnected: Boolean = connected

  override def receive(waitMillis: Long): Option[RawMQMessage] = {
    messages match {
      case head :: tail =>
        messages = tail
        Some(head)
      case Nil => None
    }
  }

  override def commit(): Unit = {
    _commitCount += 1
  }

  override def rollback(): Unit = {
    _rollbackCount += 1
  }
}
