package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.core.checkpoint.InMemoryCheckpointStore
import com.ibm.mq.spark.core.client.{DefaultMQClient, MQTransport}
import com.ibm.mq.spark.core.connection.MQConnectionConfig
import com.ibm.mq.spark.core.message.RawMQMessage
import com.ibm.mq.spark.source.MQSourceOptions
import com.ibm.mq.spark.source.schema.MQSchemaProvider
import org.apache.spark.sql.connector.read.streaming.{Offset, ReadLimit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class MQStreamingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var mockTransport: MockStreamTransport = _

  override def beforeEach(): Unit = {
    mockTransport = new MockStreamTransport()
  }

  "MQOffset" should "serialize to JSON with all fields" in {
    val offset = MQOffset(
      batchId = 5,
      processedCount = 500,
      lastBatchCount = 100,
      lastBatchTimestamp = 1704067200000L,
      lastMessageIds = Seq("msg1", "msg2")
    )

    val json = offset.json()

    json should include ("\"batchId\":5")
    json should include ("\"processedCount\":500")
    json should include ("\"lastBatchCount\":100")
    json should include ("\"lastBatchTimestamp\":1704067200000")
    json should include ("\"lastMessageIds\":[\"msg1\",\"msg2\"]")
  }

  it should "deserialize from JSON" in {
    val json = """{"batchId":5,"processedCount":500,"lastBatchCount":100,"lastBatchTimestamp":1000,"lastMessageIds":["a","b"]}"""

    val offset = MQOffset.fromJson(json)

    offset.batchId shouldBe 5
    offset.processedCount shouldBe 500
    offset.lastBatchCount shouldBe 100
    offset.lastBatchTimestamp shouldBe 1000L
    offset.lastMessageIds should contain allOf ("a", "b")
  }

  it should "deserialize legacy format for backwards compatibility" in {
    val json = """{"messageCount":100,"timestamp":1000}"""

    val offset = MQOffset.fromJson(json)

    offset.batchId shouldBe 0
    offset.processedCount shouldBe 0
  }

  it should "support comparison for ordering based on batchId" in {
    val offset1 = MQOffset(batchId = 10, processedCount = 100, lastBatchCount = 10, lastBatchTimestamp = 100L)
    val offset2 = MQOffset(batchId = 20, processedCount = 200, lastBatchCount = 10, lastBatchTimestamp = 200L)

    offset1 < offset2 shouldBe true
    offset2 > offset1 shouldBe true
  }

  it should "create next batch offset" in {
    val current = MQOffset(batchId = 5, processedCount = 500, lastBatchCount = 100, lastBatchTimestamp = 1000L)

    val next = current.nextBatch(50, Seq("newMsg1"))

    next.batchId shouldBe 6
    next.processedCount shouldBe 550
    next.lastBatchCount shouldBe 50
    next.lastMessageIds should contain ("newMsg1")
  }

  it should "limit stored message IDs to 10" in {
    val ids = (1 to 20).map(i => s"msg$i")
    val offset = MQOffset.Initial.nextBatch(20, ids)

    offset.lastMessageIds.size shouldBe 10
  }

  "MQOffset.Initial" should "represent starting state" in {
    MQOffset.Initial.batchId shouldBe 0
    MQOffset.Initial.processedCount shouldBe 0
    MQOffset.Initial.lastBatchCount shouldBe 0
    MQOffset.Initial.lastMessageIds shouldBe empty
  }

  "MQMicroBatchStream" should "return initial offset" in {
    val stream = createStream()

    val initial = stream.initialOffset()

    initial should not be null
    initial.asInstanceOf[MQOffset].batchId shouldBe 0
  }

  it should "deserialize offset from JSON" in {
    val stream = createStream()
    val json = """{"batchId":5,"processedCount":500,"lastBatchCount":100,"lastBatchTimestamp":1000,"lastMessageIds":[]}"""

    val offset = stream.deserializeOffset(json)

    offset.asInstanceOf[MQOffset].batchId shouldBe 5
    offset.asInstanceOf[MQOffset].processedCount shouldBe 500
  }

  it should "plan partitions for micro-batch" in {
    val stream = createStream()
    val start = MQOffset.Initial
    val end = MQOffset.Initial.nextBatch(100)

    val partitions = stream.planInputPartitions(start, end)

    partitions should have length 1
  }

  it should "report latest offset with incremented batchId" in {
    val stream = createStream()

    val offset1 = stream.latestOffset().asInstanceOf[MQOffset]
    val offset2 = stream.latestOffset().asInstanceOf[MQOffset]

    offset2.batchId should be > offset1.batchId
  }

  it should "commit offset on checkpoint" in {
    val stream = createStream()
    val offset = MQOffset(batchId = 10, processedCount = 1000, lastBatchCount = 100, lastBatchTimestamp = 1000L)

    noException should be thrownBy stream.commit(offset)
  }

  it should "return empty partitions when start equals end" in {
    val stream = createStream()
    val offset = MQOffset.Initial

    val partitions = stream.planInputPartitions(offset, offset)

    partitions shouldBe empty
  }

  "MQStreamingPartition" should "track offset range" in {
    val start = MQOffset.Initial
    val end = MQOffset.Initial.nextBatch(100)
    val partition = MQStreamingPartition(0, start, end, createOptions())

    partition.startOffset.batchId shouldBe 0
    partition.endOffset.batchId shouldBe 1
  }

  it should "report expected message count from options" in {
    val options = createOptions()
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(100), options)

    partition.expectedMessageCount shouldBe options.batchSize
  }

  "MQStreamingPartitionReader transaction boundary" should "commit when expected count is reached" in {
    mockTransport.enqueueMessages(
      createMessage("aa01", "payload1"),
      createMessage("aa02", "payload2"),
      createMessage("aa03", "payload3")
    )

    val options = createOptions().copy(batchSize = 3)
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(3), options)
    val reader = createStreamingReader(partition)

    var count = 0
    while (reader.next()) {
      reader.get()
      count += 1
    }

    count shouldBe 3
    mockTransport.commitCount shouldBe 1
    mockTransport.rollbackCount shouldBe 0

    reader.close()
    mockTransport.commitCount shouldBe 1
  }

  it should "rollback on close if reading did not complete" in {
    mockTransport.enqueueMessages(
      createMessage("bb01", "payload1"),
      createMessage("bb02", "payload2")
    )

    val options = createOptions().copy(batchSize = 5)
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(5), options)
    val reader = createStreamingReader(partition)

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.close()
    mockTransport.commitCount shouldBe 0
    mockTransport.rollbackCount shouldBe 1
  }

  it should "not commit until expected count is reached" in {
    mockTransport.enqueueMessages(
      createMessage("cc01", "payload1"),
      createMessage("cc02", "payload2"),
      createMessage("cc03", "payload3"),
      createMessage("cc04", "payload4"),
      createMessage("cc05", "payload5")
    )

    val options = createOptions().copy(batchSize = 5)
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(5), options)
    val reader = createStreamingReader(partition)

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 0

    reader.next() shouldBe true
    mockTransport.commitCount shouldBe 1

    reader.next() shouldBe false
    reader.close()
    mockTransport.rollbackCount shouldBe 0
  }

  it should "save checkpoint after successful commit when checkpoint store provided" in {
    mockTransport.enqueueMessages(
      createMessage("dd01", "payload1"),
      createMessage("dd02", "payload2")
    )

    val checkpointStore = new InMemoryCheckpointStore()
    val options = createOptions().copy(batchSize = 2)
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(2), options)
    val reader = new MQStreamingPartitionReader(
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
  }

  it should "not save checkpoint when rollback happens" in {
    mockTransport.enqueueMessages(
      createMessage("ee01", "payload1")
    )

    val checkpointStore = new InMemoryCheckpointStore()
    val options = createOptions().copy(batchSize = 5)
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(5), options)
    val reader = new MQStreamingPartitionReader(
      partition,
      MQSchemaProvider.canonicalSchema,
      () => new DefaultMQClient(mockTransport),
      Some(checkpointStore)
    )

    reader.next() shouldBe true
    reader.close()

    val checkpoint = checkpointStore.load("DEV.QUEUE.1", 0).get
    checkpoint shouldBe empty
    mockTransport.rollbackCount shouldBe 1
  }

  it should "commit after exhausting queue before expected count" in {
    mockTransport.enqueueMessages(
      createMessage("ff01", "payload1"),
      createMessage("ff02", "payload2")
    )

    val options = createOptions().copy(batchSize = 10)
    val partition = MQStreamingPartition(0, MQOffset.Initial, MQOffset.Initial.nextBatch(10), options)
    val reader = createStreamingReader(partition)

    var count = 0
    while (reader.next()) {
      count += 1
    }

    count shouldBe 2
    mockTransport.commitCount shouldBe 1

    reader.close()
    mockTransport.rollbackCount shouldBe 0
  }

  private def createStreamingReader(partition: MQStreamingPartition): MQStreamingPartitionReader = {
    new MQStreamingPartitionReader(
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

  it should "be serializable for distribution" in {
    val partition = MQStreamingPartition(
      0,
      MQOffset.Initial,
      MQOffset.Initial.nextBatch(100),
      createOptions()
    )

    import java.io._
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(partition)
    oos.close()

    val bais = new ByteArrayInputStream(baos.toByteArray)
    val ois = new ObjectInputStream(bais)
    val deserialized = ois.readObject().asInstanceOf[MQStreamingPartition]

    deserialized.startOffset.batchId shouldBe 0
    deserialized.endOffset.batchId shouldBe 1
  }

  private def createOptions(): MQSourceOptions = {
    MQSourceOptions(
      queueManager = "QM1",
      channel = "DEV.APP.SVRCONN",
      connectionName = "localhost(1414)",
      queueName = "DEV.QUEUE.1",
      batchSize = 100,
      pollTimeoutMs = 1000
    )
  }

  private def createStream(): MQMicroBatchStream = {
    new MQMicroBatchStream(
      createOptions(),
      MQSchemaProvider.canonicalSchema
    )
  }
}

class MockStreamTransport extends MQTransport {
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
