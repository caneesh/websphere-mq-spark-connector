package com.ibm.mq.spark.source.stream

import com.ibm.mq.spark.core.client.MQTransport
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

  "MQOffset" should "serialize to JSON" in {
    val offset = MQOffset(100, 1000L)

    val json = offset.json()

    json should include ("100")
    json should include ("1000")
  }

  it should "deserialize from JSON" in {
    val json = """{"messageCount":100,"timestamp":1000}"""

    val offset = MQOffset.fromJson(json)

    offset.messageCount shouldBe 100
    offset.timestamp shouldBe 1000L
  }

  it should "support comparison for ordering" in {
    val offset1 = MQOffset(10, 100L)
    val offset2 = MQOffset(20, 200L)

    offset1 < offset2 shouldBe true
    offset2 > offset1 shouldBe true
  }

  "MQMicroBatchStream" should "return initial offset" in {
    val stream = createStream()

    val initial = stream.initialOffset()

    initial should not be null
    initial.asInstanceOf[MQOffset].messageCount shouldBe 0
  }

  it should "deserialize offset from JSON" in {
    val stream = createStream()
    val json = """{"messageCount":50,"timestamp":500}"""

    val offset = stream.deserializeOffset(json)

    offset.asInstanceOf[MQOffset].messageCount shouldBe 50
  }

  it should "plan partitions for micro-batch" in {
    val stream = createStream()
    val start = MQOffset(0, 0L)
    val end = MQOffset(10, 100L)

    val partitions = stream.planInputPartitions(start, end)

    partitions should have length 1
  }

  it should "report latest offset" in {
    val stream = createStream()

    val offset = stream.latestOffset()

    offset should not be null
  }

  it should "commit offset on checkpoint" in {
    val stream = createStream()
    val offset = MQOffset(100, 1000L)

    noException should be thrownBy stream.commit(offset)
  }

  "MQStreamingPartition" should "track offset range" in {
    val partition = MQStreamingPartition(
      0,
      MQOffset(0, 0L),
      MQOffset(10, 100L),
      createOptions()
    )

    partition.startOffset.messageCount shouldBe 0
    partition.endOffset.messageCount shouldBe 10
  }

  it should "be serializable for distribution" in {
    val partition = MQStreamingPartition(
      0,
      MQOffset(0, 0L),
      MQOffset(10, 100L),
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

    deserialized.startOffset.messageCount shouldBe 0
    deserialized.endOffset.messageCount shouldBe 10
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

  def enqueueMessages(msgs: RawMQMessage*): Unit = {
    messages = messages ++ msgs.toList
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

  override def commit(): Unit = {}

  override def rollback(): Unit = {}
}
