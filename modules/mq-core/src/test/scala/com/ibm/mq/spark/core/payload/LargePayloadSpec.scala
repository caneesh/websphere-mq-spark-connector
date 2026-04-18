package com.ibm.mq.spark.core.payload

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LargePayloadSpec extends AnyFlatSpec with Matchers {

  "PayloadConfig" should "have sensible defaults" in {
    val config = PayloadConfig()

    config.maxPayloadSizeBytes should be > 0L
    config.warnThresholdBytes should be > 0L
    config.enableOffload shouldBe false
  }

  it should "support customization" in {
    val config = PayloadConfig(
      maxPayloadSizeBytes = 100 * 1024 * 1024,
      warnThresholdBytes = 10 * 1024 * 1024,
      enableOffload = true
    )

    config.maxPayloadSizeBytes shouldBe 104857600
    config.enableOffload shouldBe true
  }

  "PayloadSizeValidator" should "accept payloads under limit" in {
    val validator = PayloadSizeValidator(maxBytes = 1024)
    val payload = Array.fill[Byte](500)(0)

    val result = validator.validate(payload)

    result.isValid shouldBe true
    result.sizeBytes shouldBe 500
  }

  it should "reject payloads over limit" in {
    val validator = PayloadSizeValidator(maxBytes = 100)
    val payload = Array.fill[Byte](200)(0)

    val result = validator.validate(payload)

    result.isValid shouldBe false
    result.rejection should be(defined)
    result.rejection.get should include("exceeds")
  }

  it should "warn for payloads over threshold but under limit" in {
    val validator = PayloadSizeValidator(maxBytes = 1000, warnBytes = 500)
    val payload = Array.fill[Byte](700)(0)

    val result = validator.validate(payload)

    result.isValid shouldBe true
    result.shouldWarn shouldBe true
  }

  "StreamingPayloadHandler" should "estimate size without loading fully" in {
    val handler = StreamingPayloadHandler()
    val largePayload = Array.fill[Byte](1000000)(0)

    val estimate = handler.estimateSize(largePayload)

    estimate shouldBe 1000000
  }

  it should "provide chunked iterator" in {
    val handler = StreamingPayloadHandler(chunkSize = 100)
    val payload = Array.fill[Byte](250)(1)

    val chunks = handler.chunks(payload).toList

    chunks should have size 3
    chunks(0).length shouldBe 100
    chunks(1).length shouldBe 100
    chunks(2).length shouldBe 50
  }

  "PayloadOffloadHook" should "be invokable for future extensibility" in {
    var offloadCalled = false

    val hook = new PayloadOffloadHook {
      override def shouldOffload(sizeBytes: Long): Boolean = sizeBytes > 1000
      override def offload(payload: Array[Byte], metadata: Map[String, String]): OffloadResult = {
        offloadCalled = true
        OffloadResult.Local(payload)
      }
    }

    hook.shouldOffload(2000) shouldBe true
    hook.offload(Array[Byte](1, 2, 3), Map.empty)
    offloadCalled shouldBe true
  }

  "NoOpOffloadHook" should "always return local storage" in {
    val hook = NoOpOffloadHook

    hook.shouldOffload(Long.MaxValue) shouldBe false

    val result = hook.offload(Array(1, 2, 3), Map.empty)
    result shouldBe a[OffloadResult.Local]
  }

  "LargePayloadStrategy" should "handle payloads appropriately based on size" in {
    val strategy = LargePayloadStrategy(
      config = PayloadConfig(maxPayloadSizeBytes = 1000, warnThresholdBytes = 500),
      offloadHook = NoOpOffloadHook
    )

    val small = strategy.handle(Array.fill[Byte](100)(0), Map.empty)
    small.action shouldBe PayloadAction.Process

    val medium = strategy.handle(Array.fill[Byte](700)(0), Map.empty)
    medium.action shouldBe PayloadAction.ProcessWithWarning

    val large = strategy.handle(Array.fill[Byte](2000)(0), Map.empty)
    large.action shouldBe PayloadAction.Reject
  }
}
