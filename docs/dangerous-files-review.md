# Dangerous Files Corrective Pass

**Scope:** Targeted review of files most likely to contain critical bugs
**Date:** 2024-01

---

## File-by-File Summary

### 1. MQJmsTransport.scala

**Location:** `modules/mq-spark-source/src/main/scala/com/ibm/mq/spark/source/batch/MQJmsTransport.scala`

**Purpose:** Serializable wrapper around RealMQTransport for Spark executor distribution

**What Was Wrong:**
- `receive()` called `ensureDelegate()` which created delegate but didn't check connection state — would fail with cryptic error if not connected
- `commit()` and `rollback()` silently did nothing if delegate was null — dangerous for transaction safety (silent failures)

**Fix Applied:**
```scala
override def receive(waitMillis: Long): Option[RawMQMessage] = {
  if (delegate == null || !delegate.isConnected) {
    throw new IllegalStateException("Transport not connected. Call connect() first.")
  }
  delegate.receive(waitMillis)
}

override def commit(): Unit = {
  if (delegate == null || !delegate.isConnected) {
    throw new IllegalStateException("Cannot commit: transport not connected")
  }
  delegate.commit()
}
```

**Impact:** Fail-fast behavior for programming errors

---

### 2. MQDataSource.scala

**Location:** `modules/mq-spark-source/src/main/scala/com/ibm/mq/spark/source/MQDataSource.scala`

**Purpose:** Spark DataSource V2 TableProvider entry point

**What Was Wrong:** Nothing critical

**Review Notes:**
- Proper `TableProvider` with `SupportsRead`
- Correct capability exposure (`BATCH_READ`, `MICRO_BATCH_READ`)
- Options validation in `validateOptions()`
- Schema defaults to canonical schema

**No Fix Required**

---

### 3. MQMicroBatchStream.scala

**Location:** `modules/mq-spark-source/src/main/scala/com/ibm/mq/spark/source/stream/MQMicroBatchStream.scala`

**Purpose:** Structured Streaming micro-batch source

**What Was Wrong:**
- `planInputPartitions()` used unsafe `asInstanceOf[MQOffset]` cast without validation
- Would throw confusing `ClassCastException` if Spark passed wrong offset type

**Fix Applied:**
```scala
val startOffset = start match {
  case o: MQOffset => o
  case _ => throw new IllegalArgumentException(
    s"Expected MQOffset but got ${start.getClass.getName}...")
}
```

**Impact:** Clear error message instead of opaque ClassCastException

---

### 4. MQPartitionReader.scala

**Location:** `modules/mq-spark-source/src/main/scala/com/ibm/mq/spark/source/batch/MQPartitionReader.scala`

**Purpose:** Batch partition reader with transaction boundaries

**What Was Wrong:** Nothing — previously reviewed and correct

**Review Notes:**
- Commit only in `markReadingComplete()` after all messages read
- Rollback in `close()` if reading failed or incomplete
- Checkpoint saved after commit
- Clear documentation of failure scenarios

**No Fix Required**

---

### 5. MQStreamingPartitionReader.scala

**Location:** `modules/mq-spark-source/src/main/scala/com/ibm/mq/spark/source/stream/MQStreamingPartitionReader.scala`

**Purpose:** Streaming partition reader with transaction boundaries

**What Was Wrong:** Nothing — same pattern as batch reader

**Review Notes:**
- Same commit/rollback pattern as MQPartitionReader
- Commit when expected count reached
- Rollback on incomplete close
- Well documented

**No Fix Required**

---

### 6. pom.xml

**Location:** Root `pom.xml`

**Purpose:** Maven build configuration

**What Was Wrong:** Nothing

**Review Notes:**
- Java 8 target is correct (Scala 2.12 cannot target JVM 11)
- Scala 2.12.18 is appropriate for Spark 3.5.x
- IBM MQ client 9.3.4.1 is current
- Dependency versions are reasonable
- Shade profile available for fat JAR

**No Fix Required**

---

## Tests Added/Updated

No new tests needed — existing tests cover the fixed behaviors:
- `MQStreamingSpec` covers offset handling
- Transaction boundary tests verify commit/rollback

---

## Summary

| File | Issues Found | Severity | Fixed |
|------|--------------|----------|-------|
| MQJmsTransport.scala | Silent failures on null delegate | MEDIUM | YES |
| MQDataSource.scala | None | - | N/A |
| MQMicroBatchStream.scala | Unsafe type cast | LOW | YES |
| MQPartitionReader.scala | None | - | N/A |
| MQStreamingPartitionReader.scala | None | - | N/A |
| pom.xml | None | - | N/A |

**Total Issues:** 2 (both fixed)
**Critical Issues:** 0

---

*This review completes Gap Fix Prompt 12.*
