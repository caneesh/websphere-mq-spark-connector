# Principal Architect Review

**Date:** 2024-01
**Scope:** Full codebase review post gap-fix implementation
**Verdict:** Architecturally sound, several medium-severity issues remain

## Review Areas

### 1. Transaction Safety

**Status:** Sound

The transaction boundary design is correct:
- Messages read under MQ syncpoint (`MQGMO_SYNCPOINT`)
- Commit only after successful read completion (`markReadingComplete()`)
- Rollback on failure or incomplete close
- Clear documentation of at-least-once semantics

**Verified in:**
- `MQPartitionReader.scala`: commit on complete, rollback on failure
- `MQStreamingPartitionReader.scala`: same pattern for streaming
- `RealMQTransport.scala`: proper `commit()` and `backout()` calls

**Limitation acknowledged:** Commit happens in reader before Spark checkpoint. This is fundamental to MQ's model.

### 2. Spark Source Correctness

**Status:** Correct

DataSource V2 implementation follows the API contract:
- `TableProvider` with `SupportsRead`
- `BATCH_READ` and `MICRO_BATCH_READ` capabilities
- Proper `Scan`, `Batch`, and `MicroBatchStream` implementation
- Options validation at scan builder creation

**Verified in:**
- `MQDataSource.scala`: proper capability exposure
- `MQUnifiedScan.scala`: correct `toBatch` and `toMicroBatchStream`
- `MQMicroBatchStream.scala`: honest offset model (batch-based, not fake sequential)

### 3. Restart and Checkpoint Logic

**Status:** Functional with known gaps

Checkpoint store:
- Saves after MQ commit
- Loads on restart
- Per-queue, per-partition isolation

**Gap:** Checkpoint is advisory only. On restart, processing resumes from queue's current state, not from checkpoint position. MQ doesn't support offset seek.

### 4. Code Modularity

**Status:** Good separation

- `mq-core`: Transport, checkpoint, config, retry logic
- `mq-spark-source`: Spark-specific wiring
- `mq-parsers`: Payload parsing
- `mq-observability`: Metrics and audit
- `mq-testkit`: Test infrastructure

Clean dependency graph. No circular dependencies.

### 5. Observability and Error Handling

**Status:** Adequate

- Micrometer metrics integration
- Structured audit events
- Failure classification (transient vs permanent)
- Logging throughout

**Gap:** No circuit breaker for cascading failures.

### 6. Security Hygiene

**Status:** Needs improvement

- SSL passwords set via `System.setProperty` (global visibility) — necessary for MQ, but creates exposure
- Password redacted in debug logs
- No credential validation beyond presence check
- CCDT path handling could be exploited with malicious paths

### 7. Scaffolding vs Implementation

**Status:** All scaffolding replaced

Verified implementations:
- `RealMQTransport`: Real MQ base classes, not mocks
- `MQPartitionReader`: Full transaction logic
- `FileCheckpointStore`: Real file I/O
- `MQRowConverter`: Actual MQMD field extraction

---

## Top 15 Issues

| # | Severity | Issue | File | Status |
|---|----------|-------|------|--------|
| 1 | HIGH | SSL passwords in System.setProperty (global scope) | RealMQTransport.scala | Documented |
| 2 | HIGH | Insufficient filename sanitization in checkpoint store | FileCheckpointStore.scala | **FIXED** |
| 3 | MEDIUM | Manual JSON parsing in ConnectorCheckpoint is fragile | ConnectorCheckpoint.scala | Remaining |
| 4 | MEDIUM | Connection timeout not enforced at MQ level | RealMQTransport.scala | **FIXED** |
| 5 | MEDIUM | CheckpointStore.list() loads all checkpoints into memory | FileCheckpointStore.scala | Remaining |
| 6 | MEDIUM | CCDT path parsing uses substring without validation | RealMQTransport.scala | Remaining |
| 7 | MEDIUM | No message size limit validation (OOM risk) | RealMQTransport.scala | Remaining |
| 8 | LOW | No graceful shutdown hook for streaming | MQMicroBatchStream.scala | Remaining |
| 9 | LOW | MQOffset deserialization accepts unbounded arrays | MQOffset.scala | Remaining |
| 10 | LOW | PrintWriter without explicit charset | FileCheckpointStore.scala | Remaining |
| 11 | LOW | No circuit breaker for repeated failures | RetryPolicy | Remaining |
| 12 | LOW | SSL cipher suite not validated | MQConnectionConfig.scala | Remaining |
| 13 | LOW | Timestamp conversion assumes non-null | RealMQTransport.scala | Handled |
| 14 | INFO | Some unused imports | Various | Remaining |
| 15 | INFO | Inconsistent Try/match vs try/catch patterns | Various | Remaining |

---

## Fixes Applied

### Fix 1: Checkpoint Filename Sanitization (HIGH)

**Before:** Only replaced `/`, `\`, `:`

**After:** Also handles `..`, `<`, `>`, `|`, `*`, `?`, `"`, and leading dots to prevent path traversal and invalid filenames.

```scala
private def sanitizeName(name: String): String = {
  name
    .replace("/", "_")
    .replace("\\", "_")
    .replace(":", "_")
    .replace("..", "_")
    .replace("<", "_")
    .replace(">", "_")
    .replace("|", "_")
    .replace("*", "_")
    .replace("?", "_")
    .replace("\"", "_")
    .replaceAll("^\\.", "_")
}
```

### Fix 2: Connection Timeout (MEDIUM)

**Before:** `connectTimeout` field existed but wasn't passed to MQ

**After:** Added to connection properties

```scala
if (config.connectTimeout > 0) {
  props.put("connectTimeout", config.connectTimeout.asInstanceOf[java.lang.Long])
}
```

---

## Remaining Technical Debt

### Should Fix Before Production

1. **Replace manual JSON parsing** with a proper JSON library (circe, spray-json, or play-json). Current regex-based parsing will fail on edge cases.

2. **Add message size limit** in `extractRawMessage` to prevent OOM on malformed messages.

3. **Add circuit breaker** to retry logic to prevent cascading failures when MQ is unavailable.

### Can Fix Later

4. **CCDT path validation** — check for path traversal attempts

5. **Explicit charset** in PrintWriter — use UTF-8 explicitly

6. **Streaming shutdown hook** — register JVM shutdown hook for graceful stream termination

7. **Checkpoint list pagination** — add limit/offset to list() method

### Won't Fix (Acceptable Risk)

8. **SSL passwords in System.setProperty** — required by MQ client architecture. Document in security guidelines that JVM should run with minimal other code.

---

## Verdict

The connector is architecturally sound. Transaction boundaries are correct. The at-least-once delivery model is honestly documented. High-severity issues have been addressed.

**Recommended before production:**
1. Replace manual JSON parsing
2. Add message size limits
3. Add circuit breaker
4. Performance test under load

**Not blocking production:**
- Remaining low-severity issues
- Cosmetic cleanup

---

*Review conducted after Gap Fix Prompts 1-10 implementation.*
