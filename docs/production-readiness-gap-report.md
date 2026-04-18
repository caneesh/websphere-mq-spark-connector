# Production Readiness Gap Report

**Date:** 2024-01
**Status:** Pre-production
**Reviewer:** Principal Engineer Assessment

## Executive Summary

This IBM MQ Spark connector is functionally complete for batch and micro-batch streaming use cases with at-least-once delivery semantics. The core transaction boundary design is sound. However, several gaps remain before production deployment.

## What Is Implemented

### Core Functionality

| Component | Status | Notes |
|-----------|--------|-------|
| IBM MQ Transport | Complete | Uses base MQ classes (not JMS) for MQMD access |
| Batch Read | Complete | DataSource V2 TableProvider with SupportsRead |
| Micro-batch Streaming | Complete | MicroBatchStream with honest progress model |
| Transaction Boundaries | Complete | Commit on complete, rollback on failure |
| Checkpoint Persistence | Complete | File-based and in-memory implementations |
| Schema Provider | Complete | 21 MQMD fields mapped to Spark schema |
| Failure Classification | Complete | Transient vs permanent error detection |
| Retry with Backoff | Complete | Configurable exponential backoff |
| Poison Message Handling | Complete | Backout threshold detection, quarantine hooks |
| Payload Parsers | Complete | JSON, XML, Text with composite fallback |

### Spark Integration

- `TableProvider` with `SupportsRead` capability
- `BATCH_READ` and `MICRO_BATCH_READ` table capabilities
- `readStream.format("ibm-mq")` registration works
- Partition planning and reader factory implementation
- Options parsing with validation

### Observability

- Micrometer-based metrics integration
- Audit event emission (console, structured log, in-memory)
- Connection/poll/commit/rollback metrics
- Parse success/failure tracking

## What Is Limited

### Delivery Semantics

**At-least-once only.** This connector cannot provide exactly-once semantics because:

1. MQ transaction commit happens in the reader (executor), before Spark checkpoints (driver)
2. If Spark fails after MQ commit but before checkpoint, messages will be replayed
3. This is fundamental to MQ's transactional model — not a bug

**Mitigation:** Use `messageId` for downstream idempotency. The connector exposes `messageId` as a binary field in every row.

### Streaming Progress Model

**No arbitrary offset replay.** Unlike Kafka, MQ does not support seeking to a specific offset. The connector uses a batch-based progress model:

- `batchId` increments monotonically
- `processedCount` tracks total messages
- Restart resumes from queue's current state, not from a saved position

**Implication:** If the queue has drained between failures, you cannot "replay from offset X."

### No Write Support

The connector is read-only. Writing to MQ is not implemented. Use the IBM MQ client directly for publishing.

### Single Partition

The current implementation uses a single partition per micro-batch. MQ queues are not inherently partitionable like Kafka topics. Multi-partition support would require:

- Queue clustering configuration on MQ side
- Multiple queue consumption in parallel
- Coordination to avoid duplicate reads

### No CCDT Testing

Client Channel Definition Table (CCDT) support is implemented but not tested. The `usesCcdt` and `ccdtFilePath` options exist but have not been validated against a real CCDT file.

### Integration Tests Require MQ

Unit tests use mocks extensively. Integration tests exist but require a running MQ instance:

```bash
docker run -d --name mq-test \
  -e LICENSE=accept -e MQ_QMGR_NAME=QM1 \
  -p 1414:1414 ibmcom/mq:latest

export MQ_INTEGRATION_ENABLED=true
mvn test
```

No automated CI pipeline with MQ container is configured.

## Actual Delivery Semantics

### Batch Mode

1. Reader connects to MQ under syncpoint
2. Messages are read transactionally (GET with syncpoint)
3. After all messages read (or batch limit), reader commits
4. If reader fails mid-batch, uncommitted messages are rolled back
5. MQ redelivers rolled-back messages with incremented `backoutCount`

**Guarantee:** At-least-once. Messages are committed to MQ before Spark task completes.

### Streaming Mode

1. Micro-batch is planned with expected message count
2. Reader polls up to expected count
3. Reader commits when count reached or queue exhausted
4. Spark checkpoints offset after reader completes
5. Gap: Between MQ commit and Spark checkpoint, failure causes replay

**Guarantee:** At-least-once with potential duplicates in the commit-to-checkpoint window.

## Restart Semantics

### With Checkpoint Store

1. On restart, load checkpoint for (queue, partition)
2. Checkpoint contains: `lastMessageId`, `messagesProcessed`, `bytesProcessed`
3. `lastMessageId` can be used for downstream dedup
4. Processing resumes from queue's current state (not from checkpoint position)

### Without Checkpoint Store

1. Processing resumes from queue's current state
2. No metadata about previous progress
3. Full replay if messages weren't committed before failure

### Failure Scenarios

| Scenario | Outcome |
|----------|---------|
| Reader fails before commit | Messages rolled back, redelivered |
| Reader commits, Spark fails before checkpoint | Messages replayed (duplicates) |
| Spark checkpoints successfully | Progress is durable |
| Queue emptied between failures | Cannot replay (no offset seek) |

## Operational Expectations

### Configuration Required

```scala
spark.read
  .format("ibm-mq")
  .option("queueManager", "QM1")
  .option("channel", "DEV.APP.SVRCONN")
  .option("connectionName", "mqhost(1414)")
  .option("queueName", "MY.QUEUE")
  .option("batchSize", "1000")
  .option("pollTimeoutMs", "5000")
  .load()
```

### Monitoring Points

- `mq.messages.received` — messages consumed
- `mq.messages.committed` — messages acknowledged
- `mq.messages.rolledback` — messages returned to queue
- `mq.poll.duration` — time spent polling
- `mq.connection.attempts` — connection success/failure

### Backpressure

No backpressure mechanism exists. The connector polls up to `batchSize` messages per micro-batch. If downstream cannot keep up, either:

- Reduce `batchSize`
- Increase micro-batch interval in Spark streaming

## Known Risks

### Risk 1: MQ Connection Pooling

**Issue:** Each partition reader creates its own MQ connection. In high-parallelism scenarios, this could exhaust MQ channel limits.

**Mitigation:** Use single partition (current default) or configure MQ with higher channel limits.

### Risk 2: Large Messages

**Issue:** Large payloads (>1MB) are read fully into memory. No streaming/chunked read.

**Mitigation:** Configure MQ maximum message size appropriately. Monitor JVM heap.

### Risk 3: Poison Messages

**Issue:** Messages that consistently fail processing will increment `backoutCount` until threshold. Connector detects but does not automatically move to DLQ.

**Mitigation:** Implement poison message handler with actual DLQ routing, or configure MQ backout queue.

### Risk 4: SSL/TLS Configuration

**Issue:** SSL is supported via options but no test coverage for TLS 1.3 or client certificates.

**Mitigation:** Test SSL configuration thoroughly before production.

## What Is Not Production-Ready

1. **No automated integration tests in CI** — requires manual MQ setup
2. **No performance benchmarks** — throughput/latency characteristics unknown
3. **No documented failure recovery procedures** — operators would need to develop runbooks
4. **No health check endpoint** — Spark integration only, no standalone health probe
5. **No schema evolution strategy** — adding fields to canonical schema not documented
6. **CCDT untested** — client channel definition table support is code-complete but unvalidated

## Next Steps

1. **Set up CI with MQ container** — Testcontainers or dedicated MQ in CI pipeline
2. **Performance testing** — Establish baseline throughput and latency
3. **Operational runbook** — Document failure scenarios and recovery procedures
4. **Security review** — Audit SSL configuration, credential handling
5. **CCDT validation** — Test with actual CCDT file from MQ administrator
6. **Consider write support** — If bidirectional messaging is needed

## Conclusion

The connector is architecturally sound for at-least-once delivery from MQ to Spark. Transaction boundaries are correctly implemented. The main gaps are operational: no CI integration tests, no performance data, and no operational documentation. For a production deployment, these operational gaps must be addressed, and the exactly-once limitation must be clearly communicated to downstream consumers.

---

*This report reflects the state of the codebase as of the current commit. It should be updated as gaps are addressed.*
