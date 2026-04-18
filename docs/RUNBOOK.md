# IBM MQ Spark Connector Runbook

## Quick Start

### Batch Reading
```scala
val df = spark.read
  .format("ibm-mq")
  .option("queueManager", "QM1")
  .option("channel", "DEV.APP.SVRCONN")
  .option("connectionName", "localhost(1414)")
  .option("queueName", "DEV.QUEUE.1")
  .load()
```

### Structured Streaming
```scala
val stream = spark.readStream
  .format("ibm-mq")
  .option("queueManager", "QM1")
  .option("channel", "DEV.APP.SVRCONN")
  .option("connectionName", "localhost(1414)")
  .option("queueName", "DEV.QUEUE.1")
  .load()

stream.writeStream
  .format("console")
  .option("checkpointLocation", "/tmp/mq-checkpoint")
  .start()
```

## Common Operations

### Pause Processing
```scala
connectorControl.pause()
// Processing stops, messages remain in queue
```

### Resume Processing
```scala
connectorControl.resume()
// Processing resumes from last position
```

### Graceful Shutdown
```scala
connectorControl.enterDrainMode()
// Wait for in-flight messages to complete
coordinator.awaitDrain(timeoutMs = 30000)
connectorControl.completeDrain()
```

## Troubleshooting

### Connection Failures

**Symptom**: `MQRC_NOT_AUTHORIZED` or connection refused

**Checks**:
1. Verify queue manager is running
2. Check channel authentication (CHLAUTH)
3. Verify user has CONNECT authority
4. Check firewall/network access

**Resolution**:
```bash
# Check channel status
echo "DISPLAY CHSTATUS(DEV.APP.SVRCONN)" | runmqsc QM1

# Check user authority
dmpmqaut -m QM1 -t qmgr -p username
```

### Message Backlog

**Symptom**: Messages accumulating in queue, processing slow

**Checks**:
1. Check batch size configuration
2. Monitor poll duration metrics
3. Check downstream processing bottlenecks

**Resolution**:
- Increase `batchSize` if network latency is high
- Add more partitions/parallel readers
- Check for poison messages causing retries

### Poison Messages

**Symptom**: Same messages repeatedly failing

**Checks**:
1. Check message backout count
2. Review parse failure metrics
3. Examine message content

**Resolution**:
- Configure backout threshold
- Set up dead letter queue
- Use quarantine handler

### Memory Issues

**Symptom**: OutOfMemoryError, driver memory pressure

**Checks**:
1. Check average message size
2. Monitor batch sizes
3. Review payload offload settings

**Resolution**:
- Reduce batch size
- Configure payload size limits
- Enable large payload offload

### Checkpoint Issues

**Symptom**: Messages replayed after restart

**Checks**:
1. Verify checkpoint directory exists and is writable
2. Check checkpoint file contents
3. Review checkpoint timing

**Resolution**:
- Ensure checkpoint location is on reliable storage
- Consider using HDFS/S3 for checkpoints
- Review commit/checkpoint ordering

## Monitoring

### Key Metrics to Watch
- `mq_messages_received` - Throughput
- `mq_parse_failure` - Data quality issues
- `mq_retries` - Transient failures
- `mq_connection_failure` - Infrastructure issues

### Health Check Endpoints
```scala
val health = MQHealthCheck.check(config)
if (!health.isHealthy) {
  alert(s"MQ unhealthy: ${health.message}")
}
```

### Log Analysis
Key log patterns to monitor:
- `MQRC_` - MQ reason codes
- `backout count` - Poison message warnings
- `timeout` - Connection issues
- `parse error` - Data quality

## Recovery Procedures

### Recover from Checkpoint
Streaming automatically recovers from checkpoint on restart.
For manual recovery:
```scala
val checkpoint = store.load(queueName, partitionId)
// Resume from checkpoint.lastMessageId
```

### Clear Stuck Messages
```bash
# Browse without remove
echo "DISPLAY QSTATUS(QUEUE.NAME) ALL" | runmqsc QM1

# Clear queue (destructive!)
echo "CLEAR QLOCAL(QUEUE.NAME)" | runmqsc QM1
```

### Reset to Beginning
Delete checkpoint file and restart:
```bash
rm /checkpoint/location/queue_name/0.json
# Restart Spark job
```

## Performance Tuning

### Batch Size
- Small batches: Lower latency, more overhead
- Large batches: Higher throughput, more memory
- Start with 1000, adjust based on message size

### Poll Timeout
- Short timeout: More responsive to new messages
- Long timeout: Less CPU overhead when queue empty
- Start with 5000ms

### Throttling
For rate-limited downstream systems:
```scala
val config = QueueConfig(
  queueName = "MY.QUEUE",
  throttleMessagesPerSecond = Some(100)
)
```
