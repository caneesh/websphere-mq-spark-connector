# IBM MQ Spark Connector Architecture

## Overview

This connector enables Apache Spark applications to read messages from IBM MQ queues as DataFrames, supporting both batch and structured streaming workloads.

## Module Structure

```
websphere-mq-spark-connector/
├── modules/
│   ├── mq-core/          # Core domain, client, configuration
│   ├── mq-spark-source/  # Spark DataSource V2 implementation
│   ├── mq-parsers/       # Payload parsers (JSON, XML, text)
│   ├── mq-observability/ # Metrics, health checks, audit
│   └── mq-testkit/       # Test utilities and mocks
```

## Core Components

### Connection Layer
- `MQConnectionConfig`: Multi-host connection configuration
- `MQConnectionFactory`: XMSC property builder for JMS connections
- `MQConnectionState`: State machine for connection lifecycle

### Client Layer
- `MQTransport`: Low-level transport abstraction
- `DefaultMQClient`: Transactional consumer with batch polling
- `ClientStats`: Poll metrics and statistics

### Message Model
- `RawMQMessage`: Full MQ metadata (CCSID, encoding, priority, etc.)
- `MQMessageEnvelope`: Processing wrapper with timing
- `ConnectorCheckpoint`: Position tracking for restarts

### Retry and Backout
- `RetryPolicy`: Exponential backoff with jitter
- `FailureClassifier`: Transient vs permanent failure detection
- `PoisonMessageHandler`: Quarantine decisions for poison messages

### Spark Integration
- `MQDataSource`: TableProvider implementation
- `MQBatchScan`: Batch reading with partitions
- `MQMicroBatchStream`: Structured streaming with offsets
- `MQSchemaProvider`: Canonical DataFrame schema
- `MQRowConverter`: Message to Row conversion

## Data Flow

```
MQ Queue → MQTransport → DefaultMQClient → MQPartitionReader
                                               ↓
                                         PayloadParser
                                               ↓
                                         MQRowConverter
                                               ↓
                                          Spark Row
```

## Configuration Reference

### Required Options
- `queueManager`: MQ queue manager name
- `channel`: MQ channel name  
- `connectionName`: Host and port (e.g., "localhost(1414)")
- `queueName`: Target queue name

### Optional Options
- `user`, `password`: Authentication credentials
- `sslCipherSuite`: TLS cipher suite
- `batchSize`: Messages per batch (default: 1000)
- `pollTimeoutMs`: Poll timeout (default: 5000)

## Delivery Semantics

### At-Least-Once
The connector provides at-least-once delivery by default:
- Messages are read within a transaction
- Checkpoint saved after successful commit
- Restart replays from last checkpoint

### Considerations
- If crash occurs between MQ commit and checkpoint, messages replay
- For exactly-once, integrate with transactional checkpoint store

## Checkpoint Model

Checkpoints track:
- Queue name and partition
- Last processed message ID
- Timestamp and message counts

Storage options:
- `FileCheckpointStore`: JSON files on local/shared filesystem
- `InMemoryCheckpointStore`: Testing only (not persistent)

## Security

### TLS Configuration
- Keystore/truststore paths and passwords
- Cipher suite selection
- Protocol version (TLS 1.2+)

### Credential Management
- Environment variable substitution
- Secret redaction in logs
- Least privilege guidance for MQ permissions

## Metrics (Prometheus-Compatible)

- `mq_messages_received`: Total messages received
- `mq_bytes_received`: Total bytes received
- `mq_parse_success/failure`: Parse outcomes
- `mq_retries`: Retry attempts
- `mq_connection_success/failure`: Connection outcomes
