# IBM MQ Spark Connector

Apache Spark connector for IBM WebSphere MQ, enabling streaming and batch data pipelines from MQ queues.

## Module Structure

| Module | Description |
|--------|-------------|
| **mq-core** | Core MQ client abstractions, connection management, message types, and configuration |
| **mq-parsers** | Message parsing utilities for JSON, XML, and binary formats |
| **mq-spark-source** | Spark DataSource V2 implementation for reading from MQ queues |
| **mq-observability** | Metrics (Micrometer) and health check infrastructure |
| **mq-testkit** | Test utilities, mocks, and fixtures for unit/integration testing |
| **examples** | Usage examples demonstrating connector features |

## Requirements

- Java 8+
- Scala 2.12.18
- Apache Spark 3.5.x
- IBM MQ 9.3.x client libraries

## Building

```bash
./mvnw clean verify
```

## Usage

```scala
val df = spark.read
  .format("ibm-mq")
  .option("queueManager", "QM1")
  .option("channel", "CHANNEL1")
  .option("connectionName", "localhost(1414)")
  .option("queueName", "QUEUE1")
  .load()
```

## License

Apache License 2.0
