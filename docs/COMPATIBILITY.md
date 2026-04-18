# Runtime Compatibility Matrix

## Supported Versions

| Component | Version | Notes |
|-----------|---------|-------|
| Java | 8, 11, 17, 21 | Runtime compatible |
| Scala | 2.12.18 | Build time |
| Apache Spark | 3.5.x | Requires Scala 2.12 |
| IBM MQ Client | 9.3.4.1 | com.ibm.mq.allclient |

## Build Requirements

- **Maven**: 3.6+ (3.9.x recommended)
- **Java JDK**: 8+ for building
- **Scala**: 2.12.18 (managed by Maven)

## Bytecode Target

The connector produces **JVM 1.8 bytecode** due to Scala 2.12 constraints.
This bytecode is fully compatible with Java 8, 11, 17, and 21 runtimes.

## Java 11+ Runtime Notes

While the bytecode targets JVM 1.8, the connector runs correctly on Java 11+:

```bash
# Example: Running on Java 11
export JAVA_HOME=/path/to/java-11
spark-submit \
  --jars mq-spark-source-1.0.0-SNAPSHOT-shaded.jar \
  --class com.example.MyApp \
  my-app.jar
```

## Spark Compatibility

| Spark Version | Status | Notes |
|---------------|--------|-------|
| 3.5.x | Supported | Primary target |
| 3.4.x | Compatible | Should work |
| 3.3.x | Compatible | May work with minor adjustments |
| 3.2.x and earlier | Not tested | May require code changes |

## IBM MQ Compatibility

| MQ Version | Client Version | Status |
|------------|----------------|--------|
| 9.3.x | 9.3.4.1 | Supported |
| 9.2.x | 9.2.x | Should work |
| 9.1.x | 9.1.x | May work |
| 9.0.x and earlier | - | Not tested |

## Upgrading to Java 11 Bytecode

To produce Java 11 bytecode (future enhancement):

1. Upgrade Scala from 2.12 to 2.13
2. Update Spark dependency to Scala 2.13 builds
3. Update scala-maven-plugin configuration

Note: This requires Spark 3.5+ which has Scala 2.13 builds available.

## Testing Your Environment

```bash
# Verify Java version
java -version

# Verify Spark version and Scala
spark-shell --version

# Build the connector
./mvnw clean package -DskipTests

# Run tests
./mvnw test
```
