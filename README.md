# workflow-graph-node
Workflow Graph Node

## Description
The generic component for workflow graph that allows the execution of workflows from an incoming event stream.
In addition to running the workflows, the responsibilities of this component also include shepherding them to completion
and producing output to another message queue.  

## Technology
- Java 11 on GraalVM
- Spring Boot
    - Webflux
- RabbitMQ
- Reactor
- Reactor-RabbitMQ-Streams
- Avro
- Apollo GraphQL 

## Building
```bash
mvn clean package
```

## Running
```bash
mvn spring-boot:run
```

## Logging

Handled with log4j2, currently with two possible configs; the default just outputs to standard out/err based on log level, the `log4j2-kafka.xml` config is enabled via the `kafka` profile and includes the required config to send logs to Kafka via the Kafka Log Appender. The log config has 4 properties that can be set via env and comes with sensible defaults already configured.

Kafka logging env variables:
* `LOG4J2_GRAPH_LOG_MARKER` (*default: GraphLog*) - the `org.slf4j.Marker` used to identify logs meant for Kafka
* `LOG4J2_KAFKA_BROKERS` (*default: localhost:9092*) - Kafka brokers
* `LOG4J2_KAFKA_INFO_DEBUG_TOPIC` (*default: graphlog_info_debug*) - topic to send logs of level INFO and below
* `LOG4J2_KAFKA_ERROR_WARNING_TOPIC` (*default: graphlog_error_warning*) - topic to send logs of WARNING and above

Logs sent to Kafka are in JSON format corresponding to the out of the `toJSON()` method of the `GraphLog` class

## Design

![Design](Workflow-Graph-Node.png)