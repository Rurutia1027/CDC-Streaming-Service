# CDC Streaming Service 
## Overview 
This project demostrates a production-style Change Data Capture (CDC) pipeline using a Java-based consumer. It focuses on how to reliably propagate database changes (capture via binlog/WAL) into downstream systems using Kafka, while preserving correctness through idempotency and event-time processing. 

The system follows a core principle: 
> CDC is not the source of truth -- it is a propagation mechanism for committed database state. 

## Architecture 
```
Database | OLTP (MySQL/Postgres)
-> (binlog / WAL)
Debezium Connector (Kafka Connect)
-> 
Kafka (CDC Topics)
-> 
Java Consumer (Spring Boot basesd)
-> 
Downstream System (DB / Cache / Analytics)
```

## Key Features 
### CDC Event Consumption 

### Event Normalization Layer 

### Idempotent Processing 

### Manual Offset Management 

### Event-Time Ordering 


## Technology Stack 
- Java 17+
- Spring Boot 
- Spring Kafka 
- Apache Kafka 
- Debezium (via Kafka Connect)
- MySQL / PostgreSQL (CDC source)
