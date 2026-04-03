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
- Supports Debezium-sytle CDC events
- Compatible with Canal-style payloads (optional normalization layer)
- Handles insert / update / delete operations 

### Event Normalization Layer 
- Unifies different CDC formats into a common internal model 
- Abstracts envelope structures (before/after, op codes)

### Idempotent Processing 
- Ensures safe reprocessing (at-least-once delivery)
- Uses business keys (e.g., `event_id`, `transaction_id`) for deduplication

### Manual Offset Management
- Kafka manual acknowledment 
- Commit offsets only after successful processing  

### Event-Time Ordering 
- Preserves ordering using: 

> partition key (e.g., account_id) 
> event timestamp 

- Handles out-of-order events where applicable 


## Technology Stack 
- Java 17+
- Spring Boot 
- Spring Kafka 
- Apache Kafka 
- Debezium (via Kafka Connect)
- MySQL / PostgreSQL (CDC source)
