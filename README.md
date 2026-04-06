# Ledger CDC demo (Debezium + Kafka + Spring Boot)

Industrial-style reference for **scenario 1 (ledger / balance propagation)** without Canal:

- **MySQL** `ledger_db` is the **system of record** (append-only `ledger_transaction` + per-account running row).
- **Debezium** (Kafka Connect) reads **binlog** and publishes to Kafka.
- **Partition key** = `account_id` via `message.key.columns` (per-account ordering inside one partition).
- **Spring consumer** implements **idempotent + monotonic** projection updates and **manual offset commit**.

## Stack (docker-compose)

| Service         | Image                          | Host ports (defaults) |
|-----------------|--------------------------------|-----------------------|
| MySQL 8         | `mysql:8.0`                    | `3310` → 3306         |
| Kafka (KRaft)   | `bitnami/kafka:3.7`            | `9094` → 9092         |
| Kafka Connect   | `quay.io/debezium/connect:2.7` | `8084` → 8083 (REST)  |
| Ledger writer   | build `ledger-writer-java`     | `8090` → 8080         |
| Ledger consumer | build `ledger-consumer-java`   | (no public port)      |

## Start

```shell
docker compose up -d --build 
```

## Register the Debezium MySQL connector

After Connect is up, use curl command below register Debezium MySQL connector

```shell
curl -s -X POST http://localhost:8084/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka-connect/connectors/debezium-mysql-ledger.json
```

Then check status:

```shell
curl -s http://localhost:8084/connectors/ledger-mysql-cdc/status | jq .
```

## Post a ledger line (system of record)

```bash
curl -s -X POST http://localhost:8090/ledger/post \
  -H 'Content-Type: application/json' \
  -d '{"accountId":"acc-001","amountCents":500}'
```

## Verify projection (eventually consistent read model)

```bash
# expected data flow is 
# first request insert new record to mysql db 
# then debezium & kafka connector detect that, convert metadata + db record as one event and sync to kafka
# ledger-consumer side listen to the kafka corresponding topic, subscribe the event and extracted fields save to db in ledger_db
# and that record cna be fetched via the sql command below (we can also add controller fetch it)  
docker exec -i ledger-mysql mysql -uroot -proot ledger_db -e \
  "SELECT * FROM ledger_account_projection WHERE account_id='acc-001'\G"
```

## Four pillars mapped to code

### Partition & ordering

- `kafka-connect/connectors/debezium-mysql-ledger.json`
- `message.key.columns` = `ledger_db.ledger_transaction:account`
  All changes for one account share one Kafka partition -> broker preserves order for that key.

### Idempotent consumption

- `ledger-consumer-java/.../LedgerProjectionApplier.java` Deduplicate by `tx_id` in `ledger_consumer_processed` before
  mutating the projection.

### Replay

- Kafka's `log.retention.hours` raised in compose for local exercise; production uses retention + archival.
- Resets offsets or use a **new consumer group** to rebuild `ledger_account_projection` from the log (handlers must stay
  replay-safe).

### Effective exactly-once (business-layer)

- same applier + **monotonic `event_seq`** guard (`last_event_seq < event_seq` on update); listener uses **manual ack**
  only after the DB transaction succeeds (`application.yml`: `enable-auto-commit: false`, `ack-mode: manual_immediate`.

Core consumer entrypoint: `ledger-consumer-java/.../cdc/LedgerDebeziumListener.java`

## Operational notes

- Run **one active Debezium connector** per captured table set in production, or split by bounded context.
- Add **DLQ / retry caps** for poison messages; this demo retries by not committing the offset on failure.
- For very high volume, scale consumer instance up to the **topic partition count** and keep **partition key =
  account_id**. 
