# Ledger CDC demo (Debezium → Kafka → ClickHouse + MySQL)

## Pipeline (default)

```text
MySQL (ledger)  →  Debezium (Kafka Connect)  →  Kafka topic
                                              →  ClickHouse Kafka engine + MV  →  MergeTree (analytics.ledger_tx_events)
                                              →  Spring consumer (MySQL projection, separate group)
```

**ClickHouse pulls from Kafka** using `ENGINE = Kafka` + `MATERIALIZED VIEW` — see
`clickhouse/init/02-kafka-engine.sql`.  
No JDBC app is required for the primary ClickHouse path.

## Stack (docker-compose)

| Service         | Role                                                  | Host ports                     |
|-----------------|-------------------------------------------------------|--------------------------------|
| MySQL           | System of record                                      | `3310` → 3306                  |
| Kafka (KRaft)   | Broker                                                | `9094` → 9092                  |
| ClickHouse      | OLAP + Kafka ingest                                   | `8123` (HTTP), `9000` (native) |
| Kafka Connect   | Debezium                                              | `8084` → 8083                  |
| ledger-writer   | REST → MySQL                                          | `8090` → 8080                  |
| ledger-consumer | MySQL projection (group `ledger-projection-consumer`) | —                              |

## Optional: JDBC / Flink / Spark → ClickHouse

Same topic, different consumer groups — **not** the default (to avoid double-writing the same rows).

```bash
docker compose -f docker-compose.yml -f docker-compose.analytics-jdbc.yml up -d --build
```

- **Spark Structured Streaming** is Java: `spark-ledger-metrics-java/` (image `spark-ledger-streaming`).

## Docs (scenario tree)

- **ClickHouse analytics + tradeoffs vs other warehouses (conceptual)**: [
  `clickhouse-analytics-scenarios-tree-en.md`](./docs/clickhouse-analytics-scenarios-tree-en.md)

## Start

```bash
cd deploy/ledger-cdc-kafka-demo
docker compose up -d --build
```

## Register Debezium connector

```bash
curl -s -X POST http://localhost:8084/connectors \
  -H 'Content-Type: application/json' \
  -d @kafka-connect/connectors/debezium-mysql-ledger.json
```

## Post a ledger line

```bash
curl -s -X POST http://localhost:8090/ledger/post \
  -H 'Content-Type: application/json' \
  -d '{"accountId":"acc-001","amountCents":500}'
```

## Verify ClickHouse (Kafka → CH)

```bash
docker exec -i ledger-clickhouse clickhouse-client -q \
  "SELECT source, tx_id, account_id, amount_cents, event_seq, balance_after FROM analytics.ledger_tx_events ORDER BY ingested_at DESC LIMIT 5 FORMAT PrettyCompact"
```

You should see `source = clickhouse_kafka` for rows ingested by the native Kafka engine.

## Verify MySQL projection

```bash
docker exec -i ledger-mysql mysql -uroot -proot ledger_db -e \
  "SELECT * FROM ledger_account_projection WHERE account_id='acc-001'\G"
```

## Four pillars (code)

1. **Partitioning** — `message.key.columns` in `debezium-mysql-ledger.json`
2. **Idempotent consumption** — `LedgerProjectionApplier` (`tx_id`)
3. **Replay** — Kafka retention + new consumer group
4. **Effective exactly-once (business layer)** — monotonic `event_seq` + manual Kafka ack

## ClickHouse init SQL changes

If you edit `clickhouse/init/*.sql`, existing volumes may not re-run scripts.  
**Recreate** ClickHouse data:

```bash
docker compose down
docker volume rm ledger-cdc-kafka-demo_ledger_clickhouse_data 2>/dev/null || true
# or remove the named volume shown by: docker volume ls | grep clickhouse
docker compose up -d
```

(Exact volume name may differ; use `docker volume ls` to find it.)
