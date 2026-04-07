-- ClickHouse pulls from Kafka natively: Kafka table engine + materialized view -> MergeTree.
-- Debezium value JSON: payload.op, payload.after.{tx_id, account_id, amount_cents, event_seq, balance_after}
-- Requires topic to exist (register Debezium connector first).

CREATE TABLE IF NOT EXISTS analytics.kafka_ledger_raw
(
    raw String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'ledger_finance.ledger_db.ledger_transaction',
    kafka_group_name = 'clickhouse_ledger_ingest',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_max_block_size = 1048576;

-- Filter inserts/snapshot reads; map JSON fields (Debezium may emit numbers as strings in JSON).
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.kafka_ledger_mv
            TO analytics.ledger_tx_events
            (
            source,
            tx_id,
            account_id,
            amount_cents,
            event_seq,
            balance_after,
            ingested_at
            )
AS
SELECT 'clickhouse_kafka'                                                         AS source,
       JSONExtractString(raw, 'payload', 'after', 'tx_id')                        AS tx_id,
       JSONExtractString(raw, 'payload', 'after', 'account_id')                   AS account_id,
       toInt64OrZero(JSONExtractString(raw, 'payload', 'after', 'amount_cents'))  AS amount_cents,
       toInt64OrZero(JSONExtractString(raw, 'payload', 'after', 'event_seq'))     AS event_seq,
       toInt64OrZero(JSONExtractString(raw, 'payload', 'after', 'balance_after')) AS balance_after,
       now64(3)                                                                   AS ingested_at
FROM analytics.kafka_ledger_raw
WHERE JSONExtractString(raw, 'payload', 'op') IN ('c', 'r');
