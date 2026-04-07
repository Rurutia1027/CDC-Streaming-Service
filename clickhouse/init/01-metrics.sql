-- Raw CDC-derived rows ingested by different consumer groups / engines (microservice, Flink, Spark)
CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.ledger_tx_events
(
    source        LowCardinality(String),
    tx_id         String,
    account_id    String,
    amount_cents  Int64,
    event_seq     Int64,
    balance_after Int64,
    ingested_at   DateTime64(3) DEFAULT now64(3)
) ENGINE = MergeTree
ORDER BY (account_id, event_seq, tx_id);