CREATE DATABASE IF NOT EXISTS ledger_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Debezium user: replication + read source tables
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium';
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT, RELOAD ON *.* TO 'debezium'@'%';

-- Application user: writer + consumer projection tables
CREATE USER IF NOT EXISTS 'ledger_app'@'%' IDENTIFIED BY 'ledger_app';
GRANT ALL PRIVILEGES ON ledger_db.* TO 'ledger_app'@'%';
FLUSH PRIVILEGES;

USE ledger_db;

-- Per-account running balance + monotonic sequence (written in same DB transaction as the ledger row).
CREATE TABLE IF NOT EXISTS ledger_account_running
(
    account_id    VARCHAR(64) PRIMARY KEY,
    balance_cents BIGINT NOT NULL DEFAULT 0,
    next_seq      BIGINT NOT NULL DEFAULT 0
);

-- Immutable ledger lines (system of record append + derived balance_after).
CREATE TABLE IF NOT EXISTS ledger_transaction
(
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    account_id    VARCHAR(64)  NOT NULL,
    tx_id         VARCHAR(128) NOT NULL,
    amount_cents  BIGINT       NOT NULL,
    event_seq     BIGINT       NOT NULL,
    balance_after BIGINT       NOT NULL,
    created_at    TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    UNIQUE KEY uk_tx_id (tx_id),
    KEY           idx_account_seq(account_id, event_seq)
);

-- Consumer: exactly-once style dedup at business layer (seen tx_id).
CREATE TABLE IF NOT EXISTS ledger_consumer_processed
(
    tx_id      VARCHAR(128) PRIMARY KEY,
    applied_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
);

-- Consumer: read model (eventually consistent with ledger_transaction).
CREATE TABLE IF NOT EXISTS ledger_account_projection
(
    account_id     VARCHAR(64) PRIMARY KEY,
    balance_cents  BIGINT       NOT NULL DEFAULT 0,
    last_event_seq BIGINT       NOT NULL DEFAULT 0,
    last_tx_id     VARCHAR(128) NULL,
    updated_at     TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);
