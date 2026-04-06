package com.tus.ledger.consumer.projection;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Applies CDC-derived facts to a read model with:
 * <ul>
 *   <li>(2) Idempotent consumption: {@code tx_id} dedupe table</li>
 *   <li>(4) Monotonic versioning: ignore stale rows where {@code event_seq} is not strictly newer than projection</li>
 * </ul>
 * Together with at-least-once Kafka delivery, this yields effectively-once semantics for the projection.
 */
@Service
public class LedgerProjectionApplier {

    private final JdbcTemplate jdbcTemplate;

    public LedgerProjectionApplier(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Transactional
    public void applyInsert(String txId, String accountId, long eventSeq, long balanceAfterCents) {
        Integer seen = jdbcTemplate.queryForObject(
                "SELECT COUNT(1) FROM ledger_consumer_processed WHERE tx_id = ?", Integer.class, txId);
        if (seen != null && seen > 0) {
            return;
        }

        Long lastSeq = jdbcTemplate.query(
                "SELECT last_event_seq FROM ledger_account_projection WHERE account_id = ?",
                rs -> {
                    if (!rs.next()) {
                        return null;
                    }
                    return rs.getLong(1);
                },
                accountId);

        if (lastSeq != null && eventSeq <= lastSeq) {
            jdbcTemplate.update("INSERT INTO ledger_consumer_processed (tx_id) VALUES (?)", txId);
            return;
        }

        if (lastSeq == null) {
            jdbcTemplate.update(
                    """
                            INSERT INTO ledger_account_projection (account_id, balance_cents, last_event_seq, last_tx_id)
                            VALUES (?, ?, ?, ?)
                            """,
                    accountId,
                    balanceAfterCents,
                    eventSeq,
                    txId);
        } else {
            int updated = jdbcTemplate.update(
                    """
                            UPDATE ledger_account_projection
                            SET balance_cents = ?, last_event_seq = ?, last_tx_id = ?
                            WHERE account_id = ? AND last_event_seq < ?
                            """,
                    balanceAfterCents,
                    eventSeq,
                    txId,
                    accountId,
                    eventSeq);
            if (updated == 0) {
                Long nowSeq = jdbcTemplate.query(
                        "SELECT last_event_seq FROM ledger_account_projection WHERE account_id = ?",
                        rs -> {
                            if (!rs.next()) {
                                return null;
                            }
                            return rs.getLong(1);
                        },
                        accountId);
                if (nowSeq != null && nowSeq >= eventSeq) {
                    jdbcTemplate.update("INSERT INTO ledger_consumer_processed (tx_id) VALUES (?)", txId);
                    return;
                }
                throw new IllegalStateException(
                        "Projection version conflict for account " + accountId + "; retry for at-least-once safety");
            }
        }

        jdbcTemplate.update("INSERT INTO ledger_consumer_processed (tx_id) VALUES (?)", txId);
    }
}
