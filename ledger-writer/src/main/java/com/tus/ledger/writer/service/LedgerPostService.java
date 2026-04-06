package com.tus.ledger.writer.service;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Service
public class LedgerPostService {

    private final JdbcTemplate jdbcTemplate;

    public LedgerPostService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Ledger posting in the system of record. Same transaction: lock per-account row, bump monotonic {@code event_seq},
     * insert immutable line with {@code balance_after}. This is what CDC will stream to Kafka.
     */
    @Transactional
    public Map<String, Object> postTransaction(String accountId, long amountCents) {
        String txId = UUID.randomUUID().toString();
        jdbcTemplate.update(
                """
                        INSERT INTO ledger_account_running (account_id, balance_cents, next_seq)
                        VALUES (?, 0, 0)
                        ON DUPLICATE KEY UPDATE account_id = account_id
                        """,
                accountId);

        Running running = jdbcTemplate.queryForObject(
                """
                        SELECT balance_cents, next_seq
                        FROM ledger_account_running
                        WHERE account_id = ?
                        FOR UPDATE
                        """,
                (rs, rowNum) -> new Running(rs.getLong(1), rs.getLong(2)),
                accountId);

        long nextSeq = running.nextSeq() + 1;
        long balanceAfter = running.balanceCents() + amountCents;

        jdbcTemplate.update(
                """
                        INSERT INTO ledger_transaction (account_id, tx_id, amount_cents, event_seq, balance_after)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                accountId,
                txId,
                amountCents,
                nextSeq,
                balanceAfter);

        jdbcTemplate.update(
                """
                        UPDATE ledger_account_running
                        SET balance_cents = ?, next_seq = ?
                        WHERE account_id = ?
                        """,
                balanceAfter,
                nextSeq,
                accountId);

        return Map.of(
                "txId", txId,
                "accountId", accountId,
                "eventSeq", nextSeq,
                "balanceAfterCents", balanceAfter);
    }

    private record Running(long balanceCents, long nextSeq) {}
}
