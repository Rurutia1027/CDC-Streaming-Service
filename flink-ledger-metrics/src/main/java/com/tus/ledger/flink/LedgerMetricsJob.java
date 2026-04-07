package com.tus.ledger.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Flink streaming path: same Debezium topic, different consumer group, sink to ClickHouse for analytics.
 */
public final class LedgerMetricsJob {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private LedgerMetricsJob() {
    }

    public static void main(String[] args) throws Exception {
        String kafka = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = env("LEDGER_CDC_TOPIC", "ledger_finance.ledger_db.ledger_transaction");
        String jdbcUrl = env("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/analytics");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafka)
                .setTopics(topic)
                .setGroupId("flink-ledger-metrics")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "cdc-kafka")
                .flatMap(new DebeziumInsertFlatMap())
                .addSink(
                        JdbcSink.sink(
                                """
                                        INSERT INTO analytics.ledger_tx_events
                                        (source, tx_id, account_id, amount_cents, event_seq, balance_after)
                                        VALUES (?, ?, ?, ?, ?, ?)
                                        """,
                                (statement, row) -> {
                                    statement.setString(1, row.f0);
                                    statement.setString(2, row.f1);
                                    statement.setString(3, row.f2);
                                    statement.setLong(4, row.f3);
                                    statement.setLong(5, row.f4);
                                    statement.setLong(6, row.f5);
                                },
                                JdbcExecutionOptions.builder()
                                        .withBatchSize(50)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(3)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(jdbcUrl)
                                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                        .build()));

        env.execute("ledger-metrics-flink");
    }

    private static String env(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }

    private static String text(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return v == null || v.isNull() ? "" : v.asText();
    }

    private static final class DebeziumInsertFlatMap
            implements FlatMapFunction<String, Tuple6<String, String, String, Long, Long, Long>> {

        @Override
        public void flatMap(String value, Collector<Tuple6<String, String, String, Long, Long, Long>> out)
                throws Exception {
            JsonNode root = MAPPER.readTree(value);
            JsonNode payload = root.path("payload");
            String op = payload.path("op").asText("");
            if (!"c".equals(op) && !"r".equals(op)) {
                return;
            }
            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) {
                return;
            }
            out.collect(
                    Tuple6.of(
                            "flink",
                            text(after, "tx_id"),
                            text(after, "account_id"),
                            after.path("amount_cents").asLong(),
                            after.path("event_seq").asLong(),
                            after.path("balance_after").asLong()));
        }
    }
}
