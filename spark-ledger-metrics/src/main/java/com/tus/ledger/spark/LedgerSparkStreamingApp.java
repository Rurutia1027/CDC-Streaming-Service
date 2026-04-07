package com.tus.ledger.spark;

import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.lit;

import java.util.concurrent.TimeoutException;

/**
 * Spark Structured Streaming (Java): parallel consumer group → JDBC append to ClickHouse.
 * Spark runtime is provided by {@code spark-submit} in Docker; this JAR carries app + ClickHouse JDBC only.
 */
public final class LedgerSparkStreamingApp {

    private LedgerSparkStreamingApp() {}

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        String kafka = getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = getenv("LEDGER_CDC_TOPIC", "ledger_finance.ledger_db.ledger_transaction");
        String chUrl = getenv("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/analytics");
        String checkpoint = getenv("SPARK_CHECKPOINT", "/tmp/spark-ledger-checkpoint");

        SparkSession spark =
                SparkSession.builder().appName("ledger-metrics-spark").getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        Dataset<Row> raw =
                spark.readStream()
                        .format("kafka")
                        .option("kafka.bootstrap.servers", kafka)
                        .option("subscribe", topic)
                        .option("startingOffsets", "earliest")
                        .load();

        Dataset<Row> v = raw.selectExpr("CAST(value AS STRING) AS v");
        Dataset<Row> filtered =
                v.filter(get_json_object(col("v"), "$.payload.op").isin("c", "r"));

        Dataset<Row> parsed =
                filtered.select(
                        lit("spark").alias("source"),
                        get_json_object(col("v"), "$.payload.after.tx_id").alias("tx_id"),
                        get_json_object(col("v"), "$.payload.after.account_id").alias("account_id"),
                        get_json_object(col("v"), "$.payload.after.amount_cents").cast("long").alias("amount_cents"),
                        get_json_object(col("v"), "$.payload.after.event_seq").cast("long").alias("event_seq"),
                        get_json_object(col("v"), "$.payload.after.balance_after").cast("long").alias("balance_after"));

        VoidFunction2<Dataset<Row>, Long> sink =
                new VoidFunction2<Dataset<Row>, Long>() {
                    @Override
                    public void call(Dataset<Row> batch, Long batchId) throws Exception {
                        batch.write()
                                .format("jdbc")
                                .option("url", chUrl)
                                .option("dbtable", "ledger_tx_events")
                                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                                .mode("append")
                                .save();
                    }
                };

        StreamingQuery query =
                parsed.writeStream().foreachBatch(sink).option("checkpointLocation", checkpoint).start();

        query.awaitTermination();
    }

    private static String getenv(String key, String defaultValue) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? defaultValue : v;
    }
}
