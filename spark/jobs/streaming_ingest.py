"""
Append-only CDC ingestion: Kafka (Debezium envelopes) -> Delta silver table.

Maps directly to whitepaper section 4, rows 1-2:
  - Writes exclusively via .format('delta') + outputMode('append'). No merge,
    no upsert, ever happens in this job.
  - Surrogate keys are xxhash64(account_id || source_commit_lsn): deterministic
    and idempotent across restarts, backfills, and duplicate/out-of-order
    redelivery -- unlike monotonically_increasing_id().

Run via:
  spark-submit --packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
      streaming_ingest.py --mode stream
(see spark_common.py for why --packages must be a spark-submit CLI flag).
"""
import argparse
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType

from spark_common import build_spark

KAFKA_TOPIC = "cdc.public.accounting"
SILVER_PATH = "/opt/data/silver/accounting_cdc_staging"
CHECKPOINT_PATH = "/opt/data/checkpoints/streaming_ingest"


def parse_debezium_envelope(df):
    """
    Debezium (JsonConverter, schemas disabled) emits a flat JSON payload per event:
      {"before": {...}|null, "after": {...}|null, "source": {"lsn": ..., "ts_ms": ...},
       "op": "c"|"u"|"d"|"r", "ts_ms": ...}
    op: c=create, u=update, d=delete, r=initial snapshot read.
    """
    row_schema = StructType([
        StructField("account_id", LongType()),
        StructField("current_balance", DoubleType()),
        StructField("currency", StringType()),
        StructField("transaction_timestamp", StringType()),
        StructField("updated_at", StringType()),
    ])
    source_schema = StructType([
        StructField("lsn", LongType()),
        StructField("ts_ms", LongType()),
    ])
    envelope_schema = StructType([
        StructField("before", row_schema),
        StructField("after", row_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType()),
    ])

    parsed = (
        df.select(
            F.col("value").cast("string").alias("raw_json"),
            F.col("offset").alias("kafka_event_offset"),
            F.col("partition").alias("kafka_partition"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("event", F.from_json(F.col("raw_json"), envelope_schema))
        .select(
            "raw_json",
            "kafka_event_offset",
            "kafka_partition",
            "kafka_timestamp",
            F.col("event.op").alias("operation_code"),
            F.coalesce(F.col("event.after.account_id"), F.col("event.before.account_id")).alias("account_id"),
            F.col("event.after.current_balance").alias("current_balance"),
            F.col("event.after.currency").alias("currency"),
            F.col("event.after.transaction_timestamp").alias("transaction_timestamp"),
            F.col("event.source.lsn").alias("source_commit_lsn"),
        )
    )

    parsed = parsed.withColumn(
        "operation_type",
        F.when(F.col("operation_code") == "c", "INSERT")
         .when(F.col("operation_code") == "r", "INSERT")
         .when(F.col("operation_code") == "u", "UPDATE")
         .when(F.col("operation_code") == "d", "DELETE")
         .otherwise("UNKNOWN"),
    )

    # Deterministic, idempotent surrogate key -- see module docstring.
    parsed = parsed.withColumn(
        "surrogate_key",
        F.xxhash64(F.concat_ws("|", F.col("account_id").cast("string"), F.col("source_commit_lsn").cast("string"))),
    )

    parsed = parsed.withColumn("ingested_at", F.current_timestamp())
    parsed = parsed.withColumn(
        "event_month",
        F.date_format(
            F.coalesce(F.col("transaction_timestamp").cast("timestamp"), F.col("ingested_at")),
            "yyyy-MM",
        ),
    )

    return parsed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["once", "stream"], default="once",
        help="'once' drains everything currently on Kafka and exits -- repeatable "
             "and easy to reason about. 'stream' (used by the spark-streamer "
             "service) runs continuously with 10s micro-batches.",
    )
    parser.add_argument("--kafka-bootstrap", default="cdc-kafka:9092")
    args = parser.parse_args()

    spark = build_spark("cdc-append-only-ingest")
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events = parse_debezium_envelope(raw)

    writer = (
        events.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")  # append-only: no merge, no upsert, ever
        .partitionBy("event_month")
    )

    if args.mode == "once":
        query = writer.trigger(availableNow=True).start(SILVER_PATH)
    else:
        query = writer.trigger(processingTime="10 seconds").start(SILVER_PATH)

    query.awaitTermination()


if __name__ == "__main__":
    sys.exit(main())
