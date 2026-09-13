"""
Isolated background compaction (whitepaper section 4, row 3): marks every
raw silver row that is NOT the latest event for its account_id as logically
deleted using Delta Lake Deletion Vectors. Because deletion vectors were
enabled on the table (create_gold_view.py), this DELETE writes small bitmap
files next to the existing Parquet files instead of rewriting them -- "soft
invalidation without Parquet rewrite."

Safe to run repeatedly. It does not change what gold_accounting_current
returns (that view was already filtering to the latest row); it only shrinks
what a raw scan over the silver table has to read, and demonstrates that
superseded/duplicate rows can be invalidated without Copy-on-Write.

Run via: spark-submit --packages <delta+kafka coords> deletion_vector_compaction.py
"""
from spark_common import build_spark


def main():
    spark = build_spark("cdc-deletion-vector-compaction")
    spark.sparkContext.setLogLevel("WARN")

    before = spark.sql("SELECT COUNT(*) c FROM silver_accounting_cdc_staging").first()["c"]

    spark.sql(
        """
        DELETE FROM silver_accounting_cdc_staging
        WHERE surrogate_key NOT IN (
            SELECT surrogate_key FROM (
                SELECT surrogate_key,
                       ROW_NUMBER() OVER (
                           PARTITION BY account_id
                           ORDER BY source_commit_lsn DESC, kafka_event_offset DESC
                       ) AS rn
                FROM silver_accounting_cdc_staging
            ) ranked
            WHERE rn = 1
        )
        """
    )

    after = spark.sql("SELECT COUNT(*) c FROM silver_accounting_cdc_staging").first()["c"]
    print(f"Compaction: {before} raw rows -> {after} remain visible "
          f"(superseded rows marked invalid via deletion vectors, no Parquet rewrite).")

    print("Recent table history (look for 'DELETE' with deletion-vector metrics, not a rewrite):")
    spark.sql("DESCRIBE HISTORY silver_accounting_cdc_staging") \
        .select("version", "timestamp", "operation", "operationMetrics") \
        .show(5, truncate=False)


if __name__ == "__main__":
    main()
