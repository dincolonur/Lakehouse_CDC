"""
Prints the raw silver staging table (append-only, may contain many rows per
account_id -- inserts, updates, deletes, and any duplicate/late redelivery)
side by side with the gold_accounting_current view (one row per account,
resolved at read time). Run this after streaming_ingest.py and
create_gold_view.py.

Run via: spark-submit --packages <delta+kafka coords> query_gold_view.py
"""
from spark_common import build_spark


def main():
    spark = build_spark("cdc-query-gold")
    spark.sparkContext.setLogLevel("WARN")

    print("=== Raw silver staging (append-only: every INSERT/UPDATE/DELETE event) ===")
    total = spark.sql("SELECT COUNT(*) c FROM silver_accounting_cdc_staging").first()["c"]
    print(f"Total raw events currently in silver table: {total}")
    spark.sql(
        """
        SELECT account_id, operation_type, COUNT(*) AS event_count
        FROM silver_accounting_cdc_staging
        GROUP BY account_id, operation_type
        ORDER BY account_id, operation_type
        """
    ).show(200, truncate=False)

    print("=== Gold view: one current row per account, resolved at read time (no rewrite) ===")
    spark.sql("SELECT * FROM gold_accounting_current ORDER BY account_id").show(50, truncate=False)


if __name__ == "__main__":
    main()
