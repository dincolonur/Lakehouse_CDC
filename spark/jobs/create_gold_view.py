"""
Registers the silver Delta table + the "gold" dynamic reconciliation view in a
local Hive metastore (backed by files under /opt/data/spark-warehouse) so both
persist across separate spark-submit invocations, letting query_gold_view.py
and deletion_vector_compaction.py see the same catalog.

Maps to whitepaper section 5. OSS Apache Spark SQL does not support the
QUALIFY clause the whitepaper's Databricks-flavored SQL uses, so this uses the
equivalent ROW_NUMBER()+subquery form -- functionally identical, and still
resolved entirely at read time against the append-only silver table.

Run via: spark-submit --packages <delta+kafka coords> create_gold_view.py
(see spark_common.py for why --packages must be a CLI flag, not a .config()
call in this file).
"""
from spark_common import build_spark

SILVER_PATH = "/opt/data/silver/accounting_cdc_staging"

GOLD_VIEW_SQL = """
CREATE OR REPLACE VIEW gold_accounting_current AS
SELECT surrogate_key, account_id, current_balance, currency,
       transaction_timestamp, source_commit_lsn, operation_type
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY account_id
               ORDER BY source_commit_lsn DESC, kafka_event_offset DESC
           ) AS rn
    FROM silver_accounting_cdc_staging
) ranked
WHERE rn = 1
  AND operation_type != 'DELETE'
"""


def main():
    spark = build_spark("cdc-gold-view")
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_accounting_cdc_staging
        USING DELTA
        LOCATION '{SILVER_PATH}'
    """)

    # Enable Deletion Vectors so the compaction job can soft-invalidate
    # superseded rows without rewriting Parquet files (whitepaper section 4,
    # row 3). See spark/jobs/deletion_vector_compaction.py.
    spark.sql("""
        ALTER TABLE silver_accounting_cdc_staging
        SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
    """)

    spark.sql(GOLD_VIEW_SQL)

    print("gold_accounting_current view created/refreshed. Preview:")
    spark.sql("SELECT * FROM gold_accounting_current ORDER BY account_id").show(50, truncate=False)


if __name__ == "__main__":
    main()
