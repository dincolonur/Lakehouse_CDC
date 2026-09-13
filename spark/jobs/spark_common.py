"""
Shared SparkSession builder for the admin/streaming jobs in this folder.

IMPORTANT gotcha this file exists to avoid repeating: Delta Lake and the
Kafka connector are NOT installed as pip packages inside the Spark image --
they're pulled from Maven via `spark-submit --packages ...` on the command
line (see scripts/run_demo.sh, docker-compose.yml's spark-streamer command,
and dashboard/api/app.py). That is NOT optional or interchangeable with
setting `spark.jars.packages` / `spark.jars.ivy` here via `.config(...)`:
when a script is launched with `spark-submit script.py`, the driver JVM
(and its classpath) is already started by the `spark-submit` shell script
before your Python code runs, so any `.config("spark.jars.packages", ...)`
call made from inside the script has no effect -- the jars simply never get
downloaded, and you get a `ClassNotFoundException` for Delta classes at
first use (this bit us once while building this demo). Session-level SQL
configs like `spark.sql.extensions` and `spark.sql.catalog.spark_catalog`
*do* work fine from `.config()` here, because those are read when the SQL
session/catalog is initialized inside the already-running JVM, not at
classpath-loading time -- that distinction is why this file only sets those.
"""
from pyspark.sql import SparkSession

WAREHOUSE_DIR = "/opt/data/spark-warehouse"


def build_spark(app_name: str, extra_conf: dict | None = None) -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .enableHiveSupport()
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
