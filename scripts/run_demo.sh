#!/usr/bin/env bash
# End-to-end bring-up: infra + always-on streaming ingestion + web dashboard.
# Safe to re-run.
set -euo pipefail
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "### 1/5 Pulling base images (postgres, kafka, debezium connect) ..."
if ! docker compose pull postgres kafka connect; then
  echo
  echo "One or more image tags failed to resolve (registries occasionally prune old" >&2
  echo "tags). Check current tags on Docker Hub and override in a .env file --" >&2
  echo "see .env.example in this folder for the three overridable variables." >&2
  exit 1
fi

echo "### 1/5 Starting Postgres, Kafka, Kafka Connect, Spark (admin), Spark streamer, Dashboard ..."
docker compose up -d --build postgres kafka connect spark spark-streamer dashboard

echo "### 2/5 Registering the Debezium Postgres connector ..."
./scripts/register-connector.sh

echo "### 3/5 Seeding a few initial OLTP mutations ..."
docker compose run --rm generator --events 15 --interval 0.3

echo "### 4/5 Giving the streamer a moment to catch up (10s micro-batches) ..."
sleep 12

echo "### 5/5 Done."
cat <<'MSG'

Open the dashboard: http://localhost:8000

From there you can:
  - submit test inserts/updates/deletes and watch them flow Postgres -> silver -> gold
  - fire a random burst of mutations
  - run the Delta Deletion Vector compaction job
  - refresh the Spark-SQL gold_accounting_current view

CLI equivalents (all optional -- the streamer + dashboard already do this automatically).
Note the --packages flag: Delta/Kafka jars are resolved from Maven at spark-submit
time, not baked into the image -- see spark/jobs/spark_common.py for why it has
to be a CLI flag here rather than something set inside the job scripts.

  SPARK_PKGS="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

  docker compose run --rm generator --events 20   # --next-id-start is optional now; it auto-picks MAX(account_id)+1
  docker compose exec spark spark-submit --packages "$SPARK_PKGS" /opt/jobs/create_gold_view.py
  docker compose exec spark spark-submit --packages "$SPARK_PKGS" /opt/jobs/query_gold_view.py
  docker compose exec spark spark-submit --packages "$SPARK_PKGS" /opt/jobs/deletion_vector_compaction.py

Tear everything down (add -v to also wipe Postgres/Kafka/Delta data):
  docker compose down
MSG
