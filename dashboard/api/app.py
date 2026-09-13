"""
Monitoring + test-data dashboard API for the CDC lakehouse demo.

- Reads the Postgres `accounting` table directly (the OLTP "source of truth").
- Reads the Delta silver table straight off disk via the `deltalake` (delta-rs)
  library -- no Spark round trip needed for a fast dashboard refresh.
- Computes the "gold" read-time dedup view in pandas, mirroring exactly the
  SQL in spark/jobs/create_gold_view.py (whitepaper section 5).
- Proxies Debezium/Kafka Connect connector status.
- Lets the caller inject test mutations (insert/update/delete) straight into
  Postgres -- these flow through Debezium -> Kafka -> the always-on
  spark-streamer service -> the silver table automatically, usually within
  one ~10s micro-batch.
- Can trigger the one-off Spark admin jobs (deletion vector compaction, gold
  Spark-SQL view refresh) via `docker exec` into the `spark` container.
"""
import os
import random
import time
from datetime import datetime, timezone
from typing import Optional, Literal

import psycopg2
import psycopg2.extras
import requests
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

PG_PARAMS = dict(
    host=os.environ.get("PGHOST", "cdc-postgres"),
    port=os.environ.get("PGPORT", "5432"),
    dbname=os.environ.get("PGDATABASE", "sourcedb"),
    user=os.environ.get("PGUSER", "cdc_user"),
    password=os.environ.get("PGPASSWORD", "cdc_pass"),
)
CONNECT_URL = os.environ.get("CONNECT_URL", "http://cdc-connect:8083")
CONNECTOR_NAME = "accounting-postgres-connector"
SILVER_PATH = os.environ.get("SILVER_PATH", "/opt/data/silver/accounting_cdc_staging")
SPARK_CONTAINER = os.environ.get("SPARK_CONTAINER", "cdc-spark")
SPARK_STREAMER_CONTAINER = os.environ.get("SPARK_STREAMER_CONTAINER", "cdc-spark-streamer")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "cdc-kafka:9092")
KAFKA_TOPIC = "cdc.public.accounting"
SPARK_PACKAGES = "io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

app = FastAPI(title="CDC Lakehouse Demo Dashboard")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


# ---------------------------------------------------------------------------
# Postgres helpers
# ---------------------------------------------------------------------------

def pg_conn():
    conn = psycopg2.connect(**PG_PARAMS)
    conn.autocommit = True
    return conn


def pg_ok():
    try:
        conn = pg_conn()
        conn.close()
        return True
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Kafka / Docker helpers
# ---------------------------------------------------------------------------

def kafka_status():
    try:
        from kafka import KafkaConsumer

        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            request_timeout_ms=3000,
            api_version_auto_timeout_ms=3000,
        )
        topics = sorted(consumer.topics())
        consumer.close()
        return {
            "reachable": True,
            "topic_exists": KAFKA_TOPIC in topics,
            "topic_count": len(topics),
        }
    except Exception as exc:
        return {"reachable": False, "error": str(exc)}


def _docker_client():
    import docker

    return docker.DockerClient(base_url="unix://var/run/docker.sock")


def container_status(name: str):
    try:
        client = _docker_client()
        container = client.containers.get(name)
        return {"reachable": True, "status": container.status}  # "running", "exited", ...
    except Exception as exc:
        return {"reachable": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# API models
# ---------------------------------------------------------------------------

class MutationRequest(BaseModel):
    action: Literal["insert", "update", "delete"]
    account_id: int
    balance: Optional[float] = None
    currency: Optional[str] = "USD"


class GenerateRequest(BaseModel):
    events: int = 10
    next_id_start: Optional[int] = None


# ---------------------------------------------------------------------------
# Health / status
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health():
    """
    One-shot status for every stage of the pipeline, in order, for the
    dashboard's "Pipeline steps" panel:
      Postgres -> Debezium (Kafka Connect) -> Kafka -> Spark streamer -> Silver -> Gold
    """
    postgres_up = pg_ok()

    connect_status = {"reachable": False}
    try:
        r = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=3)
        if r.status_code == 200:
            body = r.json()
            connect_status = {
                "reachable": True,
                "connector_state": body.get("connector", {}).get("state"),
                "tasks": [t.get("state") for t in body.get("tasks", [])],
            }
        else:
            connect_status = {"reachable": True, "connector_state": "NOT_FOUND", "http_status": r.status_code}
    except Exception as exc:
        connect_status = {"reachable": False, "error": str(exc)}

    kafka = kafka_status()
    streamer = container_status(SPARK_STREAMER_CONTAINER)

    silver_exists = os.path.isdir(os.path.join(SILVER_PATH, "_delta_log"))
    silver_count = None
    gold_count = None
    if silver_exists:
        try:
            df = _load_silver_pandas()
            if df is not None:
                silver_count = int(len(df))
                gold_count = int(len(_gold_from_silver(df)))
        except Exception:
            pass  # counts are a nice-to-have; don't fail health over them

    return {
        "postgres_up": postgres_up,
        "kafka_connect": connect_status,
        "kafka": kafka,
        "spark_streamer": streamer,
        "silver_table_exists": silver_exists,
        "silver_count": silver_count,
        "gold_count": gold_count,
        "server_time": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Source (Postgres OLTP table)
# ---------------------------------------------------------------------------

@app.get("/api/source")
def get_source():
    try:
        conn = pg_conn()
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT account_id, current_balance, currency, transaction_timestamp, updated_at
                FROM accounting
                ORDER BY account_id
                """
            )
            rows = cur.fetchall()
        conn.close()
        return {"rows": [dict(r) for r in rows]}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Postgres unavailable: {exc}")


# ---------------------------------------------------------------------------
# Silver (raw, append-only Delta table) + Gold (read-time dedup)
# ---------------------------------------------------------------------------

def _load_silver_pandas():
    from deltalake import DeltaTable

    if not os.path.isdir(os.path.join(SILVER_PATH, "_delta_log")):
        return None
    dt = DeltaTable(SILVER_PATH)
    df = dt.to_pandas()
    return df


@app.get("/api/silver")
def get_silver(limit: int = 100):
    df = _load_silver_pandas()
    if df is None or df.empty:
        return {"total_count": 0, "rows": []}

    df = df.sort_values("ingested_at", ascending=False)
    cols = [
        "surrogate_key", "account_id", "operation_type", "current_balance",
        "currency", "transaction_timestamp", "source_commit_lsn",
        "kafka_event_offset", "event_month", "ingested_at",
    ]
    cols = [c for c in cols if c in df.columns]
    preview = df[cols].head(limit)
    return {
        "total_count": int(len(df)),
        "rows": preview.astype(object).where(preview.notnull(), None).to_dict(orient="records"),
    }


def _gold_from_silver(df):
    """
    Mirrors spark/jobs/create_gold_view.py's SQL exactly, computed here in
    pandas so the dashboard doesn't need a live Spark session to serve reads:

        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY account_id ORDER BY source_commit_lsn DESC, kafka_event_offset DESC
            ) rn FROM silver_accounting_cdc_staging
        ) WHERE rn = 1 AND operation_type != 'DELETE'
    """
    if df is None or df.empty:
        return df.iloc[0:0] if df is not None else None
    ranked = df.sort_values(["source_commit_lsn", "kafka_event_offset"], ascending=[False, False])
    latest = ranked.groupby("account_id", as_index=False).first()
    return latest[latest["operation_type"] != "DELETE"]


@app.get("/api/gold")
def get_gold():
    df = _load_silver_pandas()
    latest = _gold_from_silver(df)
    if latest is None or latest.empty:
        return {"rows": []}

    cols = [
        "surrogate_key", "account_id", "current_balance", "currency",
        "transaction_timestamp", "source_commit_lsn", "operation_type",
    ]
    cols = [c for c in cols if c in latest.columns]
    latest = latest[cols].sort_values("account_id")
    return {"rows": latest.astype(object).where(latest.notnull(), None).to_dict(orient="records")}


# ---------------------------------------------------------------------------
# Test-data injection (writes straight to Postgres; Debezium/Kafka/Spark pick
# it up automatically from there)
# ---------------------------------------------------------------------------

@app.post("/api/mutate")
def mutate(req: MutationRequest):
    try:
        conn = pg_conn()
        with conn.cursor() as cur:
            if req.action == "insert":
                if req.balance is None:
                    raise HTTPException(400, "balance is required for insert")
                cur.execute(
                    """
                    INSERT INTO accounting (account_id, current_balance, currency, transaction_timestamp, updated_at)
                    VALUES (%s, %s, %s, now(), now())
                    ON CONFLICT (account_id) DO UPDATE
                        SET current_balance = EXCLUDED.current_balance,
                            currency = EXCLUDED.currency,
                            transaction_timestamp = now(),
                            updated_at = now()
                    """,
                    (req.account_id, req.balance, req.currency or "USD"),
                )
            elif req.action == "update":
                if req.balance is None:
                    raise HTTPException(400, "balance is required for update")
                cur.execute(
                    """
                    UPDATE accounting
                    SET current_balance = %s, transaction_timestamp = now(), updated_at = now()
                    WHERE account_id = %s
                    """,
                    (req.balance, req.account_id),
                )
                if cur.rowcount == 0:
                    raise HTTPException(404, f"account_id {req.account_id} not found")
            elif req.action == "delete":
                cur.execute("DELETE FROM accounting WHERE account_id = %s", (req.account_id,))
                if cur.rowcount == 0:
                    raise HTTPException(404, f"account_id {req.account_id} not found")
        conn.close()
        return {"ok": True, "message": f"{req.action} committed to Postgres for account_id={req.account_id}. "
                                        f"It will appear in the silver table within one streaming micro-batch (~10s)."}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Postgres error: {exc}")


@app.post("/api/generate")
def generate(req: GenerateRequest):
    """Fires a small burst of random inserts/updates/deletes, for a quick demo."""
    events = max(1, min(req.events, 200))
    try:
        conn = pg_conn()
        next_id = req.next_id_start
        results = []
        with conn.cursor() as cur:
            for i in range(events):
                cur.execute("SELECT account_id FROM accounting")
                existing = [r[0] for r in cur.fetchall()]
                action = random.choices(
                    ["insert", "update", "update", "update", "delete"],
                    weights=[3, 5, 5, 5, 1],
                )[0]
                if action == "insert" or not existing:
                    if next_id is None:
                        next_id = (max(existing) + 1) if existing else 2000
                    # Retry on collision (e.g. a manual mutate() or the CLI
                    # generator inserted this id concurrently) instead of
                    # crashing the whole burst with a 503.
                    for _ in range(20):
                        cur.execute(
                            """
                            INSERT INTO accounting (account_id, current_balance, currency, transaction_timestamp, updated_at)
                            VALUES (%s, %s, 'USD', now(), now())
                            ON CONFLICT (account_id) DO NOTHING
                            """,
                            (next_id, round(random.uniform(0, 5000), 2)),
                        )
                        if cur.rowcount > 0:
                            results.append({"action": "insert", "account_id": next_id})
                            next_id += 1
                            break
                        next_id += 1
                elif action == "delete" and len(existing) > 3:
                    target = random.choice(existing)
                    cur.execute("DELETE FROM accounting WHERE account_id = %s", (target,))
                    results.append({"action": "delete", "account_id": target})
                else:
                    target = random.choice(existing)
                    delta = round(random.uniform(-200, 200), 2)
                    cur.execute(
                        """
                        UPDATE accounting
                        SET current_balance = current_balance + %s, transaction_timestamp = now(), updated_at = now()
                        WHERE account_id = %s
                        """,
                        (delta, target),
                    )
                    results.append({"action": "update", "account_id": target})
        conn.close()
        return {"ok": True, "mutations": results}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Postgres error: {exc}")


# ---------------------------------------------------------------------------
# Admin actions that need the real Delta/Spark engine (deletion vectors, the
# Spark-SQL gold view for interactive spark-sql use). Executed via `docker
# exec` into the long-lived `spark` container.
# ---------------------------------------------------------------------------

def _docker_exec_spark_job(job_path: str, extra_args=None):
    try:
        client = _docker_client()
        container = client.containers.get(SPARK_CONTAINER)
    except Exception as exc:
        raise HTTPException(503, f"Cannot reach Docker / '{SPARK_CONTAINER}' container: {exc}")

    # --packages must be a spark-submit CLI flag, not a .config() call inside
    # the job script -- see spark/jobs/spark_common.py for why.
    cmd = [
        "spark-submit",
        "--packages", SPARK_PACKAGES,
        "--conf", "spark.jars.ivy=/opt/ivy_cache",
        job_path,
    ] + (extra_args or [])
    start = time.time()
    exec_result = container.exec_run(cmd, demux=True)
    duration = round(time.time() - start, 1)
    stdout, stderr = exec_result.output
    stdout = (stdout or b"").decode(errors="replace")
    stderr = (stderr or b"").decode(errors="replace")

    def tail(s, n=6000):
        return s[-n:]

    return {
        "ok": exec_result.exit_code == 0,
        "exit_code": exec_result.exit_code,
        "duration_seconds": duration,
        "stdout_tail": tail(stdout),
        "stderr_tail": tail(stderr),
    }


@app.post("/api/actions/compact")
def run_compaction():
    """Runs the Delta Deletion Vector compaction job (see spark/jobs/deletion_vector_compaction.py)."""
    return _docker_exec_spark_job("/opt/jobs/deletion_vector_compaction.py")


@app.post("/api/actions/refresh-spark-gold-view")
def refresh_spark_gold_view():
    """(Re)creates the Spark-SQL `gold_accounting_current` view for interactive spark-sql use."""
    return _docker_exec_spark_job("/opt/jobs/create_gold_view.py")


# ---------------------------------------------------------------------------
# Static frontend
# ---------------------------------------------------------------------------

STATIC_DIR = "/app/static"
app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="static")
