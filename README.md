# High-Throughput CDC Lakehouse — Local Demo

A runnable, small-scale build of the architecture described in
`Bonus_Lakehouse_CDC_Architecture.pdf` — **write-optimized append-only
staging + read-time reconciliation**, instead of continuous `MERGE INTO` —
plus a web dashboard that shows every stage of the pipeline live and lets
you inject test data and watch it flow through end to end.

This README explains what's actually running, why it's built this way, how
to use the dashboard, and how to fix it if something breaks (four real
issues came up while building this — they're documented below so you don't
have to re-diagnose them).

## Contents

1. [The problem this solves](#1-the-problem-this-solves)
2. [What's actually running](#2-whats-actually-running)
3. [Quick start](#3-quick-start)
   - [Starting, stopping, and resetting the stack](#starting-stopping-and-resetting-the-stack)
4. [Using the dashboard](#4-using-the-dashboard)
5. [Manual walkthrough (no dashboard, all CLI)](#5-manual-walkthrough-no-dashboard-all-cli)
6. [Things that broke while building this (and the actual fixes)](#6-things-that-broke-while-building-this-and-the-actual-fixes)
7. [Simplifications vs. the whitepaper (and why)](#7-simplifications-vs-the-whitepaper-and-why)
8. [Where things live](#8-where-things-live)
9. [Inspecting things directly](#9-inspecting-things-directly)
10. [License](#10-license)
11. [Source](#11-source)

---

## 1. The problem this solves

Modern systems need Postgres/MySQL changes to show up in an analytical
Lakehouse table (Delta Lake or Iceberg) within minutes, at very high volume,
without corrupting history and without falling over. The obvious approach —
run `MERGE INTO` on every micro-batch to upsert changed rows — breaks down
at scale for three concrete reasons, all called out in the source whitepaper:

1. **Write amplification.** Delta/Iceberg tables are made of large Parquet
   files (Copy-on-Write). Changing one row means rewriting the whole file it
   lives in. At millions of updates a day against multi-terabyte tables, this
   is catastrophic I/O and contends with anyone trying to read the table.
2. **Out-of-order and duplicate events.** Kafka consumer rebalances and
   network partitions mean CDC events can arrive late or twice. A naive
   "last write wins by wall-clock time" merge gets this wrong.
3. **No external state store allowed.** You can't just keep a Redis/RocksDB
   deduplication cache — many environments (cost, air-gapped security)
   forbid it, and it doesn't survive job restarts cleanly anyway.


The whitepaper's answer, which this repo actually implements:

- **Never update or delete rows in the hot path.** Every CDC event —
  insert, update, or delete — is *appended* to a Delta table. Appends don't
  rewrite existing files, so there's no write amplification and no lock
  contention with readers.
- **Deduplicate at read time, not write time.** A view (or, here, an
  equivalent pandas computation) picks the latest row per business key using
  a window function. No external cache needed — the "state" is just the
  data that's already in the table.
- **Make keys deterministic, not sequential.** Instead of an
  auto-incrementing ID (which depends on partition layout and job history,
  and breaks under replay), every row's surrogate key is a hash of
  `(business_key, source_commit_lsn)`. The same underlying database change
  always produces the same key, no matter how many times or in what order it
  gets redelivered — which is exactly what makes duplicates and replays
  harmless.
- **"Delete" without deleting.** A row marked for hard deletion isn't
  removed; it's appended as a `DELETE` event, and the read-time view simply
  filters it out. The audit trail stays intact for compliance.

## 2. What's actually running

```
┌────────────┐   WAL    ┌──────────┐  publish  ┌───────┐  consume   ┌────────────────┐
│  Postgres  │ ───────► │ Debezium │ ────────► │ Kafka │ ─────────► │ Spark streamer  │
│ (accounting)│         │ (Connect)│           │       │            │ (append-only)   │
└────────────┘          └──────────┘           └───────┘            └────────┬────────┘
                                                                              │ append
                                                                              ▼
                                                                    ┌────────────────────┐
                                                                    │ Delta silver table  │
                                                                    │ (every raw event)   │
                                                                    └─────────┬──────────┘
                                                          read-time dedup     │
                                                     (ROW_NUMBER over key)    ▼
                                                                    ┌────────────────────┐
                                                                    │ Gold: one row/acct  │
                                                                    └────────────────────┘

                          ▲ inject test data                     ▲ watch it flow through
                          └──────────────── Web dashboard (localhost:8000) ─────────────┘
```

| # | Component | What it does here | Where |
|---|---|---|---|
| 1 | **Postgres** | The OLTP system of record. A single `accounting` table (`account_id`, `current_balance`, ...) stands in for the whitepaper's 4-billion-row table. Logical replication (`wal_level=logical`) is turned on so Debezium can read the write-ahead log. | `postgres/init.sql` |
| 2 | **Debezium (Kafka Connect)** | Subscribes to Postgres's replication slot and publishes one JSON event per row change (insert/update/delete) to Kafka. This is the industry-standard way to do CDC without touching application code. | `debezium/postgres-connector.json`, registered by `scripts/register-connector.sh` |
| 3 | **Kafka** | The durable, replayable log between the database and the lakehouse. In production this is what absorbs 48-hour-late redeliveries and consumer rebalances; here it's a single-broker KRaft (no Zookeeper) instance. | `docker-compose.yml` (`kafka` service) |
| 4 | **Spark streamer** | A Spark Structured Streaming job that reads Kafka continuously (10-second micro-batches) and appends every event into the Delta table — never merges, never updates. This is what makes the whole thing safe against duplicates: append-only writes can't corrupt anything. | `spark/jobs/streaming_ingest.py`, run continuously by the `spark-streamer` service |
| 5 | **Delta silver table** | The append-only, immutable audit trail. Contains every raw event ever seen, including duplicates and out-of-order arrivals, each with a deterministic `surrogate_key = xxhash64(account_id, source_commit_lsn)`. | `data/silver/accounting_cdc_staging/` |
| 6 | **Gold view** | Resolves "what does this account look like *right now*" by taking, per `account_id`, the row with the highest `source_commit_lsn` (ties broken by Kafka offset), and dropping rows whose latest event was a delete. Computed fresh on every read — nothing is pre-materialized or mutated to get here. | `spark/jobs/create_gold_view.py` (Spark SQL) and `dashboard/api/app.py::_gold_from_silver` (equivalent pandas logic used by the dashboard) |
| 7 | **Deletion vector compaction** | An optional background job that soft-invalidates rows that are no longer anyone's "latest" event, using Delta Lake's Deletion Vectors feature — a bitmap file marking rows as gone, written *next to* the existing Parquet files rather than rewriting them. This is the safe way to reclaim space without paying the Copy-on-Write cost. | `spark/jobs/deletion_vector_compaction.py` |
| 8 | **Web dashboard** | Everything above, visualized: live status of every stage, a form to inject test mutations, and buttons to trigger the admin jobs. See section 4. | `dashboard/` |

## 3. Quick start

**Prerequisites**: Docker Desktop running. Every image here is multi-arch
(works on Apple Silicon and Intel): `postgres:16`, `apache/kafka:3.8.0`,
`quay.io/debezium/connect:latest`, and two images built locally from
`eclipse-temurin:17-jre-jammy` + `python:3.11-slim`.

```bash
cd Lakehouse_CDC
./scripts/run_demo.sh
```

This pulls the base images, starts Postgres/Kafka/Connect, builds and starts
the Spark containers and the dashboard, registers the Debezium connector,
and seeds a few starter mutations. Then open:

**http://localhost:8000**

First run takes longer than subsequent ones — building the Spark image (pip
install) and the first `spark-submit` (downloading the Delta/Kafka jars from
Maven, cached afterwards in a Docker volume) both take real time. Expect
1-3 minutes on first run, seconds after that.

> **If an image tag ever fails to resolve** ("manifest unknown"): registries
> occasionally prune old tags (this happened twice while building this demo
> — see section 6). Copy `.env.example` to `.env`, find the current tag on
> Docker Hub / Quay.io, and override it there — no need to touch
> `docker-compose.yml`.

### Starting, stopping, and resetting the stack

`./scripts/run_demo.sh` is `docker compose up` plus the one-time setup steps
(connector registration, seed data). Once it's up, these are the commands
you'll actually reach for day to day:

| Command | What it does | Data? | When to use it |
|---|---|---|---|
| `docker compose ps` | Lists what's running and its health | — | Check status |
| `docker compose logs -f <service>` | Tails a service's logs (e.g. `spark-streamer`, `connect`) | — | Debugging |
| `docker compose stop` | Stops all containers but leaves them in place | kept | Pausing for a break — fastest to resume, no rebuild |
| `docker compose start` | Resumes containers stopped with `stop` | kept | Resuming after `stop` |
| `docker compose up -d` | Starts everything (builds images if needed); safe to re-run | kept | First start, or after a reboot / `stop` |
| `docker compose down` | Stops **and removes** the containers | kept (named volumes survive) | Clean shutdown when you're done for now |
| `docker compose down -v` | Stops, removes containers, **and deletes the volumes** | **wiped** | Full reset — Postgres, Kafka, and the Delta tables under `data/` all start empty next time |

The distinction that trips people up: `down` removes the *containers*, not
the *data* — Postgres's rows, Kafka's log, and the Delta silver table all
live in named volumes (`pg_data`, `kafka_data`) or the bind-mounted `data/`
folder, which `down` alone doesn't touch. Only `-v` wipes them. After a
plain `down`, running `./scripts/run_demo.sh` again (or `docker compose up
-d ...`) picks up right where you left off; after `down -v`, it starts from
a genuinely empty database and an empty Delta table.

## 4. Using the dashboard

Open **http://localhost:8000**. Auto-refresh is on by default (every 5s).

### Pipeline panel

Six cards, left to right, matching the diagram in section 2: **Postgres →
Debezium → Kafka → Spark streamer → Silver → Gold**. Each has a status dot:

- **Green** = healthy (Postgres reachable, Debezium connector `RUNNING`,
  Kafka reachable with the topic present, Spark streamer container
  `running`, silver/gold tables populated).
- **Amber** = reachable but not fully ready yet (e.g. connector still
  starting, silver table not created yet because no data has flowed
  through).
- **Red** = unreachable — check `docker compose ps` and `docker compose logs
  <service>` for that stage.

This panel is the fastest way to answer "where is my data right now" —
watch the Silver and Gold counts change as you submit mutations below.

### 1 · Inject test data

Pick Insert / Update / Delete, an `account_id`, and (for insert/update) a
balance, then submit. This writes directly to Postgres — exactly like a real
application would. From there it's entirely automatic:

1. Debezium notices the change in Postgres's write-ahead log (sub-second).
2. It publishes a CDC event to the `cdc.public.accounting` Kafka topic.
3. The always-on Spark streamer picks it up on its next 10-second
   micro-batch and appends it to the silver Delta table.
4. The dashboard's auto-refresh (or a manual "Refresh now") shows it in the
   Silver panel, and — if it's the latest event for that account — in Gold.

Watch the Silver row count go up immediately-ish and the Gold panel update
to reflect the new balance. There's also a "Generate burst" button that
fires N random inserts/updates/deletes at once, useful for seeing the silver
table grow and the gold view stay correct under churn.

### 2 · Pipeline admin actions

These run the real Spark/Delta jobs via `docker exec` into the `spark`
container (separate from the always-on streamer):

- **Run deletion-vector compaction** — soft-invalidates every silver row
  that isn't the current "latest" for its account, using Delta Deletion
  Vectors (no Parquet rewrite). Safe to run repeatedly; doesn't change what
  Gold shows.
- **Refresh Spark-SQL gold view** — (re)creates the `gold_accounting_current`
  Spark SQL view in a local Hive metastore, for anyone who wants to poke at
  it with `spark-sql` directly rather than through the dashboard's own
  pandas-computed Gold panel.

Both stream back real stdout/stderr and can take 10-30 seconds the first
time while Maven dependencies download (see section 6 for why this needs a
`--packages` flag at all).

### Three data panels

- **Source** — the Postgres `accounting` table as it exists right now.
  Ground truth.
- **Silver** — every raw CDC event, append-only. Expect to see multiple rows
  per `account_id` here (every update is a new row) — that's the point.
- **Gold** — one row per `account_id`, computed fresh on every load by
  ranking silver rows per account by `(source_commit_lsn, kafka_event_offset)`
  descending and taking the top one, then dropping deletes. This is the
  "current state" a downstream analytics query would read.

### Security note

The dashboard container is unauthenticated and mounts your Docker socket
(`/var/run/docker.sock`) so its admin buttons can `docker exec` into the
Spark container. That's a reasonable simplification for a local demo bound
to `localhost` — don't expose port 8000 beyond your machine.

## 5. Manual walkthrough (no dashboard, all CLI)

```bash
# 1. Infra + always-on streamer
docker compose up -d --build postgres kafka connect spark spark-streamer

# 2. Point Debezium at Postgres
./scripts/register-connector.sh

# 3. Simulate OLTP traffic
docker compose run --rm generator --events 30 --interval 0.3

# The streamer picks these up automatically within ~10s. Delta/Kafka jars are
# resolved from Maven via --packages -- see section 6 for why this can't be
# baked into the script or the image.
SPARK_PKGS="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

# 4. (optional) Create/refresh the Spark-SQL dedup view for spark-sql/notebook use
docker compose exec spark spark-submit --packages "$SPARK_PKGS" /opt/jobs/create_gold_view.py

# 5. Compare raw silver (has duplicates/updates) vs. gold (one row/account)
docker compose exec spark spark-submit --packages "$SPARK_PKGS" /opt/jobs/query_gold_view.py

# 6. Soft-invalidate superseded rows via Delta Deletion Vectors
docker compose exec spark spark-submit --packages "$SPARK_PKGS" /opt/jobs/deletion_vector_compaction.py
```

See "Starting, stopping, and resetting the stack" in section 3 for the
full `up` / `stop` / `down` / `down -v` breakdown.

## 6. Things that broke while building this (and the actual fixes)

Real registries drift and real Spark deployment modes have real gotchas.
Documenting these so a future `docker compose up` failure is a two-minute
fix, not a re-investigation:

**`bitnami/kafka:3.7` — manifest not found.** Bitnami pruned most of their
simple version-tagged images from Docker Hub in 2025. Fixed by switching to
`apache/kafka:3.8.0`, the Kafka project's own official multi-arch image.

**`debezium/connect:2.7` and even `debezium/connect:latest` — manifest not
found.** The `debezium/connect` Docker Hub repository turned out to be
unreliable entirely, not just missing a tag. Debezium's authoritative
registry is Quay.io. Fixed by switching to `quay.io/debezium/connect:latest`.

**`apache/spark-py:v3.5.1` — not found.** Guessed at a vendor tagging
convention that didn't hold. Fixed by dropping the vendor image entirely:
the Spark image here is built from `eclipse-temurin:17-jre-jammy` (a
long-lived, standard OpenJDK base) with `pip install pyspark==3.5.1` — a
PyPI version pin is unambiguous in a way a Docker Hub tag apparently isn't.

**`ClassNotFoundException: io.delta.sql.DeltaSparkSessionExtension` when
running a job.** This one's a real PySpark deployment-mode gotcha, not a
registry problem. `configure_spark_with_delta_pip()` (and any
`.config("spark.jars.packages", ...)` call) works fine when Python launches
its own JVM — but when a script is run via `spark-submit script.py`, the
`spark-submit` *shell script* starts the driver JVM (with whatever
`--packages`/`--conf` flags were given on **that** command line) before your
Python code ever executes; `.config()` calls made from inside the script
run against an already-frozen classpath and are silently ignored for
jar-loading purposes. The fix: every `spark-submit` invocation in this repo
passes `--packages io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1`
and `--conf spark.jars.ivy=/opt/ivy_cache` as actual command-line flags (see
`docker-compose.yml`'s `spark-streamer` command and
`dashboard/api/app.py::_docker_exec_spark_job`) — never inside the job
scripts. `spark/jobs/spark_common.py` has the full explanation inline, and
only sets session-level configs (`spark.sql.extensions`,
`spark.sql.catalog.spark_catalog`) via `.config()`, because those *do* work
correctly from inside a spark-submit'd script — they're read when the SQL
session initializes inside the already-running JVM, not at classpath-load
time.

**`psycopg2.errors.UniqueViolation` from the generator on a re-run.**
`generator/generate_events.py` used to start new `account_id`s from a
hardcoded `2000` every time. Re-running it (or `./scripts/run_demo.sh`, which
calls it to seed data) against a table that already had accounts from a
previous run collided on the same id. Fixed by defaulting
`--next-id-start` to `MAX(account_id)+1` from whatever's already in the
table (falling back to `2000` only when it's empty), and by having inserts
use `ON CONFLICT (account_id) DO NOTHING` with a retry instead of crashing
on a collision — both the CLI generator and the dashboard's "Generate
burst" button now do this, so re-running either one is always safe.

If you hit a *new* "manifest unknown" error on a fresh pull, the fix is the
same shape as the first two: check `.env.example` for the overridable image
variables, find the current tag on the registry, and set it in a `.env`
file.

## 7. Simplifications vs. the whitepaper (and why)

- **Scale.** This runs against a handful of rows instead of 4B+/50M
  mutations a day. All the *mechanics* — append-only writes, deterministic
  keys, read-time dedup, deletion vectors — are the real thing, just at demo
  size.
- **QUALIFY.** The whitepaper's SQL uses `QUALIFY ROW_NUMBER() OVER (...) = 1`,
  a Databricks SQL / Snowflake extension. Open-source Spark SQL doesn't have
  `QUALIFY`, so `create_gold_view.py` (and the dashboard's pandas
  equivalent) use the same logic via a `ROW_NUMBER()` subquery instead —
  identical semantics, resolved entirely at read time.
- **48-hour out-of-order replay.** The generator/dashboard emit mutations in
  commit order; they don't hold events back for 48 hours. What *is* real:
  because surrogate keys are `xxhash64(account_id, source_lsn)` rather than
  wall-clock-based, replaying or duplicating any event is naturally
  idempotent-safe at read time — try running the ingestion job twice against
  the same Kafka offsets and note the gold view is unaffected even though
  silver grows.
- **Single Kafka broker, KRaft mode.** No Zookeeper, no replication factor —
  simplest thing that gives you a real, replayable Kafka log locally.
- **Decimal handling.** The Debezium connector uses
  `decimal.handling.mode=double` so `NUMERIC` balances arrive as plain JSON
  doubles. A production pipeline would use `precise`/`string` mode plus exact
  decimal parsing on the Spark side.
- **Persistence across `spark-submit` runs.** Each admin job is a fresh JVM,
  so the Spark-SQL gold view is registered in a local file-backed Hive
  metastore (`data/spark-warehouse/`) rather than an in-memory catalog. The
  dashboard sidesteps this entirely by reading the Delta table straight off
  disk with `deltalake` (delta-rs) and recomputing the dedup in pandas — no
  live Spark session required for monitoring reads.
- **Dashboard admin actions run via the Docker socket.** Simplest way for a
  small FastAPI service to trigger real `spark-submit` jobs in another
  container without duplicating the Spark/Delta runtime inside the dashboard
  image. Not something you'd do for an internet-facing service (see the
  security note in section 4).

## 8. Where things live

```
Lakehouse_CDC/
├── docker-compose.yml         all services; image tags overridable via .env
├── .env.example                copy to .env to override a stuck image tag
├── postgres/init.sql            accounting table + logical replication setup
├── debezium/postgres-connector.json   the CDC connector config
├── scripts/
│   ├── register-connector.sh    registers the connector via Connect's REST API
│   └── run_demo.sh              one-shot bring-up of the whole stack
├── generator/generate_events.py CLI tool: random inserts/updates/deletes
├── spark/
│   ├── Dockerfile               eclipse-temurin + pip-installed PySpark
│   └── jobs/
│       ├── spark_common.py       shared SparkSession builder (+ the --packages gotcha writeup)
│       ├── streaming_ingest.py   Kafka -> Delta, append-only, always-on
│       ├── create_gold_view.py   Spark-SQL gold_accounting_current view
│       ├── query_gold_view.py    prints silver vs. gold side by side
│       └── deletion_vector_compaction.py   soft-invalidate old rows
├── dashboard/
│   ├── api/app.py                FastAPI backend (status, mutate, admin actions)
│   └── static/                   plain HTML/CSS/JS frontend
└── data/                         Delta tables + Spark warehouse (gitignored)
```

## 9. Inspecting things directly

```bash
# raw Delta files on disk (partitioned by event_month, append-only)
find data/silver/accounting_cdc_staging -maxdepth 2

# Kafka topic Debezium is publishing to
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic cdc.public.accounting --from-beginning --max-messages 5

# connector status
curl -s http://localhost:8083/connectors/accounting-postgres-connector/status | python3 -m json.tool

# dashboard API directly (same data the UI shows)
curl -s http://localhost:8000/api/health | python3 -m json.tool
curl -s http://localhost:8000/api/gold | python3 -m json.tool
```

## 10. License

[MIT](LICENSE) — do whatever you want with this, no warranty implied.

## 11. Source

Based on `Bonus_Lakehouse_CDC_Architecture.pdf` (included in this repo),
a whitepaper-style scenario on high-throughput CDC lakehouse design. This
repo is an independent, runnable implementation of the architecture it
describes, built and debugged end to end rather than just summarized.
