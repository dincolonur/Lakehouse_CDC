"""
Simulates OLTP mutations against the `accounting` table: inserts, updates,
and the occasional delete. Every mutation Postgres commits here becomes a
Debezium CDC event on Kafka a moment later -- this script never touches
Kafka, Spark, or Delta directly.

Safe to re-run: new account_ids are chosen starting from
MAX(account_id)+1 in the table (unless --next-id-start is given explicitly),
and inserts fall back to picking a fresh id on a collision instead of
crashing -- so running this multiple times against a table that already has
data (e.g. from a previous run, or from mutations submitted via the
dashboard) just keeps adding accounts rather than erroring out.
"""
import os
import random
import time
import argparse

import psycopg2

CONN_PARAMS = dict(
    host=os.environ.get("PGHOST", "localhost"),
    port=os.environ.get("PGPORT", "5432"),
    dbname=os.environ.get("PGDATABASE", "sourcedb"),
    user=os.environ.get("PGUSER", "cdc_user"),
    password=os.environ.get("PGPASSWORD", "cdc_pass"),
)

DEFAULT_FIRST_ID = 2000


def get_existing_account_ids(cur):
    cur.execute("SELECT account_id FROM accounting")
    return [row[0] for row in cur.fetchall()]


def next_available_id(cur, floor=DEFAULT_FIRST_ID):
    """MAX(account_id)+1 if the table has rows, else `floor`."""
    cur.execute("SELECT COALESCE(MAX(account_id), %s - 1) FROM accounting", (floor,))
    return cur.fetchone()[0] + 1


def insert_account(cur, account_id):
    """Returns True if inserted, False if account_id already existed."""
    cur.execute(
        """
        INSERT INTO accounting (account_id, current_balance, currency, transaction_timestamp, updated_at)
        VALUES (%s, %s, 'USD', now(), now())
        ON CONFLICT (account_id) DO NOTHING
        """,
        (account_id, round(random.uniform(0, 5000), 2)),
    )
    return cur.rowcount > 0


def update_balance(cur, account_id):
    delta = round(random.uniform(-200, 200), 2)
    cur.execute(
        """
        UPDATE accounting
        SET current_balance = current_balance + %s,
            transaction_timestamp = now(),
            updated_at = now()
        WHERE account_id = %s
        """,
        (delta, account_id),
    )


def delete_account(cur, account_id):
    cur.execute("DELETE FROM accounting WHERE account_id = %s", (account_id,))


def main():
    parser = argparse.ArgumentParser(description="Simulates OLTP mutations against the accounting table.")
    parser.add_argument("--events", type=int, default=30, help="Number of mutations to emit, then exit.")
    parser.add_argument("--interval", type=float, default=0.5, help="Seconds to sleep between mutations.")
    parser.add_argument(
        "--next-id-start", type=int, default=None,
        help="First account_id to use for new inserts. Defaults to MAX(account_id)+1 "
             "in the table (or 2000 if empty), so re-running this never collides "
             "with accounts from a previous run.",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**CONN_PARAMS)
    conn.autocommit = True

    print(f"Connected to {CONN_PARAMS['host']}:{CONN_PARAMS['port']}/{CONN_PARAMS['dbname']}. "
          f"Emitting {args.events} mutations...")

    with conn.cursor() as cur:
        next_new_id = args.next_id_start if args.next_id_start is not None else next_available_id(cur)

    for i in range(args.events):
        with conn.cursor() as cur:
            existing = get_existing_account_ids(cur)
            action = random.choices(
                ["insert", "update", "update", "update", "delete"],
                weights=[3, 5, 5, 5, 1],
            )[0]

            if action == "insert" or not existing:
                # Retry on collision (e.g. the dashboard or another run created
                # this id in the meantime) instead of crashing.
                for _ in range(20):
                    if insert_account(cur, next_new_id):
                        print(f"[{i + 1}/{args.events}] INSERT account_id={next_new_id}")
                        next_new_id += 1
                        break
                    next_new_id += 1
                else:
                    print(f"[{i + 1}/{args.events}] WARNING: could not find a free account_id, skipping insert")
            elif action == "delete" and len(existing) > 3:
                target = random.choice(existing)
                delete_account(cur, target)
                print(f"[{i + 1}/{args.events}] DELETE account_id={target}")
            else:
                target = random.choice(existing)
                update_balance(cur, target)
                print(f"[{i + 1}/{args.events}] UPDATE account_id={target}")

        time.sleep(args.interval)

    conn.close()
    print("Done. Every one of these mutations is now (or will shortly be) a Debezium CDC event on Kafka.")


if __name__ == "__main__":
    main()
