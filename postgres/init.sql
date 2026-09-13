-- ------------------------------------------------------------------------------------
-- Source OLTP schema: a small "accounting" table standing in for the
-- 4-billion-row, 50M+ mutations/day table described in the whitepaper.
-- ------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS accounting (
    account_id          BIGINT PRIMARY KEY,
    current_balance      NUMERIC(14, 2) NOT NULL,
    currency              TEXT NOT NULL DEFAULT 'USD',
    transaction_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Debezium needs the full "before" image on UPDATE/DELETE to build accurate
-- change events, since Postgres logical decoding only emits changed columns
-- by default (REPLICA IDENTITY DEFAULT).
ALTER TABLE accounting REPLICA IDENTITY FULL;

-- A logical replication publication is how Debezium's Postgres connector
-- subscribes to WAL changes for this table (pgoutput plugin).
CREATE PUBLICATION cdc_publication FOR TABLE accounting;

-- Seed a handful of accounts so the demo has something to look at
-- before the generator starts producing ongoing mutations.
INSERT INTO accounting (account_id, current_balance, currency, transaction_timestamp, updated_at)
VALUES
    (1001, 500.00,  'USD', now(), now()),
    (1002, 1250.75, 'USD', now(), now()),
    (1003, 0.00,    'USD', now(), now()),
    (1004, 9999.99, 'USD', now(), now()),
    (1005, 42.10,   'USD', now(), now())
ON CONFLICT (account_id) DO NOTHING;
