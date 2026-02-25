#!/usr/bin/env python3
"""
Run once in CI (or locally) to ensure required tables/triggers/indexes exist on Turso/libsql.
Usage (GitHub Actions): run this as a step before running cfr.py
"""
import os
import sys
import libsql

DT_URL = os.getenv("DBT_URL") or os.getenv("DT_URL") or os.getenv("LIBSQL_URL")
DT_KEY = os.getenv("DBT_KEY") or os.getenv("DT_KEY") or os.getenv("LIBSQL_KEY")

if not DT_URL or not DT_KEY:
    print("Missing DBT_URL/DBT_KEY or DT_URL/DT_KEY in environment", file=sys.stderr)
    sys.exit(2)

DDL = [
    """CREATE TABLE IF NOT EXISTS rate
    (
        id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))),
        name TEXT NOT NULL,
        symbol TEXT NOT NULL,
        rate TEXT,
        version INTEGER NOT NULL DEFAULT 1,
        updated_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE (name, symbol)
    ) STRICT;""",
    "CREATE INDEX IF NOT EXISTS idx_rate_name   ON rate(name);",
    "CREATE INDEX IF NOT EXISTS idx_rate_symbol ON rate(symbol);",
    """CREATE TRIGGER IF NOT EXISTS trg_rate_updated_at
       AFTER UPDATE ON rate
    BEGIN
      UPDATE rate
      SET updated_at = NEW.updated_at,
          version = OLD.version + 1
      WHERE id = OLD.id;
    END;""",
    """CREATE TABLE IF NOT EXISTS snapshots (
         id TEXT PRIMARY KEY,
         ts TEXT NOT NULL,
         payload TEXT NOT NULL
    ) STRICT;""",
]

def ensure():
    conn = libsql.connect(DT_URL, auth_token=DT_KEY)
    cur = conn.cursor()
    try:
        for sql in DDL:
            cur.execute(sql)
        conn.commit()
        print("Schema ensured ✅")
    finally:
        conn.close()

if __name__ == "__main__":
    ensure()
