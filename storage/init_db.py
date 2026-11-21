"""
init_db.py

One-time, idempotent setup script: applies every storage/ddl/*.sql file
against the Postgres instance in docker-compose.yml.

Not a migration tool — three tables so far (candles_1m, candle_indicators,
candle_revisions)
and no schema-change history to manage yet: every file is CREATE ... IF
NOT EXISTS, so a column added to an existing table needs an ALTER TABLE
by hand. Revisit with a real migration tool (e.g. Alembic) once that
stops being rare.

Run once, after `docker compose up -d`, from the repo root (which must
be on PYTHONPATH for config/ to import — see config/settings.py):

    PYTHONPATH=. python storage/init_db.py

Author: @DS
"""

from __future__ import annotations

from pathlib import Path

import psycopg

from config.settings import POSTGRES_DSN

DDL_DIR = Path(__file__).parent / "ddl"


def main() -> None:
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        for ddl_file in sorted(DDL_DIR.glob("*.sql")):
            print(f"Applying {ddl_file.name}...")
            conn.execute(ddl_file.read_text())
    print("Done.")


if __name__ == "__main__":
    main()
