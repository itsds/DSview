"""
init_db.py

One-time, idempotent setup script: applies every storage/ddl/*.sql file
against the Postgres instance in docker-compose.yml.

Not a migration tool — four tables so far, per schema (candles_1m, candle_indicators,
candle_revisions, footprint_1m), three more for live's backtest results
and one for its Futures ratios, and no schema-change history to manage yet: every file is CREATE ... IF
NOT EXISTS, so a column added to an existing table needs an ALTER TABLE
by hand. Revisit with a real migration tool (e.g. Alembic) once that
stops being rare.

Every pipeline gets the same tables (config/settings.py's Pipeline): live's
in `public`, as always, and the replay's in a `replay` schema created here
(Phase 4a), each file applied once per schema. The backtest results
(backtests.sql, Phase 4b) and the long/short ratios (futures_ratios.sql,
Phase 4c) are live's alone: LIVE_ONLY.

Run once, after `docker compose up -d`, from the repo root (which must
be on PYTHONPATH for config/ to import — see config/settings.py):

    PYTHONPATH=. python storage/init_db.py

Author: @DS
"""

from __future__ import annotations

from pathlib import Path

import psycopg

from psycopg import sql

from config.settings import LIVE_PIPELINE, PIPELINE_NAMES, Pipeline

DDL_DIR = Path(__file__).parent / "ddl"
# Applied to live's schema only: backtests read live's lake, and a replay has
# no results to keep (docs/ROADMAP.md, Phase 4b); nor a Futures feed (4c).
LIVE_ONLY = {"backtests.sql", "futures_ratios.sql"}


def main() -> None:
    for name in PIPELINE_NAMES:
        pipeline = Pipeline(name)
        if not pipeline.is_live:
            with psycopg.connect(LIVE_PIPELINE.postgres_dsn, autocommit=True) as conn:
                conn.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(pipeline.postgres_schema)))
        # The pipeline's DSN has only its schema on the search path, so each
        # file's unqualified CREATE ... IF NOT EXISTS lands there.
        with psycopg.connect(pipeline.postgres_dsn, autocommit=True) as conn:
            for ddl_file in sorted(DDL_DIR.glob("*.sql")):
                if ddl_file.name in LIVE_ONLY and not pipeline.is_live:
                    continue
                print(f"Applying {ddl_file.name} to {pipeline.postgres_schema}...")
                conn.execute(ddl_file.read_text())
    print("Done.")


if __name__ == "__main__":
    main()
