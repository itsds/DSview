"""
init_db.py

One-time, idempotent setup script: applies every storage/ddl/*.sql file
against the Postgres instance in docker-compose.yml.

Not a migration tool — there's only one table so far and no schema-change
history to manage yet. Revisit with a real migration tool (e.g. Alembic)
once that stops being true.

Run once, after `docker compose up -d`:

    python storage/init_db.py

Author: @DS
"""

from __future__ import annotations

from pathlib import Path

import psycopg

# TODO(@DS): move to config/settings.py once the config/ module is scaffolded.
POSTGRES_DSN = "postgresql://dsview:dsview@localhost:5432/dsview"

DDL_DIR = Path(__file__).parent / "ddl"


def main() -> None:
    with psycopg.connect(POSTGRES_DSN, autocommit=True) as conn:
        for ddl_file in sorted(DDL_DIR.glob("*.sql")):
            print(f"Applying {ddl_file.name}...")
            conn.execute(ddl_file.read_text())
    print("Done.")


if __name__ == "__main__":
    main()
