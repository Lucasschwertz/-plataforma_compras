from __future__ import annotations

import os
import sys

import psycopg2


def main() -> None:
    host = os.environ.get("PGHOST", "localhost")
    port = int(os.environ.get("PGPORT", "5432"))
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD")
    db_name = os.environ.get("PGDATABASE", "portal_compras")

    if not password:
        raise RuntimeError("Defina a senha em PGPASSWORD.")

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname="postgres",
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone():
                print(f"Database '{db_name}' ja existe.")
                return
            cur.execute(f'CREATE DATABASE "{db_name}"')
            print(f"Database '{db_name}' criado.")
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Erro ao criar database: {exc}")
        sys.exit(1)
