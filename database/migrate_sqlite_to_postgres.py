from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from typing import Iterable, List

import psycopg2


def _sqlite_tables(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return [row[0] for row in rows]


def _sqlite_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [row[1] for row in rows]


def _postgres_columns(cur, table: str) -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def _fetch_rows(conn: sqlite3.Connection, table: str, columns: Iterable[str]) -> List[tuple]:
    cols = ", ".join(columns)
    return conn.execute(f"SELECT {cols} FROM {table}").fetchall()


def migrate(sqlite_path: str, postgres_url: str, truncate: bool) -> None:
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row
    pg_conn = psycopg2.connect(postgres_url)
    pg_conn.autocommit = True

    try:
        with pg_conn.cursor() as cur:
            tables = _sqlite_tables(sqlite_conn)
            for table in tables:
                pg_columns = _postgres_columns(cur, table)
                if not pg_columns:
                    continue
                sqlite_columns = _sqlite_columns(sqlite_conn, table)
                common = [col for col in sqlite_columns if col in pg_columns]
                if not common:
                    continue

                if truncate:
                    cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")

                rows = _fetch_rows(sqlite_conn, table, common)
                if not rows:
                    continue

                placeholders = ", ".join(["%s"] * len(common))
                cols_sql = ", ".join(common)
                insert_sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

                cur.executemany(insert_sql, rows)
                print(f"{table}: {len(rows)} registros")
    finally:
        sqlite_conn.close()
        pg_conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrar dados SQLite -> PostgreSQL.")
    parser.add_argument("--sqlite", default="database/plataforma_compras.db", help="SQLite db path.")
    parser.add_argument("--postgres", default=os.environ.get("DATABASE_URL"), help="Postgres URL.")
    parser.add_argument("--truncate", action="store_true", help="Limpa tabelas antes de migrar.")
    args = parser.parse_args()

    if not args.postgres:
        raise RuntimeError("Informe --postgres ou defina DATABASE_URL.")
    if not os.path.exists(args.sqlite):
        raise RuntimeError(f"SQLite nao encontrado: {args.sqlite}")

    migrate(args.sqlite, args.postgres, args.truncate)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"Falha na migracao: {exc}")
        sys.exit(1)
