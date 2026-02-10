from __future__ import annotations

import os
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = None


def _normalize_sqlalchemy_url(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        raise RuntimeError("URL de banco nao informada para Alembic.")

    if value.startswith("postgres://"):
        return "postgresql://" + value[len("postgres://") :]

    if value.startswith(("postgresql://", "postgresql+", "sqlite://", "sqlite+pysqlite://")):
        return value

    sqlite_path = Path(value).expanduser().resolve()
    return f"sqlite:///{sqlite_path.as_posix()}"


def _database_url() -> str:
    env_url = os.environ.get("DATABASE_URL") or os.environ.get("DB_PATH")
    configured_url = config.get_main_option("sqlalchemy.url")
    return _normalize_sqlalchemy_url(env_url or configured_url)


def run_migrations_offline() -> None:
    url = _database_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    section = config.get_section(config.config_ini_section) or {}
    section["sqlalchemy.url"] = _database_url()

    connectable = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
