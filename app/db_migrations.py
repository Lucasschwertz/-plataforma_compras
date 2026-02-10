from __future__ import annotations

from pathlib import Path

import click
from alembic import command
from alembic.config import Config as AlembicConfig
from flask import Flask


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _normalize_postgres_url(url: str) -> str:
    if url.startswith("postgres://"):
        return "postgresql://" + url[len("postgres://") :]
    return url


def to_sqlalchemy_url(raw_db_path: str) -> str:
    raw = (raw_db_path or "").strip()
    if not raw:
        raise RuntimeError("DB_PATH indefinido para migrations.")

    normalized = _normalize_postgres_url(raw)
    if normalized.startswith(("postgresql://", "postgresql+")):
        return normalized
    if normalized.startswith(("sqlite://", "sqlite+pysqlite://")):
        return normalized

    sqlite_path = Path(normalized).expanduser().resolve()
    return f"sqlite:///{sqlite_path.as_posix()}"


def build_alembic_config(app: Flask) -> AlembicConfig:
    root = _project_root()
    alembic_ini = root / "alembic.ini"
    if not alembic_ini.exists():
        raise RuntimeError("alembic.ini nao encontrado na raiz do projeto.")

    alembic_cfg = AlembicConfig(str(alembic_ini))
    alembic_cfg.set_main_option("script_location", str((root / "migrations").as_posix()))
    alembic_cfg.set_main_option("sqlalchemy.url", to_sqlalchemy_url(app.config["DB_PATH"]))
    return alembic_cfg


def register_db_cli(app: Flask) -> None:
    @app.cli.group("db")
    def db_group() -> None:
        """Comandos de migration (Alembic)."""

    @db_group.command("upgrade")
    @click.argument("revision", required=False, default="head")
    def db_upgrade(revision: str) -> None:
        cfg = build_alembic_config(app)
        command.upgrade(cfg, revision)
        click.echo(f"Migration aplicada ate {revision}.")

    @db_group.command("downgrade")
    @click.argument("revision", required=False, default="-1")
    def db_downgrade(revision: str) -> None:
        cfg = build_alembic_config(app)
        command.downgrade(cfg, revision)
        click.echo(f"Rollback aplicado ate {revision}.")

    @db_group.command("current")
    def db_current() -> None:
        cfg = build_alembic_config(app)
        command.current(cfg, verbose=True)
