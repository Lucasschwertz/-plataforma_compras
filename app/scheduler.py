from __future__ import annotations

import os
import threading
import time
from typing import Iterable

from flask import Flask

from app.db import close_db, get_db
from app.routes.procurement_routes import (
    SYNC_SUPPORTED_SCOPES,
    _finish_sync_run,
    _start_sync_run,
    _sync_from_erp,
)


DEFAULT_SCHEDULER_SCOPES = ("supplier", "purchase_request", "purchase_order", "receipt")


class SyncScheduler:
    def __init__(self, app: Flask) -> None:
        self.app = app
        self.interval_seconds = _int_config(app, "SYNC_SCHEDULER_INTERVAL_SECONDS", 120, 10, 3600)
        self.min_backoff_seconds = _int_config(app, "SYNC_SCHEDULER_MIN_BACKOFF_SECONDS", 30, 5, 3600)
        self.max_backoff_seconds = _int_config(
            app,
            "SYNC_SCHEDULER_MAX_BACKOFF_SECONDS",
            600,
            self.min_backoff_seconds,
            86_400,
        )
        self.limit = _int_config(app, "SYNC_SCHEDULER_LIMIT", 200, 1, 5000)
        self.scopes = _parse_scopes(app.config.get("SYNC_SCHEDULER_SCOPES"))

        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure_counts: dict[tuple[str, str], int] = {}
        self._next_run_at: dict[tuple[str, str], float] = {}
        self._parent_sync_run_id: dict[tuple[str, str], int] = {}

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run_loop, name="sync-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            self.run_once()
            self._stop_event.wait(self.interval_seconds)

    def run_once(self) -> None:
        with self.app.app_context():
            db = get_db()
            try:
                tenant_rows = db.execute("SELECT id FROM tenants ORDER BY id").fetchall()
                tenant_ids = [row["id"] for row in tenant_rows]
                for tenant_id in tenant_ids:
                    for scope in self.scopes:
                        self._run_scope(db, tenant_id, scope)
            finally:
                close_db()

    def _run_scope(self, db, tenant_id: str, scope: str) -> None:
        key = (tenant_id, scope)
        if not self._is_due(key):
            return
        if self._has_running_sync(db, tenant_id, scope):
            return

        attempt = self._failure_counts.get(key, 0) + 1
        parent_sync_run_id = self._parent_sync_run_id.get(key)
        sync_run_id = _start_sync_run(
            db,
            tenant_id,
            scope=scope,
            attempt=attempt,
            parent_sync_run_id=parent_sync_run_id,
        )

        try:
            result = _sync_from_erp(db, tenant_id, scope, limit=self.limit)
            _finish_sync_run(
                db,
                tenant_id,
                sync_run_id,
                status="succeeded",
                records_in=result["records_in"],
                records_upserted=result["records_upserted"],
            )
            db.commit()
            self._clear_backoff(key)
        except Exception as exc:  # noqa: BLE001 - MVP: loga erro resumido
            _finish_sync_run(db, tenant_id, sync_run_id, status="failed", records_in=0, records_upserted=0)
            db.execute(
                """
                UPDATE sync_runs
                SET error_summary = ?, error_details = ?
                WHERE id = ? AND tenant_id = ?
                """,
                (str(exc)[:200], str(exc)[:1000], sync_run_id, tenant_id),
            )
            db.commit()
            self._register_failure(key, sync_run_id)

    def _has_running_sync(self, db, tenant_id: str, scope: str) -> bool:
        row = db.execute(
            """
            SELECT id
            FROM sync_runs
            WHERE tenant_id = ? AND scope = ? AND status = 'running'
            ORDER BY started_at DESC, id DESC
            LIMIT 1
            """,
            (tenant_id, scope),
        ).fetchone()
        return row is not None

    def _is_due(self, key: tuple[str, str]) -> bool:
        next_run_at = self._next_run_at.get(key)
        if next_run_at is None:
            return True
        return time.monotonic() >= next_run_at

    def _clear_backoff(self, key: tuple[str, str]) -> None:
        self._failure_counts.pop(key, None)
        self._next_run_at.pop(key, None)
        self._parent_sync_run_id.pop(key, None)

    def _register_failure(self, key: tuple[str, str], sync_run_id: int) -> None:
        failure_count = self._failure_counts.get(key, 0) + 1
        self._failure_counts[key] = failure_count
        if key not in self._parent_sync_run_id:
            self._parent_sync_run_id[key] = sync_run_id

        backoff_seconds = min(
            self.max_backoff_seconds,
            self.min_backoff_seconds * (2 ** (failure_count - 1)),
        )
        self._next_run_at[key] = time.monotonic() + backoff_seconds


def start_sync_scheduler(app: Flask) -> SyncScheduler | None:
    if not _should_start_scheduler(app):
        return None
    scheduler = SyncScheduler(app)
    scheduler.start()
    app.extensions["sync_scheduler"] = scheduler
    app.logger.info(
        "Sync scheduler started: interval=%ss scopes=%s",
        scheduler.interval_seconds,
        ", ".join(scheduler.scopes),
    )
    return scheduler


def _should_start_scheduler(app: Flask) -> bool:
    if not app.config.get("SYNC_SCHEDULER_ENABLED", False):
        return False
    if app.config.get("TESTING"):
        return False
    if app.debug:
        run_main = os.environ.get("WERKZEUG_RUN_MAIN")
        if run_main and run_main.lower() != "true":
            return False
    return True


def _int_config(app: Flask, key: str, default: int, min_value: int, max_value: int) -> int:
    try:
        value = int(app.config.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(value, max_value))


def _parse_scopes(value: object) -> list[str]:
    items: Iterable[str]
    if value is None:
        items = DEFAULT_SCHEDULER_SCOPES
    elif isinstance(value, str):
        items = [scope.strip() for scope in value.split(",") if scope.strip()]
    elif isinstance(value, (list, tuple, set)):
        items = [str(scope).strip() for scope in value if str(scope).strip()]
    else:
        items = DEFAULT_SCHEDULER_SCOPES

    filtered = [scope for scope in items if scope in SYNC_SUPPORTED_SCOPES]
    if not filtered:
        filtered = [scope for scope in DEFAULT_SCHEDULER_SCOPES if scope in SYNC_SUPPORTED_SCOPES]
    return filtered
