from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[2]
_TEMP_ROOT = Path(tempfile.gettempdir()).resolve()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def assert_safe_temp_db_path(db_path: str) -> None:
    resolved = Path(db_path).resolve()
    if not _is_within(resolved, _TEMP_ROOT):
        raise ValueError(f"Temporary DB must live under TEMP: {resolved}")
    if _is_within(resolved, _REPO_ROOT):
        raise ValueError(f"Temporary DB cannot live inside repository: {resolved}")
    for part in resolved.parts:
        lowered = part.lower()
        if lowered == ".tmp_run":
            raise ValueError(f"Temporary DB cannot live under .tmp_run: {resolved}")
        if lowered.startswith("pc_"):
            raise ValueError(f"Temporary DB cannot live under pc_* path: {resolved}")


def open_sqlite_temp_connection(db_path: str) -> sqlite3.Connection:
    assert_safe_temp_db_path(db_path)
    parent = Path(db_path).parent
    parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        db_path,
        timeout=30.0,
        isolation_level=None,
    )
    conn.execute("PRAGMA journal_mode=DELETE")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def ensure_temp_db_file(db_path: str) -> None:
    conn = open_sqlite_temp_connection(db_path)
    conn.close()


def _remove_file_with_retry(path: Path, attempts: int = 8, base_delay: float = 0.05) -> None:
    for attempt in range(attempts):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            time.sleep(base_delay * (2**attempt))
        except FileNotFoundError:
            return


def _force_permissions(root: Path) -> None:
    for current_root, dirnames, filenames in os.walk(root, topdown=False):
        current = Path(current_root)
        try:
            os.chmod(current, 0o700)
        except OSError:
            pass
        for name in filenames:
            file_path = current / name
            try:
                os.chmod(file_path, 0o600)
            except OSError:
                pass
        for name in dirnames:
            dir_path = current / name
            try:
                os.chmod(dir_path, 0o700)
            except OSError:
                pass


def remove_tree_with_retry(path: str, attempts: int = 8, base_delay: float = 0.05) -> None:
    root = Path(path)
    if not root.exists():
        return
    for attempt in range(attempts):
        try:
            shutil.rmtree(root)
            return
        except FileNotFoundError:
            return
        except PermissionError:
            _force_permissions(root)
            time.sleep(base_delay * (2**attempt))
        except OSError:
            _force_permissions(root)
            time.sleep(base_delay * (2**attempt))


@dataclass
class TempDbSandbox:
    prefix: str = "plataforma_compras_tests"
    db_name: str = "plataforma_compras_test.db"

    def __post_init__(self) -> None:
        temp_root = Path(tempfile.gettempdir()).resolve()
        folder = temp_root / f"{self.prefix}_{uuid.uuid4().hex}"
        folder.mkdir(parents=True, exist_ok=False)
        self.temp_dir = str(folder)
        self.db_path = str(folder / self.db_name)
        assert_safe_temp_db_path(self.db_path)
        ensure_temp_db_file(self.db_path)

    def make_config(self, base_config, **overrides):
        attrs = {
            "DATABASE_DIR": self.temp_dir,
            "DB_PATH": self.db_path,
            "SYNC_SCHEDULER_ENABLED": False,
        }
        attrs.update(overrides)
        return type("TempConfig", (base_config,), attrs)

    def cleanup(self) -> None:
        db_file = Path(self.db_path)
        _remove_file_with_retry(db_file)
        _remove_file_with_retry(db_file.with_suffix(db_file.suffix + "-journal"))
        _remove_file_with_retry(db_file.with_suffix(db_file.suffix + "-wal"))
        _remove_file_with_retry(db_file.with_suffix(db_file.suffix + "-shm"))
        remove_tree_with_retry(self.temp_dir)

