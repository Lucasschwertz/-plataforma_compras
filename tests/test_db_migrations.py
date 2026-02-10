import os
import shutil
import sqlite3
import tempfile
import unittest
import uuid

from app import create_app
from app.config import Config
from app.db import close_db


def _table_exists(db_path: str, table_name: str) -> bool:
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


class DbMigrationsTest(unittest.TestCase):
    def setUp(self) -> None:
        base_tmp = tempfile.gettempdir()
        self._tmpdir_path = os.path.join(base_tmp, f"pc_migrations_test_{uuid.uuid4().hex}")
        os.makedirs(self._tmpdir_path, exist_ok=True)
        self.db_path = os.path.join(self._tmpdir_path, "plataforma_compras_test.db")
        self._prev_env = os.environ.get("FLASK_ENV")
        os.environ["FLASK_ENV"] = "development"

    def tearDown(self) -> None:
        if self._prev_env is None:
            os.environ.pop("FLASK_ENV", None)
        else:
            os.environ["FLASK_ENV"] = self._prev_env
        shutil.rmtree(self._tmpdir_path, ignore_errors=True)

    def _build_app(self, *, testing: bool, db_auto_init: bool):
        db_path = self.db_path

        class TempConfig(Config):
            DATABASE_DIR = self._tmpdir_path
            DB_PATH = db_path
            TESTING = testing
            DB_AUTO_INIT = db_auto_init
            SYNC_SCHEDULER_ENABLED = False

        app = create_app(TempConfig)
        return app

    def test_schema_not_created_by_default(self) -> None:
        app = self._build_app(testing=False, db_auto_init=False)
        with app.app_context():
            close_db()

        self.assertFalse(_table_exists(self.db_path, "tenants"))

    def test_schema_created_with_explicit_dev_flag(self) -> None:
        app = self._build_app(testing=False, db_auto_init=True)
        with app.app_context():
            close_db()

        self.assertTrue(_table_exists(self.db_path, "tenants"))

    def test_flask_db_upgrade_and_downgrade(self) -> None:
        app = self._build_app(testing=False, db_auto_init=False)
        runner = app.test_cli_runner()

        upgrade_result = runner.invoke(args=["db", "upgrade"])
        self.assertEqual(upgrade_result.exit_code, 0, msg=upgrade_result.output)
        self.assertTrue(_table_exists(self.db_path, "tenants"))

        downgrade_result = runner.invoke(args=["db", "downgrade", "base"])
        self.assertEqual(downgrade_result.exit_code, 0, msg=downgrade_result.output)
        self.assertFalse(_table_exists(self.db_path, "tenants"))

        reupgrade_result = runner.invoke(args=["db", "upgrade"])
        self.assertEqual(reupgrade_result.exit_code, 0, msg=reupgrade_result.output)
        self.assertTrue(_table_exists(self.db_path, "tenants"))
