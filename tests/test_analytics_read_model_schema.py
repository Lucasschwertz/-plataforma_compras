import os
import unittest

from app import create_app
from app.config import Config
from app.db import close_db
from tests.helpers.temp_db import TempDbSandbox, open_sqlite_temp_connection


def _table_exists(db_path: str, table_name: str) -> bool:
    conn = open_sqlite_temp_connection(db_path)
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
            (table_name,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


class AnalyticsReadModelSchemaTest(unittest.TestCase):
    def setUp(self) -> None:
        self._temp_db = TempDbSandbox(prefix="analytics_read_model_schema")
        self.db_path = self._temp_db.db_path
        self._prev_env = os.environ.get("FLASK_ENV")
        os.environ["FLASK_ENV"] = "development"

    def tearDown(self) -> None:
        if self._prev_env is None:
            os.environ.pop("FLASK_ENV", None)
        else:
            os.environ["FLASK_ENV"] = self._prev_env
        self._temp_db.cleanup()

    def _build_app(self):
        TempConfig = self._temp_db.make_config(
            Config,
            TESTING=False,
            DB_AUTO_INIT=False,
            AUTH_ENABLED=False,
            SYNC_SCHEDULER_ENABLED=False,
        )
        return create_app(TempConfig)

    def test_read_model_tables_exist_after_upgrade(self) -> None:
        app = self._build_app()
        runner = app.test_cli_runner()

        upgrade_result = runner.invoke(args=["db", "upgrade"])
        self.assertEqual(upgrade_result.exit_code, 0, msg=upgrade_result.output)

        expected_tables = {
            "ar_projection_state",
            "ar_event_dedupe",
            "ar_kpi_daily",
            "ar_supplier_daily",
            "ar_process_stage_daily",
        }
        for table_name in expected_tables:
            self.assertTrue(_table_exists(self.db_path, table_name), msg=f"missing table: {table_name}")

        with app.app_context():
            close_db()


if __name__ == "__main__":
    unittest.main()
