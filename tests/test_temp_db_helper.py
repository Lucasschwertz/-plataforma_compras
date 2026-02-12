import os
import tempfile
import unittest

from tests.helpers.temp_db import TempDbSandbox, assert_safe_temp_db_path, open_sqlite_temp_connection


class TempDbHelperTest(unittest.TestCase):
    def test_temp_db_create_and_cleanup(self) -> None:
        sandbox = TempDbSandbox(prefix="temp_db_sanity")
        db_path = sandbox.db_path
        temp_dir = sandbox.temp_dir

        self.assertTrue(os.path.exists(temp_dir))
        self.assertTrue(db_path.startswith(tempfile.gettempdir()))

        conn = open_sqlite_temp_connection(db_path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS sanity (id INTEGER PRIMARY KEY, value TEXT)")
            conn.execute("INSERT INTO sanity (value) VALUES ('ok')")
            row = conn.execute("SELECT COUNT(*) FROM sanity").fetchone()
            self.assertEqual(int(row[0]), 1)
        finally:
            conn.close()

        sandbox.cleanup()
        self.assertFalse(os.path.exists(db_path))
        self.assertFalse(os.path.exists(temp_dir))

    def test_disallow_workspace_paths(self) -> None:
        workspace_db = os.path.join(os.getcwd(), "plataforma_compras_test.db")
        with self.assertRaises(ValueError):
            assert_safe_temp_db_path(workspace_db)


if __name__ == "__main__":
    unittest.main()
