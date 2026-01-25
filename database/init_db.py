import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from app import create_app
from app.db import init_db


app = create_app()


if __name__ == "__main__":
    with app.app_context():
        init_db()
    print("Database initialized.")