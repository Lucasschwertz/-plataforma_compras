import os


class Config:
    BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    DATABASE_DIR = os.path.join(BASE_DIR, "database")
    DB_PATH = os.path.join(DATABASE_DIR, "plataforma_compras.db")

    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-plataforma-compras")