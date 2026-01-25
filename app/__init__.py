from flask import Flask, g, request

from app.config import Config
from app.db import close_db, init_db


def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)

    _ensure_database_dir(app)
    _register_tenant(app)
    _register_blueprints(app)

    with app.app_context():
        init_db()

    app.teardown_appcontext(close_db)
    return app


def _ensure_database_dir(app: Flask) -> None:
    database_dir = app.config.get("DATABASE_DIR")
    if database_dir:
        import os

        os.makedirs(database_dir, exist_ok=True)


def _register_blueprints(app: Flask) -> None:
    from app.routes.home_routes import home_bp
    from app.routes.procurement_routes import procurement_bp

    app.register_blueprint(home_bp)
    app.register_blueprint(procurement_bp)


def _register_tenant(app: Flask) -> None:
    @app.before_request
    def load_company() -> None:
        # Prototype: allow overriding company via header for testing.
        header_company = request.headers.get("X-Company-Id")
        if header_company:
            try:
                g.company_id = int(header_company)
                return
            except ValueError:
                pass
        g.company_id = None