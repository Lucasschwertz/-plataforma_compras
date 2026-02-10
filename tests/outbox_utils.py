from __future__ import annotations

from app.db import close_db, get_db
from app.procurement.erp_outbox import process_purchase_order_outbox


def process_erp_outbox_once(app, tenant_id: str | None = None, limit: int = 25) -> dict:
    with app.app_context():
        db = get_db()
        try:
            result = process_purchase_order_outbox(
                db,
                tenant_id=tenant_id,
                limit=limit,
            )
            db.commit()
            return result
        finally:
            close_db()
