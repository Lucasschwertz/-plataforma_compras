from __future__ import annotations

from app.db import close_db, get_db
from app.procurement.erp_outbox import process_purchase_order_outbox
from app.workers.erp_runtime import build_worker_push_fn


def process_erp_outbox_once(app, tenant_id: str | None = None, limit: int = 25, push_fn=None) -> dict:
    with app.app_context():
        db = get_db()
        try:
            resolved_push_fn = push_fn or build_worker_push_fn()
            result = process_purchase_order_outbox(
                db,
                tenant_id=tenant_id,
                limit=limit,
                push_fn=resolved_push_fn,
            )
            db.commit()
            return result
        finally:
            close_db()
