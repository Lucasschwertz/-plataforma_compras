from __future__ import annotations

import argparse
import os
import time
import uuid

from app import create_app
from app.db import close_db, get_db
from app.procurement.erp_outbox import process_purchase_order_outbox
from app.workers.erp_runtime import build_worker_push_fn


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Worker de processamento da fila ERP (outbox).")
    parser.add_argument("--once", action="store_true", help="Processa um lote unico e encerra.")
    parser.add_argument("--tenant-id", default="", help="Processa apenas um tenant especifico.")
    parser.add_argument("--limit", type=int, default=0, help="Quantidade maxima por lote.")
    parser.add_argument("--interval", type=int, default=0, help="Intervalo em segundos entre lotes.")
    return parser


def _run_once(app, tenant_id: str | None, limit: int, push_fn) -> dict:
    with app.app_context():
        db = get_db()
        try:
            result = process_purchase_order_outbox(
                db,
                tenant_id=tenant_id,
                limit=limit,
                push_fn=push_fn,
            )
            db.commit()
            return result
        finally:
            close_db()


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    os.environ.setdefault("SYNC_SCHEDULER_ENABLED", "false")
    os.environ.setdefault("DB_AUTO_INIT", "false")
    app = create_app()

    configured_limit = int(app.config.get("ERP_OUTBOX_WORKER_BATCH_SIZE", 25) or 25)
    configured_interval = int(app.config.get("ERP_OUTBOX_WORKER_INTERVAL_SECONDS", 5) or 5)
    limit = max(1, int(args.limit or configured_limit))
    interval_seconds = max(1, int(args.interval or configured_interval))
    tenant_id = str(args.tenant_id or "").strip() or None
    push_fn = build_worker_push_fn()

    while True:
        run_request_id = f"worker-{uuid.uuid4().hex[:12]}"
        summary = _run_once(app, tenant_id=tenant_id, limit=limit, push_fn=push_fn)
        app.logger.info(
            "erp_outbox_worker_batch_completed",
            extra={
                "request_id": run_request_id,
                "tenant_id": tenant_id or "all",
                "processed": summary.get("processed", 0),
                "succeeded": summary.get("succeeded", 0),
                "requeued": summary.get("requeued", 0),
                "failed": summary.get("failed", 0),
            },
        )
        if args.once:
            break
        time.sleep(interval_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
