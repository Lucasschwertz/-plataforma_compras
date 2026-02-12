from __future__ import annotations

import os
from functools import lru_cache
from typing import Callable


def _activate_worker_erp_context() -> None:
    os.environ["ERP_CLIENT_CONTEXT"] = "worker"


@lru_cache(maxsize=1)
def _gateway():
    _activate_worker_erp_context()
    from app.infrastructure.erp import SeniorErpGateway

    return SeniorErpGateway()


def build_worker_push_fn() -> Callable[[dict], dict]:
    gateway = _gateway()
    return gateway.push_purchase_order


def build_worker_fetch_fn() -> Callable[[str, str | None, str | None, int], list[dict]]:
    gateway = _gateway()

    def _fetch(entity: str, since_updated_at: str | None, since_id: str | None, limit: int = 100) -> list[dict]:
        return gateway.fetch_records(entity, since_updated_at, since_id, limit=limit)

    return _fetch

