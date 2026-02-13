from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List


class ErpGatewayError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        definitive: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = str(code or "").strip() or None
        self.definitive = bool(definitive)


class ErpGateway(ABC):
    @abstractmethod
    def fetch_records(
        self,
        entity: str,
        since_updated_at: str | None,
        since_id: str | None,
        *,
        limit: int = 100,
    ) -> List[dict]:
        raise NotImplementedError

    @abstractmethod
    def push_purchase_order(self, purchase_order: dict) -> dict:
        raise NotImplementedError

