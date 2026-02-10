from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass(frozen=True)
class ServiceOutput:
    payload: Dict[str, Any]
    status_code: int = 200


@dataclass(frozen=True)
class PurchaseRequestCreateInput:
    status: str
    priority: str
    number: str | None
    requested_by: str | None
    department: str | None
    needed_at: str | None
    items: List[Dict[str, Any]]


@dataclass(frozen=True)
class RfqCreateInput:
    title: str
    purchase_request_item_ids: List[int]


@dataclass(frozen=True)
class RfqAwardInput:
    rfq_id: int
    reason: str
    supplier_name: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PurchaseOrderFromAwardInput:
    award_id: int
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PurchaseOrderErpIntentInput:
    purchase_order_id: int
    request_id: str | None
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AnalyticsRequestInput:
    section: str
    role: str
    tenant_id: str
    request_args: Dict[str, Any]
    user_email: str | None
    display_name: str | None
    team_members: List[str]


@dataclass(frozen=True)
class AuthLoginInput:
    email: str
    password: str


@dataclass(frozen=True)
class AuthRegisterInput:
    email: str
    password: str
    display_name: str | None
    company_name: str | None


@dataclass(frozen=True)
class AuthUser:
    email: str
    display_name: str
    tenant_id: str
    role: str
