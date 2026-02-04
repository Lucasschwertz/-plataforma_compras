from __future__ import annotations

from typing import Dict, List


DEFAULT_RISK_FLAGS = {
    "no_supplier_response": False,
    "late_delivery": False,
    "sla_breach": False,
}

ERP_DATA: Dict[str, List[dict]] = {
    "supplier": [
        {
            "external_id": "SUP-100",
            "name": "Fornecedor Alpha",
            "tax_id": "00000000000100",
            "risk_flags": DEFAULT_RISK_FLAGS,
            "updated_at": "2026-01-20T10:00:00Z",
        },
        {
            "external_id": "SUP-200",
            "name": "Fornecedor Beta",
            "tax_id": "00000000000200",
            "risk_flags": DEFAULT_RISK_FLAGS,
            "updated_at": "2026-01-22T12:00:00Z",
        },
        {
            "external_id": "SUP-300",
            "name": "Fornecedor Gamma",
            "tax_id": "00000000000300",
            "risk_flags": DEFAULT_RISK_FLAGS,
            "updated_at": "2026-01-25T09:30:00Z",
        },
    ],
    "purchase_request": [
        {
            "external_id": "PR-1001",
            "number": "SR-1001",
            "status": "pending_rfq",
            "priority": "high",
            "requested_by": "Joao",
            "department": "Manutencao",
            "needed_at": "2026-02-10",
            "num_cot": "1001",
            "num_pct": "5001",
            "dat_efc": "2026-01-25",
            "updated_at": "2026-01-18T08:00:00Z",
        },
        {
            "external_id": "PR-1002",
            "number": "SR-1002",
            "status": "in_rfq",
            "priority": "urgent",
            "requested_by": "Maria",
            "department": "Operacoes",
            "needed_at": "2026-02-05",
            "num_cot": "1002",
            "num_pct": "5002",
            "dat_efc": "2026-01-28",
            "updated_at": "2026-01-21T14:15:00Z",
        },
        {
            "external_id": "PR-1003",
            "number": "SR-1003",
            "status": "awarded",
            "priority": "medium",
            "requested_by": "Paulo",
            "department": "Compras",
            "needed_at": "2026-02-20",
            "num_cot": "1003",
            "num_pct": "5003",
            "dat_efc": "2026-01-30",
            "updated_at": "2026-01-26T09:45:00Z",
        },
    ],
    "quote": [
        {
            "external_id": "COT-1001-1",
            "num_cot": "1001",
            "num_pct": "5001",
            "cod_for": "SUP-100",
            "dat_cot": "2026-02-01",
            "hor_cot": "10:00",
            "updated_at": "2026-02-01T10:00:00Z",
        },
        {
            "external_id": "COT-1001-2",
            "num_cot": "1001",
            "num_pct": "5001",
            "cod_for": "SUP-200",
            "dat_cot": "2026-02-01",
            "hor_cot": "14:30",
            "updated_at": "2026-02-01T14:30:00Z",
        },
        {
            "external_id": "COT-1002-1",
            "num_cot": "1002",
            "num_pct": "5002",
            "cod_for": "SUP-300",
            "dat_cot": "2026-02-02",
            "hor_cot": "09:15",
            "updated_at": "2026-02-02T09:15:00Z",
        },
    ],
    "purchase_order": [
        {
            "external_id": "SENIOR-OC-000001",
            "number": "OC-2001",
            "status": "erp_accepted",
            "supplier_name": "Fornecedor Alpha",
            "currency": "BRL",
            "total_amount": 12450.90,
            "updated_at": "2026-01-27T12:00:00Z",
        },
        {
            "external_id": "SENIOR-OC-000002",
            "number": "OC-2002",
            "status": "partially_received",
            "supplier_name": "Fornecedor Beta",
            "currency": "BRL",
            "total_amount": 9870.00,
            "updated_at": "2026-01-28T08:20:00Z",
        },
        {
            "external_id": "SENIOR-OC-000003",
            "number": "OC-2003",
            "status": "received",
            "supplier_name": "Fornecedor Gamma",
            "currency": "BRL",
            "total_amount": 6500.00,
            "updated_at": "2026-01-29T09:00:00Z",
        },
    ],
    "receipt": [
        {
            "external_id": "REC-1001",
            "purchase_order_external_id": "SENIOR-OC-000002",
            "status": "partially_received",
            "received_at": "2026-01-28T10:00:00Z",
            "updated_at": "2026-01-28T10:00:00Z",
        },
        {
            "external_id": "REC-1002",
            "purchase_order_external_id": "SENIOR-OC-000003",
            "status": "received",
            "received_at": "2026-01-29T10:30:00Z",
            "updated_at": "2026-01-29T10:30:00Z",
        },
    ],
    "quote_process": [
        {
            "external_id": "PCT-5001",
            "NumPct": "5001",
            "DatAbe": "2026-01-24",
            "updated_at": "2026-01-24T08:00:00Z",
        },
        {
            "external_id": "PCT-5002",
            "NumPct": "5002",
            "DatAbe": "2026-01-27",
            "updated_at": "2026-01-27T08:00:00Z",
        },
    ],
    "quote_supplier": [
        {
            "external_id": "FPC-5001-SUP-100",
            "NumPct": "5001",
            "CodFor": "SUP-100",
            "updated_at": "2026-01-24T08:00:00Z",
        },
        {
            "external_id": "FPC-5001-SUP-200",
            "NumPct": "5001",
            "CodFor": "SUP-200",
            "updated_at": "2026-01-24T08:10:00Z",
        },
    ],
}


def push_purchase_order(purchase_order: dict) -> dict:
    purchase_order_id = purchase_order.get("id") or 0
    external_id = f"SENIOR-OC-{int(purchase_order_id):06d}"
    return {
        "external_id": external_id,
        "status": "erp_accepted",
        "message": "Ordem enviada e aceita no ERP (simulado).",
    }


def fetch_erp_records(
    entity: str,
    since_updated_at: str | None,
    since_id: str | None,
    limit: int = 100,
) -> List[dict]:
    records = list(ERP_DATA.get(entity, []))
    records.sort(key=lambda item: (item["updated_at"], item["external_id"]))

    if since_updated_at:
        filtered: List[dict] = []
        for record in records:
            updated_at = record["updated_at"]
            external_id = record["external_id"]
            if updated_at > since_updated_at:
                filtered.append(record)
                continue
            if updated_at == since_updated_at and since_id and external_id > since_id:
                filtered.append(record)
        records = filtered

    return records[:limit]
