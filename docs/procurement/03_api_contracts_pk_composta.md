# API Contracts (Fase 1) com PK composta

Regra obrigatória: `tenant_id` vem do contexto autenticado (token/sessão) e NUNCA do cliente.

Todas as operações devem filtrar por `(tenant_id, id)`.

Exemplo de padrão de query segura:

```sql
select *
from rfqs
where tenant_id = $1 and id = $2;
```

## GET /api/inbox

Query params:
- `limit` (default 50)
- `offset` (default 0)

Response:

```json
{
  "items": [
    {
      "type": "purchase_request",
      "id": "9c2c6d3a-2c5e-4efb-9d6f-7b6d4a1a1d01",
      "ref": "SR-10231",
      "status": "pending_rfq",
      "priority": "high",
      "needed_at": "2026-02-10",
      "age_days": 5
    }
  ],
  "kpis": {
    "pending_rfq": 12,
    "awaiting_quotes": 7,
    "awarded_waiting_po": 3,
    "awaiting_erp_push": 2
  },
  "paging": {
    "limit": 50,
    "offset": 0,
    "has_more": true
  }
}
```

## POST /api/rfqs

Request:

```json
{
  "title": "Compra emergencial - rolamentos",
  "purchase_request_item_ids": [
    "4f4b3b2a-8b7b-4c0a-9c4f-59b3a3ab1111"
  ],
  "supplier_ids": [
    "2a6f2c70-1b8d-4a5d-9b8e-5f1111111111"
  ]
}
```

Response:

```json
{
  "id": "8d9f0a0a-6d63-4c1e-b0a1-222222222222",
  "status": "open",
  "created_at": "2026-01-25T14:10:00Z"
}
```

## GET /api/rfqs/{id}/comparison

Notas:
- `suggested_supplier_id` é opcional e deve ser baseado em regra simples e explícita.
- Exemplo de regra Fase 1: menor `total` com desempate por menor `lead_time_days`.

Response:

```json
{
  "rfq_id": "8d9f0a0a-6d63-4c1e-b0a1-222222222222",
  "items": [
    {
      "rfq_item_id": "c1",
      "description": "Rolamento XYZ",
      "quantity": 10,
      "quotes": [
        {
          "supplier_id": "s1",
          "unit_price": 100.0,
          "lead_time_days": 7,
          "total": 1000.0
        },
        {
          "supplier_id": "s2",
          "unit_price": 92.0,
          "lead_time_days": 12,
          "total": 920.0
        }
      ],
      "suggested_supplier_id": "s2",
      "suggestion_reason": "menor total"
    }
  ],
  "suppliers": [
    {
      "supplier_id": "s1",
      "name": "Fornecedor A",
      "risk_flags": {
        "no_supplier_response": false,
        "late_delivery": false,
        "sla_breach": false
      }
    }
  ]
}
```

## POST /api/rfqs/{id}/award

Request:

```json
{
  "supplier_id": "2a6f2c70-1b8d-4a5d-9b8e-5f1111111111",
  "reason": "Melhor prazo mantendo preço dentro da meta"
}
```

Response:

```json
{
  "award_id": "a7f0f0f0-1111-4444-8888-333333333333",
  "rfq_id": "8d9f0a0a-6d63-4c1e-b0a1-222222222222",
  "status": "awarded"
}
```

## POST /api/purchase-orders/from-award/{awardId}

Request:

```json
{
  "notes": "Gerada automaticamente a partir do award"
}
```

Response:

```json
{
  "purchase_order_id": "po-44444444-aaaa-bbbb-cccc-555555555555",
  "status": "draft"
}
```

## POST /api/purchase-orders/{id}/push-to-erp

Request:

```json
{
  "mode": "async"
}
```

Response:

```json
{
  "purchase_order_id": "po-44444444-aaaa-bbbb-cccc-555555555555",
  "erp_push": {
    "status": "queued",
    "sync_run_id": "sr-66666666-aaaa-bbbb-cccc-777777777777"
  }
}
```