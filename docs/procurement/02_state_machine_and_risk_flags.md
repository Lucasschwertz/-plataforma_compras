# Procurement Fase 1: State Machines, Entities/Scopes e Risk Flags

Principios obrigatorios neste MVP:
- ERP Senior e a fonte da verdade.
- O backend e o unico ponto de integracao.
- Multi-tenant forte desde o inicio (sempre filtrar por tenant_id).

## Entity e Scope padrao (lista fechada)

Padronizacao: usar a mesma lista (sempre singular) em:
- status_events.entity
- sync_runs.scope
- integration_watermarks.entity
- erp_links.entity

Valores permitidos (`integration_entity`):
- purchase_request
- rfq
- award
- purchase_order
- receipt
- supplier
- category

Regras praticas:
- Nao usar plural como scope tecnico (ex: evitar purchase_orders).
- Entity/scope representa recurso de dominio, nao tipo de consulta.
- O filtro por tenant_id vem do contexto autenticado, nunca do cliente.

## State Machines (Fase 1)

### Purchase Request (pr_status)
Fluxo principal:
- pending_rfq -> in_rfq -> awarded -> ordered -> partially_received -> received

Cancelamento:
- Pode ir para cancelled a partir de qualquer estado nao final.

### RFQ (rfq_status)
Fluxo principal:
- draft -> open -> collecting_quotes -> awarded

Encerramento sem award:
- draft|open|collecting_quotes -> closed

Cancelamento:
- draft|open|collecting_quotes -> cancelled

### Award (award_status)
Fluxo principal:
- awarded -> converted_to_po

Cancelamento:
- awarded -> cancelled (somente antes de gerar PO)

### Purchase Order (po_status)
Fluxo principal:
- draft -> approved -> sent_to_erp -> erp_accepted -> partially_received -> received

Erro de integracao:
- sent_to_erp -> erp_error

Cancelamento:
- draft|approved|erp_error -> cancelled

Nota sobre fonte da verdade:
- O estado efetivo deve convergir para o que vier do ERP Senior.
- Eventos locais servem para UX/auditoria, mas devem ser reconciliados via sync.

## Contrato minimo de risk_flags

`risk_flags` e um objeto JSON associado ao fornecedor (supplier) e deve ser estavel na Fase 1.

Chaves obrigatorias (todas booleanas):
- no_supplier_response
- late_delivery
- sla_breach

Regras de compatibilidade:
- As chaves obrigatorias nunca devem ser removidas ou renomeadas.
- Novas chaves podem ser adicionadas sem quebrar clientes existentes.
- Na duvida, default = false.

Exemplo canonico:

```json
{
  "no_supplier_response": false,
  "late_delivery": false,
  "sla_breach": false
}
```

## Watermarks e Idempotencia (Fase 1)

- integration_watermarks e o cursor incremental por tenant_id + system + entity.
- Atualizar somente apos sync bem-sucedido (inbound ou outbound).
- last_success_source_updated_at + last_success_source_id garantem ordenacao e desempate.
- sync_runs e auditoria, nao watermark.
- Reexecucoes devem ser idempotentes: nao criar novos sync_runs se o status final ja foi alcancado, nem regredir watermark.

## Sync incremental (mock no MVP)

Endpoint:
- POST /api/procurement/integrations/sync?scope={entity}

Scopes suportados neste MVP:
- supplier
- purchase_request

Payload opcional:
```json
{ "limit": 100 }
```

Resposta:
```json
{
  "status": "succeeded",
  "scope": "supplier",
  "sync_run_id": 1,
  "result": {
    "records_in": 3,
    "records_upserted": 3
  }
}
```
