# RFQ SLA – Service Level Agreement

## Objetivo
Garantir tempo máximo de resposta dos fornecedores em RFQs ativas.

## Escopo
Entidade: RFQ  
Isolamento: por tenant_id

## Início do SLA
Evento: rfq_created  
Timestamp: rfq.created_at

## Atualização de atividade
Evento: supplier_quote_received  
Timestamp: supplier_quote.created_at

## Regra de violação
Se (now - last_supplier_activity_at) > RFQ_SLA_DAYS

## Configuração
RFQ_SLA_DAYS:
- default: 5
- 0: SLA desativado

## Observações
- SLA é informativo (não bloqueante)
- Não gera automação nem penalidade
- Em modo mock, timestamps são locais
- Em modo ERP, timestamps serão mapeados futuramente
