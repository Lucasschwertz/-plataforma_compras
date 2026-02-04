# RFQ SLA - Service Level Agreement

## Objetivo
Garantir tempo maximo de resposta dos fornecedores em RFQs ativas.

## Escopo
Entidade: RFQ  
Isolamento: por tenant_id

## Inicio do SLA
Evento: rfq_created  
Timestamp: rfq.created_at

## Atualizacao de atividade
Evento: supplier_quote_received  
Timestamp: supplier_quote.created_at

## Regra de violacao
Se (now - last_supplier_activity_at) > RFQ_SLA_DAYS

## Configuracao
RFQ_SLA_DAYS:
- default: 5
- 0: SLA desativado

## Observacoes
- SLA e informativo (nao bloqueante)
- Nao gera automacao nem penalidade
- Em modo mock, timestamps sao locais
- Em modo ERP, inicio usa E405SOL.DatEfc e ultima proposta usa E410COT.DatCot + E410COT.HorCot
