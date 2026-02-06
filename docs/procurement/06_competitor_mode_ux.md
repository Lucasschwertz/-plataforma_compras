# Modo Concorrente UX

## Objetivo
Definir um modelo de produto equivalente ao padrao de e-procurement, com identidade propria da Plataforma Compras.

## Principios
- Plataforma modular, com areas claras por contexto.
- Linguagem de negocio em portugues para usuario final.
- Fluxo orientado por acao, com proxima decisao visivel.
- Dados com origem explicita quando relevante (ERP ou local).
- Complexidade progressiva, resumo primeiro e detalhe sob demanda.

## Arquitetura visual alvo
- `Dashboard`
  - Visao executiva do ciclo
  - Atalhos para operacao diaria
  - Alertas de risco e prioridade
- `Operacoes`
  - Inbox
  - Solicitacoes
  - Cotacoes
  - Decisoes
  - Ordens de compra
- `Integracoes`
  - Sincronizacoes
  - Eventos
  - Falhas e retries
- `Governanca`
  - Auditoria
  - Regras de estado
  - Indicadores de compliance

## Linguagem oficial
- `rfq` -> `Cotacao`
- `award` -> `Decisao`
- `purchase_request` -> `Solicitacao`
- `purchase_order` -> `Ordem de compra`

## Entregas do ciclo atual
1. Layout base modular com navegacao por areas.
2. Dashboard inicial orientado ao ciclo de compras.
3. Mapa central de labels para garantir consistencia de linguagem.

## Proximos incrementos
1. Analytics de fornecedor com comparativos por periodo.
2. Lista de aprovacoes por papel com filtros operacionais.
3. Painel de excecoes ERP com resolucao guiada.
