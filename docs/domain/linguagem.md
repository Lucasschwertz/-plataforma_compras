# Linguagem da Plataforma

## Objetivo
Padronizar a linguagem exibida ao usuario final e separar termos tecnicos de implementacao.

## Regra de produto
- A interface mostra termos de negocio em portugues.
- Backend, banco e contratos de API podem manter nomes tecnicos em ingles para estabilidade.

## Glossario oficial
- `rfq` -> `Cotacao`
- `award` -> `Decisao`
- `purchase_request` -> `Solicitacao`
- `purchase_order` -> `Ordem de compra`
- `receipt` -> `Recebimento`
- `supplier` -> `Fornecedor`

## Regra de implementacao UI
- Toda label de status, entidade e escopo deve sair do mapa central `app/static/js/ui_labels.js`.
- Evitar hardcode de label em scripts de tela.
