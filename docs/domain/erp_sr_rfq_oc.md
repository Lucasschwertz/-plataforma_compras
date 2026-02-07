# Fluxo ERP Senior, Solicitacao ate Ordem de Compra

## Objetivo
Definir o fluxo oficial do produto para compras, mantendo fidelidade ao processo ERP Senior e linguagem de negocio na plataforma.

## Fluxo macro
1. Solicitacao de compra (SR)
2. Processo de cotacao
3. Cotacao por fornecedor
4. Analise e decisao
5. Geracao da ordem de compra

## 1) Solicitacao de compra
Tabela principal: `E405SOL`

Cada registro representa item de solicitacao, nao apenas cabecalho.

Chave:
- `CodEmp`
- `NumSol`
- `SeqSol`

Campos chave para produto:
- `NumSol`, `SeqSol`
- `ProSer`, `CodPro`, `CodDer`, `CodSer`
- `UniMed`, `QtdSol`, `QtdApr`
- `FilSol`, `DatSol`, `DatEfc`
- `ObsSol`

Regra de negocio:
- Enquanto o item esta somente na `E405SOL`, nao existe fornecedor nem preco.
- `DatEfc` e marco natural para inicio do SLA quando houver envio para cotacao.

## 2) Processo de cotacao
Tabela principal: `E410PCT`

Papel:
- Cabecalho do processo de cotacao.
- Agrupa itens de solicitacoes e controla parametros gerais do processo.

Chave:
- `CodEmp`
- `NumPct`

## 3) Fornecedores habilitados no processo
Tabela principal: `E410FPC`

Papel:
- Lista de fornecedores autorizados a participar do processo.

Chave:
- `CodEmp`
- `NumPct`
- `CodFor`

Campos chave:
- `CodFor`
- `EmaCot`
- `NotAfo`

Regra de negocio:
- Sem registro em `E410FPC`, fornecedor nao participa daquela cotacao.

## 4) Cotacao por fornecedor
Tabela principal: `E410COT`

Papel:
- Proposta por item e por fornecedor, contendo preco, prazo e informacoes fiscais.

Chave:
- `CodEmp`
- `NumCot`
- `SeqCot`

Campos chave:
- `NumCot`, `CodFor`, `SeqCot`
- `ProSer`, `CodPro`, `CodDer`, `CodSer`
- `QtdCot`, `PreCot`
- `DatCot`, `HorCot`
- `DatPrv`, `PrzEnt`

Regra de negocio:
- `DatCot + HorCot` e referencia principal para ultima atividade de fornecedor no SLA.

## 5) Analise e decisao
Papel:
- Decisao humana sobre fornecedor vencedor por item ou por escopo, conforme politica.
- O ERP registra resultado, nao substitui decisao do comprador.

Entrada principal de decisao:
- Propostas consolidadas de `E410COT`.

## 6) Ordem de compra
Cabecalho: `E420OCP`  
Itens: `E420IPO`

Papel:
- Materializar a decisao de compra.

Chaves:
- `E420OCP`: `CodEmp`, `NumOcp`
- `E420IPO`: `CodEmp`, `NumOcp`, `SeqIpo`

Campos chave dos itens:
- `CodPro`, `CodDer`
- `QtdPed`, `PreUni`, `DatPrv`
- Vinculos com origem de cotacao e solicitacao quando disponiveis

## Relacao das etapas
- Solicitacao: `E405SOL`
- Processo de cotacao: `E410PCT`
- Fornecedores habilitados: `E410FPC`
- Propostas: `E410COT`
- Decisao: camada de aplicacao
- Ordem de compra: `E420OCP` + `E420IPO`

## Tradução para o modelo da plataforma
- `purchase_requests` <- `E405SOL`
- `rfqs` e `rfq_items` <- `E410PCT` e itens vinculados
- `quotes` e `quote_items` <- `E410COT`
- `awards` <- decisao humana com rastreabilidade
- `purchase_orders` <- `E420OCP` e `E420IPO`

## Diretriz de produto
- A plataforma replica o processo funcional do ERP, com UX superior e rastreabilidade ampliada.
- A estrutura fisica do banco da plataforma pode diferir do ERP, desde que preserve o fluxo de negocio acima.
