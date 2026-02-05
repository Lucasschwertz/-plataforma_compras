# Deploy no Render (app somente)

Este guia publica o app no Render usando Postgres externo (ex.: Neon). O Render nao hospeda o banco.

## Pre-requisitos
- DATABASE_URL externo acessivel pela internet (Neon recomendado).
- SECRET_KEY definido.

## Passo a passo (Render)
1. Crie um Web Service e selecione este repositorio.
2. Render vai detectar o `render.yaml` automaticamente.
3. Configure as variaveis em **Environment**:
   - DATABASE_URL (obrigatorio)
   - SECRET_KEY (obrigatorio)
   - ERP_MODE (opcional, default: mock)
   - ERP_BASE_URL, ERP_TOKEN (somente se ERP_MODE=real)
   - RFQ_SLA_DAYS (opcional)
4. Deploy.

## Observacoes
- Use a connection string **pooled** do Neon.
- Garanta `sslmode=require` no DATABASE_URL.
- Health check: `/health`.

## Teste rapido
- Acesse `/health` e confirme status 200.
- Acesse `/procurement/inbox`.
