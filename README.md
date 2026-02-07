# Plataforma Compras

- SLA de Cotacao (RFQ) documentado em docs/domain/sla.md
- Linguagem da interface documentada em docs/domain/linguagem.md
- Modelo UX concorrente documentado em docs/procurement/06_competitor_mode_ux.md

## Banco de Dados (PostgreSQL)

1. Criar o database:

```powershell
$env:PGHOST="localhost"
$env:PGPORT="5432"
$env:PGUSER="postgres"
$env:PGPASSWORD="SUA_SENHA"
$env:PGDATABASE="portal_compras"
python database\create_postgres_db.py
```

2. Configurar a URL:

```powershell
$env:DATABASE_URL="postgresql://postgres:SUA_SENHA@localhost:5432/portal_compras"
```

3. Inicializar o schema:

```powershell
python database\init_db.py
```

4. Importar dados ERP (CSV):

```powershell
$env:PYTHONPATH='.'
python database\import_erp_csv.py --schema tabelas.csv `
  --e405sol "C:\plataforma_compras\lista solicitações e405sol.csv" `
  --e410cot "C:\plataforma_compras\lista solicitações e410cot.csv" `
  --e410pct "C:\plataforma_compras\lista solicitações e410pct.csv" `
  --e410fpc "C:\plataforma_compras\lista solicitações e410fpc.csv"
```

## Production

Variaveis obrigatorias:
- DATABASE_URL
- SECRET_KEY
- ERP_MODE

Variaveis recomendadas:
- ERP_TIMEOUT_SECONDS
- ERP_RETRY_ATTEMPTS
- ERP_RETRY_BACKOFF_MS
- RFQ_SLA_DAYS
- LOG_LEVEL
- DATABASE_READ_URL

Integracao ERP por modo:
- `ERP_MODE=mock`: usa dados simulados internos.
- `ERP_MODE=senior`: exige `ERP_BASE_URL` e `ERP_TOKEN` (ou `ERP_API_KEY`).
- `ERP_MODE=senior_csv`: usa bridge de CSV com sync incremental por watermark.
Arquivos principais do bridge CSV:
- `ERP_CSV_SCHEMA`, `ERP_CSV_E405SOL`, `ERP_CSV_E410COT`, `ERP_CSV_E410PCT`, `ERP_CSV_E410FPC`
- opcionais para etapa OC e recebimento: `ERP_CSV_E420OCP`, `ERP_CSV_E420IPO`, `ERP_CSV_E440NFC`, `ERP_CSV_E440IPC`, `ERP_CSV_E440ISC`

Deploy (Docker Compose):
- docs/deploy/docker-compose.md
Deploy (Render app-only):
- docs/deploy/render.md

Operacao:
- docs/ops/backups.md
- docs/ops/read_replica.md

## UX
- Estilo visual: minimalista (aplicado nas telas de Inbox, Cotacao e Decisao/OC).
