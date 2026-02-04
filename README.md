# Plataforma Compras

- SLA de RFQ documentado em docs/domain/sla.md

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
- ERP_BASE_URL
- ERP_TOKEN

Variaveis recomendadas:
- ERP_TIMEOUT_SECONDS
- ERP_RETRY_ATTEMPTS
- ERP_RETRY_BACKOFF_MS
- RFQ_SLA_DAYS
- LOG_LEVEL
- DATABASE_READ_URL

Deploy (Docker Compose):
- docs/deploy/docker-compose.md

Operacao:
- docs/ops/backups.md
- docs/ops/read_replica.md
