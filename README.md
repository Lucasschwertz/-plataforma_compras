# Plataforma Compras

- SLA de Cotacao (RFQ) documentado em docs/domain/sla.md
- Linguagem da interface documentada em docs/domain/linguagem.md
- Fluxo ERP SR -> Cotacao -> OC documentado em docs/domain/erp_sr_rfq_oc.md
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

3. Aplicar migrations (schema previsivel):

```powershell
$env:FLASK_APP="run.py"
flask db upgrade
```

4. (Opcional) rollback de migration:

```powershell
flask db downgrade -1
```

5. Criar o espelho completo de tabelas ERP:

```powershell
$env:PYTHONPATH='.'
python database\import_erp_csv.py --schema tabelas.csv --mirror-schema-only
```

6. Importar dados ERP (espelho + dominio):

```powershell
$env:PYTHONPATH='.'
python database\import_erp_csv.py --schema tabelas.csv `
  --e405sol "C:\plataforma_compras\lista solicitações e405sol.csv" `
  --e410cot "C:\plataforma_compras\lista solicitações e410cot.csv" `
  --e410pct "C:\plataforma_compras\lista solicitações e410pct.csv" `
  --e410fpc "C:\plataforma_compras\lista solicitações e410fpc.csv" `
  --e420ocp "C:\plataforma_compras\lista solicitações e420ocp.csv" `
  --e420ipo "C:\plataforma_compras\lista solicitações e420ipo.csv" `
  --e440nfc "C:\plataforma_compras\lista solicitações e440nfc.csv" `
  --e440ipc "C:\plataforma_compras\lista solicitações e440ipc.csv"
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
- LOG_JSON
- DATABASE_READ_URL
- RATE_LIMIT_ENABLED
- RATE_LIMIT_WINDOW_SECONDS
- RATE_LIMIT_MAX_REQUESTS
- CSRF_ENABLED
- SECURITY_HEADERS_ENABLED

Integracao ERP por modo:
- `ERP_MODE=mock`: usa dados simulados internos.
- `ERP_MODE=senior`: exige `ERP_BASE_URL` e `ERP_TOKEN` (ou `ERP_API_KEY`).
- `ERP_MODE=senior_csv`: usa bridge de CSV com sync incremental por watermark.
Arquivos principais do bridge CSV:
- `ERP_CSV_SCHEMA`, `ERP_CSV_E405SOL`, `ERP_CSV_E410COT`, `ERP_CSV_E410PCT`, `ERP_CSV_E410FPC`
- opcionais para etapa OC e recebimento: `ERP_CSV_E420OCP`, `ERP_CSV_E420IPO`, `ERP_CSV_E440NFC`, `ERP_CSV_E440IPC`, `ERP_CSV_E440ISC`

Worker de integracao ERP (outbox assincrono):
- A rota de envio de OC apenas registra a intencao e coloca na fila interna (`sync_runs` com `scope=purchase_order`).
- Para processar pendencias execute worker separado:

```powershell
python -m app.workers.erp_outbox_worker --once
python -m app.workers.erp_outbox_worker
```

- Configuracoes:
  - `ERP_OUTBOX_MAX_ATTEMPTS` (default: `4`)
  - `ERP_OUTBOX_BACKOFF_SECONDS` (default: `30`)
  - `ERP_OUTBOX_MAX_BACKOFF_SECONDS` (default: `600`)
  - `ERP_OUTBOX_WORKER_INTERVAL_SECONDS` (default: `5`)
  - `ERP_OUTBOX_WORKER_BATCH_SIZE` (default: `25`)

Observabilidade e hardening:
- Logs estruturados em JSON para web e worker (com `request_id`).
- `/health` inclui status da fila/worker ERP e metricas HTTP basicas.
- Headers de seguranca habilitados por padrao.
- Rate limiting simples em memoria (configuravel por janela/limite).
- CSRF para formularios de autenticacao (`/login` e `/register`).

Deploy (Docker Compose):
- docs/deploy/docker-compose.md
Deploy (Render app-only):
- docs/deploy/render.md

Operacao:
- docs/ops/backups.md
- docs/ops/read_replica.md

## UX
- Estilo visual: minimalista (aplicado nas telas de Inbox, Cotacao e Decisao/OC).

## Startup de Banco

- Por padrao o app **nao** cria schema em runtime.
- Em desenvolvimento, se quiser auto-init explicito:

```powershell
$env:FLASK_ENV="development"
$env:DB_AUTO_INIT="1"
python run.py
```

- Em testes (`TESTING=True`) o auto-init continua habilitado para manter isolamento dos testes temporarios.

## Qualidade e CI

Workflow: `.github/workflows/ci.yml`

Como a CI funciona:
- Dispara em `push` e `pull_request` para `main`.
- Usa Python `3.11` (mesma base do `Dockerfile`).
- Instala dependencias com `requirements.txt`.
- Valida migrations com `flask db upgrade` e `flask db downgrade base`.
- Executa toda a suite com `unittest`:
  `python -m unittest discover -s tests -p "test_*.py" -v`
- Falha o pipeline se qualquer teste falhar.
- Executa coverage em etapa nao bloqueante (somente relatorio).
- Publica artefatos de coverage (`coverage.xml`, `coverage.txt`, `htmlcov`).

Ambiente de teste seguro na CI:
- `ERP_MODE=mock` para evitar ERP real.
- `SYNC_SCHEDULER_ENABLED=false` para evitar jobs assincronos.
- Sem uso de secrets obrigatorios.
- Banco de teste em SQLite (configuracao padrao dos testes com DB temporario).

Como rodar testes localmente:

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
python -m unittest discover -s tests -p "test_*.py" -v
```

Como rodar coverage localmente:

```powershell
pip install coverage
coverage run -m unittest discover -s tests -p "test_*.py" -v
coverage report -m
coverage html -d htmlcov
```

Obs: coverage nao bloqueia CI neste momento; o objetivo atual e visibilidade continua de qualidade.
