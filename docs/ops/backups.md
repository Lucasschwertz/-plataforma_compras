# Backups (PostgreSQL)

## Objetivo
Garantir backup diario e restore simples sem custo adicional.

## Como gerar backup

```bash
bash backup_postgres.sh
```

Isso cria um arquivo `backup_YYYYMMDD_HHMMSS.sql` no diretorio atual.

## Como restaurar

```bash
bash restore_postgres.sh backup_YYYYMMDD_HHMMSS.sql
```

## Politica recomendada
- Diario
- Retencao 7/30/90
- Teste de restore mensal
