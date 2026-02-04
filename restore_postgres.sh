#!/bin/bash

set -euo pipefail

if [ $# -lt 1 ]; then
  echo "Uso: ./restore_postgres.sh <arquivo_backup.sql>"
  exit 1
fi

BACKUP_FILE="$1"

if [ ! -f "$BACKUP_FILE" ]; then
  echo "Arquivo nao encontrado: $BACKUP_FILE"
  exit 1
fi

cat "$BACKUP_FILE" | docker exec -i plataforma_compras_db psql -U compras compras
