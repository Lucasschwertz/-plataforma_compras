#!/bin/bash

set -euo pipefail

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

docker exec plataforma_compras_db \
  pg_dump -U compras compras > "backup_${TIMESTAMP}.sql"
