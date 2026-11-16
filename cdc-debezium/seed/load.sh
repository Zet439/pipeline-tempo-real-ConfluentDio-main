#!/usr/bin/env bash
# Carrega schema + seed + permissões de CDC no Postgres (Neon) e registra a contagem como evidência.
# Pré-requisitos: psql, .env preenchido (PG_*), role cdc_user já criado com REPLICATION.
# Dê `chmod +x` neste arquivo (ou rode com `bash load.sh`) para não cair em "Permission denied".
set -euo pipefail
cd "$(dirname "$0")"
ROOT="$(cd ../.. && pwd)"
[ -f "$ROOT/.env" ] || { echo "Falta o .env na raiz do repositório"; exit 1; }
set -a; source "$ROOT/.env"; set +a
export PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require
# Neon exige SNI. Clientes psql antigos (libpq < 14) não o enviam; o contorno é informar o endpoint
# (primeira parte do host, ex.: ep-xxxx) via PGOPTIONS. Em psql moderno (libpq >= 14) isso é redundante,
# mas inofensivo, então fica aqui para funcionar nos dois casos.
export PGOPTIONS="endpoint=${PG_HOST%%.*}"
# PG_ADMIN_USER é o NOME do usuário do Postgres (ex.: cdc_user ou o owner do Neon), nunca o hostname.
# Preencher com o hostname gera "password authentication failed for user '<hostname>'".
PSQL=(psql -h "$PG_HOST" -p "${PG_PORT:-5432}" -U "$PG_ADMIN_USER" -d "$PG_DB" -v ON_ERROR_STOP=1)

[ -f 02_seed.sql ] || python3 generate_seed.py > 02_seed.sql
"${PSQL[@]}" -f 00_schema.sql
"${PSQL[@]}" -f 02_seed.sql
"${PSQL[@]}" -f 01_cdc_setup.sql
"${PSQL[@]}" -c "SELECT (SELECT count(*) FROM customers) AS customers, (SELECT count(*) FROM accounts) AS accounts, (SELECT count(*) FROM cards) AS cards, (SELECT count(*) FROM merchants) AS merchants, (SELECT count(*) FROM transactions) AS transactions;" | tee "$ROOT/evidencias/02-seed-contagem.txt"
