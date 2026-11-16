# teardown.sh: derruba TUDO, na ordem certa, e verifica que não sobrou nada cobrando.
# A ordem importa: statements Flink e conector primeiro (param a cobrança por CFU/task-hora e liberam
# o slot de replicação), depois Postgres, chaves, service accounts e por fim o environment.
# Uso: ./teardown.sh        (pede a palavra APAGAR; use --yes para pular a pergunta)
# Pré-requisitos: Confluent CLI logado, psql, python3, .env preenchido. Dê `chmod +x teardown.sh`
# na primeira vez para evitar "Permission denied" do shell.
set -euo pipefail

[ -f ./.env ] || { echo "Falta o .env"; exit 1; }
set -a; source ./.env; set +a
: "${CONFLUENT_ENV_ID:?CONFLUENT_ENV_ID vazio no .env}"
: "${CONFLUENT_CLUSTER_ID:?CONFLUENT_CLUSTER_ID vazio no .env}"
EVID="${EVID_DIR:-evidencias}"; mkdir -p "$EVID"
LOG="$EVID/07-teardown.txt"
: > "$LOG"
say() { echo "$@" | tee -a "$LOG"; }

if [ "${1:-}" != "--yes" ]; then
  echo "Isto APAGA o environment $CONFLUENT_ENV_ID (cluster, conector, Schema Registry, Flink) e as service accounts."
  read -r -p "Digite APAGAR para continuar: " ok
  [ "$ok" = "APAGAR" ] || { echo "Cancelado."; exit 1; }
fi

say ">> 1/6 Parar statements Flink (cobram CFU enquanto rodam)"
for pool in $(confluent flink compute-pool list --environment "$CONFLUENT_ENV_ID" -o json \
    | python3 -c 'import sys,json; [print(p["id"]) for p in json.load(sys.stdin)]'); do
  for st in $(confluent flink statement list --environment "$CONFLUENT_ENV_ID" --compute-pool "$pool" -o json \
      | python3 -c 'import sys,json; [print(s["name"]) for s in json.load(sys.stdin)]'); do
    confluent flink statement delete "$st" --environment "$CONFLUENT_ENV_ID" --force | tee -a "$LOG" || true
  done
done

say ">> 2/6 Remover conectores"
for id in $(confluent connect cluster list --environment "$CONFLUENT_ENV_ID" --cluster "$CONFLUENT_CLUSTER_ID" -o json \
    | python3 -c 'import sys,json; [print(c["id"]) for c in json.load(sys.stdin)]'); do
  confluent connect cluster delete "$id" --environment "$CONFLUENT_ENV_ID" --cluster "$CONFLUENT_CLUSTER_ID" --force | tee -a "$LOG"
done

say ">> 3/6 Remover o replication slot e a publication no Postgres (slot órfão retém WAL e enche o disco)"
# PGSSLMODE=require: o Neon exige TLS. PGOPTIONS "endpoint=" só é necessário em libpq antigo (< 14),
# que não envia SNI; em psql moderno é redundante e inofensivo (ver cdc-debezium/seed/load.sh).
# PG_ADMIN_USER precisa ser o NOME do usuário (ex.: cdc_user), não o hostname do Neon.
export PGPASSWORD="$PG_ADMIN_PASSWORD" PGSSLMODE=require PGOPTIONS="endpoint=${PG_HOST%%.*}"
PSQL=(psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_ADMIN_USER" -d "$PG_DB" -v ON_ERROR_STOP=1 -t -A)
for i in 1 2 3 4 5 6; do   # tenta algumas vezes: o slot só libera depois que o conector fecha a conexão
  "${PSQL[@]}" -c "SELECT pg_drop_replication_slot('desafio_slot') FROM pg_replication_slots WHERE slot_name='desafio_slot';" >/dev/null 2>&1 && break
  sleep 10
done
"${PSQL[@]}" -c "DROP PUBLICATION IF EXISTS desafio_pub;"
say "Slots restantes no Postgres: $("${PSQL[@]}" -c "SELECT count(*) FROM pg_replication_slots WHERE slot_name='desafio_slot';")"

say ">> 4/6 Apagar API keys (Cloud, Tableflow e Kafka). Chaves já apagadas dão erro, ignorado de propósito."
# As API keys pertencem à organização e sobrevivem ao environment, por isso são apagadas aqui à parte.
for k in "${CLOUD_API_KEY:-}" "${TABLEFLOW_API_KEY:-}" "${KAFKA_API_KEY:-}"; do
  [ -n "$k" ] && { confluent api-key delete "$k" --force | tee -a "$LOG" || true; }
done

say ">> 5/6 Apagar as service accounts (pertencem à organização: sobrevivem ao environment)"
for sa in "${SA_PRODUCER_ID:-}" "${SA_CONSUMER_ID:-}"; do
  [ -n "$sa" ] && { confluent iam service-account delete "$sa" --force | tee -a "$LOG" || true; }
done

say ">> 6/6 Deletar o environment (leva cluster, Schema Registry, compute pools e tópicos)"
confluent environment delete "$CONFLUENT_ENV_ID" --force | tee -a "$LOG"

say ">> Verificação (não deve restar nada do projeto)"
say "--- confluent environment list"; confluent environment list | tee -a "$LOG"
say "--- confluent iam service-account list"; confluent iam service-account list | tee -a "$LOG"
say "--- confluent api-key list"; confluent api-key list | tee -a "$LOG"
# A cobrança leva alguns dias para fechar; confira depois para garantir que nada continua custando.
say "Teardown concluído. Confira os custos em 2-3 dias: confluent billing cost list --start-date 2026-10-27 --end-date <hoje>"
