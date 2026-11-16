# setup.sh: sobe o pipeline, camada por camada.
# Pré-requisitos: Confluent CLI logado (`confluent login`), psql, envsubst, python3, .env preenchido.
# Uso: ./setup.sh fundacao | contrato | ingestao | processamento | operacao | all
#
# Dê permissão de execução antes do primeiro uso: `chmod +x setup.sh`. Baixado via git clone ou
# descompactado de um zip, o arquivo costuma perder o bit de execução e `./setup.sh` falha com
# "Permission denied" (erro do shell, não do Confluent). Alternativa: `bash setup.sh fundacao`.
#
# Legenda de status nos comentários abaixo:
#  comando executado durante o desenvolvimento, com evidência em evidencias/
#  comando escrito a partir da documentação; reexecute e confira antes de confiar
set -euo pipefail

[ -f ./.env ] || { echo "Falta o .env (cp .env.example .env)"; exit 1; }
set -a; source ./.env; set +a
EVID="${EVID_DIR:-evidencias}"   # defina EVID_DIR para gravar as evidências em outra pasta
mkdir -p "$EVID"

# grava/atualiza CHAVE=valor no .env, para persistir os IDs que o Confluent gera (env, cluster, SA).
# Sem isso, os passos seguintes não saberiam a qual environment/cluster se referir.
setenv() {
  local k="$1" v="$2"
  if grep -q "^${k}=" .env; then sed -i "s#^${k}=.*#${k}=${v}#" .env; else echo "${k}=${v}" >> .env; fi
  export "${k}=${v}"
}

fundacao() {
  echo ">> Camada 1: Fundação"
  # cria o environment e um cluster Basic; o ID retornado é salvo no .env por setenv.
  # Basic é o tipo certo para POC de volume baixo (o seed tem ~2000 transações) e cobra só pelo uso.
  confluent environment create "$CONFLUENT_ENV_NAME" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/env_id
  setenv CONFLUENT_ENV_ID "$(cat /tmp/env_id)"; rm -f /tmp/env_id
  confluent environment use "$CONFLUENT_ENV_ID"
  confluent kafka cluster create "$CONFLUENT_CLUSTER_NAME" --cloud "$CONFLUENT_CLOUD" \
    --region "$CONFLUENT_REGION" --type basic -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/cl_id
  setenv CONFLUENT_CLUSTER_ID "$(cat /tmp/cl_id)"; rm -f /tmp/cl_id
  confluent kafka cluster use "$CONFLUENT_CLUSTER_ID"

  # duas identidades por menor privilégio: o conector CDC só escreve, o consumidor só lê.
  # Se a chave do consumidor vazar, ela não consegue gravar nos tópicos; e vice-versa.
  confluent iam service-account create "$SA_PRODUCER_NAME" --description "escreve nos topicos desafio-*" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/sa_p
  confluent iam service-account create "$SA_CONSUMER_NAME" --description "le os topicos desafio-*" -o json \
    | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])' > /tmp/sa_c
  setenv SA_PRODUCER_ID "$(cat /tmp/sa_p)"; setenv SA_CONSUMER_ID "$(cat /tmp/sa_c)"; rm -f /tmp/sa_p /tmp/sa_c

  # ACLs por prefixo "desafio", limitando o alcance aos tópicos do projeto.
  # A 2a linha é obrigatória: o Debezium grava um tópico de heartbeat FORA do prefixo do projeto;
  # sem essa ACL o conector fica FAILED com "not authorized". Damos só a permissão mínima necessária.
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations write,describe,create --topic "$TOPIC_PREFIX" --prefix
  confluent kafka acl create --allow --service-account "$SA_PRODUCER_ID" --operations write,describe,create --topic __debezium-heartbeat --prefix
  confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" --operations read,describe --topic "$TOPIC_PREFIX" --prefix
  confluent kafka acl create --allow --service-account "$SA_CONSUMER_ID" --operations read --consumer-group "$TOPIC_PREFIX" --prefix

  #  API key do produtor. O segredo só é exibido uma vez, então é gravado no .env aqui mesmo.
  # O .env está no .gitignore; nunca versione esse segredo.
  confluent api-key create --resource "$CONFLUENT_CLUSTER_ID" --service-account "$SA_PRODUCER_ID" --description "desafio-producer" -o json > /tmp/k_p.json
  python3 - <<'PY'
import json
k = json.load(open('/tmp/k_p.json'))
with open('.env', 'a') as f:
    f.write(f"\nKAFKA_API_KEY={k['api_key']}\nKAFKA_API_SECRET={k['api_secret']}\n")
PY
  rm -f /tmp/k_p.json

  #  evidência da fundação: as service accounts, as ACLs de cada uma e as API keys.
  # Os secrets nunca aparecem nessas listagens.
  { confluent iam service-account list
    confluent kafka acl list --service-account "$SA_PRODUCER_ID"
    confluent kafka acl list --service-account "$SA_CONSUMER_ID"
    confluent api-key list; } | tee $EVID/01-fundacao.txt
}

contrato() {
  echo ">> Camada 2: Contrato (Schema Registry)"
  # O conector CDC é quem registra os schemas Avro dos tópicos, então rode esta função
  # DEPOIS de `ingestao`, senão o subject ainda não existe.
  #  testes de compatibilidade no modo BACKWARD (padrão do Schema Registry):
  #   adicionar campo opcional com default -> compatível; campo obrigatório sem default -> quebra;
  #   remover campo -> aceito em BACKWARD. Usa `validate`, que não registra versão nem suja o subject.
  S="${TOPIC_PREFIX}.public.customers-value"
  : > $EVID/02-compatibilidade.txt
  for f in add-optional add-required remove-required; do
    echo "=== $f ===" | tee -a $EVID/02-compatibilidade.txt
    confluent schema-registry schema compatibility validate "kafka/schemas/customers-$f.avsc" \
      --type avro --subject "$S" --version latest 2>&1 | tee -a $EVID/02-compatibilidade.txt
  done
}

ingestao() {
  echo ">> Camada 3: Ingestão (CDC)"
  # PASSOS MANUAIS no console do Neon, feitos uma vez antes de rodar:
  #   1) Settings > habilitar replicação lógica (reinicia o compute; é a origem do WAL que o CDC lê)
  #   2) criar o role $CDC_USER COM o atributo REPLICATION. No Neon, role criado só por SQL pode não
  #      receber esse atributo; crie pelo painel de Roles. A senha fica no .env (CDC_PASSWORD).
  # Atenção ao .env: PG_ADMIN_USER deve ser o NOME do usuário (ex.: cdc_user ou o owner do Neon),
  # nunca o hostname. Hostname no lugar do usuário causa "password authentication failed for user ...".
  # carrega schema + seed + publication no Postgres
  ./cdc-debezium/seed/load.sh
  # renderiza cdc.json substituindo as variáveis do .env (SA_PRODUCER_ID, PG_*, etc.) com
  # envsubst, cria o conector a partir do arquivo renderizado e o apaga logo em seguida — assim os
  # segredos (CDC_PASSWORD) não ficam em disco nem são versionados. Rode a partir da raiz do repositório.
  bash -c 'set -a; source ./.env; set +a; envsubst < cdc-debezium/cdc.json > cdc-debezium/cdc.rendered.json'
  confluent connect cluster create --config-file cdc-debezium/cdc.rendered.json \
    --environment "$CONFLUENT_ENV_ID" --cluster "$CONFLUENT_CLUSTER_ID"
  rm -f cdc-debezium/cdc.rendered.json
  confluent connect cluster list --environment "$CONFLUENT_ENV_ID" --cluster "$CONFLUENT_CLUSTER_ID" | tee $EVID/03-conector.txt
  echo "Aguarde o conector ficar RUNNING. Para gerar eventos c/u/d, veja o README (camada 3)."
}

processamento() {
  echo ">> Camada 4: Processamento (Flink SQL)"
  # compute pool com teto de 5 CFU. Os statements cobram CFU enquanto rodam, então
  # pare-os assim que a evidência for gerada (o teardown.sh também os apaga).
  confluent flink compute-pool create desafio-pool --cloud "$CONFLUENT_CLOUD" --region "$CONFLUENT_REGION" \
    --max-cfu 5 --wait | tee $EVID/04-compute-pool.txt
  # os statements de flink-sql/ foram rodados à mão no shell do Flink (confluent flink shell),
  # nesta ordem: 01_tx_stream_create, 02_tx_stream_insert, 03_fraud_alerts_create, 04_fraud_detect e,
  # para o enriquecimento, 05_tx_enriched_create e 06_tx_enrich.
  # disparar os statements pela CLI em vez do shell; confira a sintaxe com
  #   `confluent flink statement create --help` antes de confiar.
  echo "Abra: confluent flink shell --compute-pool <ID-do-pool> --environment $CONFLUENT_ENV_ID"
  echo "e rode os arquivos de flink-sql/ na ordem (USE CATALOG/USE database antes)."
  echo "Para a rajada de fraude: cole cdc-debezium/seed/03_fraud_burst.sql no SQL Editor do Neon."
}

operacao() {
  echo ">> Camada 5: Operação"
  # métricas (pico de received_bytes e lag por grupo) pela Metrics API. Exige uma Cloud
  # API key no .env (CLOUD_API_KEY/CLOUD_API_SECRET), criada com: confluent api-key create --resource cloud
  ./scripts/metrics.sh
  # custo acumulado. A cobrança leva de 24 a 48 h para aparecer, então consulte alguns dias
  # depois. O conector é a maior fatia do gasto (cobra por task-hora, mesmo sem tráfego); desligue-o
  # assim que a captura terminar.
  confluent billing cost list --start-date 2026-10-27 --end-date "$(date +%F)"
}

case "${1:-all}" in
  fundacao|contrato|ingestao|processamento|operacao) "$1" ;;
  all) fundacao; ingestao; contrato; processamento; operacao ;;
  *) echo "Uso: ./setup.sh [fundacao|contrato|ingestao|processamento|operacao|all]"; exit 1 ;;
esac
