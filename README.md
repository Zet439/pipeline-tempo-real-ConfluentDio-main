# Pipeline de Dados em Tempo Real para Agentes de IA

Pipeline de dados end-to-end em tempo real sobre **Confluent Cloud (Kafka)**: captura mudanças de um banco existente via CDC, processa o fluxo com Flink e prepara os dados para consumo.

O caso de uso é um **pipeline de pagamentos**: Postgres (Neon) → CDC (Debezium) → tópicos Avro + Schema Registry → Flink SQL (enriquecimento + regra de fraude) → consumo. Os dados são sintéticos, com seed determinístico (`seed=42`).

## Stack

| Camada | Tecnologia |
|---|---|
| Banco de origem | PostgreSQL (Neon) |
| Streaming / Event Log | Apache Kafka (Confluent Cloud) |
| Governança de contratos | Schema Registry |
| Integração / CDC | Kafka Connect + Debezium |
| Processamento de stream | Apache Flink / Flink SQL |

## Estrutura do repositório

| Pasta / arquivo | Camada | Conteúdo |
|---|---|---|
| `kafka/schemas/` | Contrato | schema Avro atual de `customers` (`customers-v1-atual.avsc`) e três variantes usadas nos testes de compatibilidade |
| `cdc-debezium/cdc.json` | Ingestão | configuração do conector CDC, com variáveis `${...}` resolvidas em runtime (sem segredos no arquivo) |
| `cdc-debezium/seed/` | Ingestão | dados sintéticos e scripts: `00_schema.sql`, `01_cdc_setup.sql`, `generate_seed.py` (gera `02_seed.sql`), `03_fraud_burst.sql`, `load.sh`, `apply_tags.sh` (tags PII/PCI por campo) e `04_pii_tags.md` |
| `flink-sql/` | Processamento | statements SQL: criação das tabelas, conversão para append-only, regra de fraude e enriquecimento |
| `scripts/metrics.sh` | Operação | consulta à Metrics API do Confluent Cloud |
| `setup.sh` / `teardown.sh` | todas | subir e derrubar o pipeline |
| `.env.example` | — | modelo das variáveis de ambiente (o `.env` nunca é versionado) |

## Pré-requisitos

Conta no Confluent Cloud, projeto no Neon, [Confluent CLI](https://docs.confluent.io/confluent-cli/current/overview.html), `psql`, `envsubst` e `python3`.

## Como subir do zero

```bash
cp .env.example .env     # preencha os valores; o .env NUNCA é versionado
chmod +x setup.sh teardown.sh scripts/metrics.sh cdc-debezium/seed/*.sh
./setup.sh fundacao      # confira o resultado antes de avançar
./setup.sh ingestao
./setup.sh contrato      # depois da ingestão: os schemas são registrados pelo conector
./setup.sh processamento
./setup.sh operacao
```

> Os scripts `.sh` precisam do bit de execução. Rode o `chmod +x` acima (ou use `bash <script>`) antes do primeiro uso; sem isso o shell responde `Permission denied`.

Dois passos são manuais e precisam ser feitos uma vez no console do Neon, antes da ingestão:

1. Habilitar a **replicação lógica** (reinicia o compute; é a origem do WAL que o CDC lê).
2. Criar o role `cdc_user` **com o atributo `REPLICATION`** pelo painel de Roles do Neon. No Neon, um role criado só por SQL pode não receber esse atributo. A senha do `cdc_user` fica no `.env` (`CDC_PASSWORD`), nunca em arquivo versionado.

Os statements de `flink-sql/` são executados no shell do Flink (`confluent flink shell`), na ordem numérica dos arquivos.

### Configuração do `.env`

- `PG_ADMIN_USER` é o **nome do usuário** do Postgres (ex.: o owner do Neon ou `cdc_user`), **nunca o hostname** (`PG_HOST`). Preencher com o hostname gera `password authentication failed for user '<hostname>'`.
- O Neon exige **SNI**. Clientes `psql`/`libpq` modernos (>= 14) o enviam automaticamente. Para `libpq` antigo há o contorno `PGOPTIONS="endpoint=<1a parte do host>"`, mantido nos scripts porque é inofensivo nas versões novas.
- Segredos (senhas, API keys) ficam só no `.env`, que está no `.gitignore`. A configuração do conector é renderizada em runtime com `envsubst < cdc-debezium/cdc.json`, a partir da raiz do repositório, para não versionar segredos.

## Como derrubar tudo

```bash
./teardown.sh
```

O `teardown.sh` para os statements Flink, remove o conector, apaga o replication slot e a publication no Postgres, apaga as API keys e service accounts e, por fim, deleta o environment. A ordem importa: parar Flink e conector primeiro interrompe a cobrança por CFU/task-hora e libera o slot de replicação (um slot órfão retém WAL e enche o disco).

---

## Camada 1: Fundação

**O que faz:**

- Environment dedicado e cluster Kafka **Basic** em `aws/us-east-1`, single-zone.
- Duas identidades (service accounts), uma por papel, com uma API key cada:
  - produtor (`desafio-producer`): `WRITE`, `DESCRIBE` e `CREATE` nos tópicos com prefixo `desafio`.
  - consumidor (`desafio-consumer`): `READ` e `DESCRIBE` nos tópicos `desafio*` e `READ` nos grupos de consumo `desafio*`.

**Por que?:**

- **Duas identidades, menor privilégio:** quem escreve não lê e quem lê não escreve. Se uma chave vazar, o dano é limitado.
- **ACL por prefixo (`desafio`):** limita o alcance aos tópicos do projeto, incluindo os que o conector CDC cria.
- **Cluster Basic:** tipo certo para dev e POC; cobra pelo uso e comporta com folga o volume do desafio (cerca de 2.000 transações sintéticas).
- **Environment dedicado:** dá uma fronteira de isolamento e permite derrubar tudo com um único `delete` no teardown.
- **ACL do heartbeat do Debezium:** o produtor também recebe `CREATE`/`DESCRIBE`/`WRITE` no prefixo `__debezium-heartbeat`. O Debezium grava um tópico de batimento fora do prefixo do projeto; sem essa ACL o conector fica `FAILED` por falta de autorização. A permissão é a menor possível, em vez de um acesso amplo.

## Camada 2: Contrato de dados (`kafka/schemas/`)

**O que faz:** o Debezium registra automaticamente um schema Avro por tabela no Schema Registry (subjects `desafio.public.<tabela>-value`, envelope com `before`/`after`/`source`/`op`). Esses schemas são o contrato do pipeline. Em `kafka/schemas/` ficam o schema atual de `customers` e três variantes usadas nos testes de evolução.

**Tags de PII/PCI:** os campos sensíveis são classificados no catálogo do Stream Governance, por campo: `PII` em `customers.full_name`, `email` e `phone`, e `PCI` em `cards.last4`. A aplicação é feita pela API do catálogo (`cdc-debezium/seed/apply_tags.sh`, entidade `sr_field`), porque a aba Schema do console só oferece tag no nível do schema ou do tópico. A tag vale para `before` e `after`, que usam o mesmo record `Value`. O mapeamento está em `cdc-debezium/seed/04_pii_tags.md`.

**Por que?:** a compatibilidade usada é `BACKWARD` (padrão do Schema Registry): o consumidor novo lê dados antigos, então os consumidores (Flink) podem ser atualizados antes do produtor. Nessa regra, adicionar campo opcional com `default` é seguro; adicionar campo obrigatório sem `default` quebra; remover campo é aceito. Os testes usam `validate`, que não registra versão e não suja o subject. As três variantes em `kafka/schemas/` cobrem exatamente esses três casos.

## Camada 3: Ingestão CDC (`cdc-debezium/`)

**O que faz:** o conector gerenciado `PostgresCdcSourceV2` lê o WAL do Postgres (Neon) com o usuário `cdc_user` (permissão mínima de replicação) e publica as 5 tabelas (`customers`, `accounts`, `cards`, `merchants`, `transactions`) em tópicos `desafio.public.<tabela>`, em Avro, com a chave primária como chave Kafka. A configuração está em `cdc-debezium/cdc.json` (sem segredos; as variáveis são resolvidas por `envsubst` em runtime).

**Por que?:** o CDC lê o log de transações, então captura INSERT, UPDATE e DELETE com baixa latência e sem consultar as tabelas; polling perderia DELETEs e estados intermediários. O schema usa `REPLICA IDENTITY FULL` (definido em `00_schema.sql`) para que `before` venha completo em UPDATE/DELETE — custo: mais bytes no WAL. Cada tópico tem 1 partição (padrão do conector), o que preserva a ordem por chave e limita o paralelismo.

**Seed determinístico:** `generate_seed.py` gera `02_seed.sql` com `seed=42` (mesma entrada, mesma saída), nas proporções 200 clientes, 240 contas, 320 cartões, 60 comerciantes e 2.000 transações. O gerador espaça as transações de cada cartão em pelo menos 10 minutos, de modo que a base não dispara a regra de fraude por si só — o alerta vem de `03_fraud_burst.sql`, rodado só depois que o Flink está processando.

## Camada 4: Processamento (`flink-sql/`)

**Escopo:** conversão para append-only, regra de fraude e enriquecimento transação × conta.

**O que faz?:**

1. `01_tx_stream_create.sql` / `02_tx_stream_insert.sql`: convertem `desafio.public.transactions` (changelog `retract` do Debezium) numa tabela append-only `tx_stream`, com `created_at` em `TIMESTAMP(3)` e watermark.
2. `03_fraud_alerts_create.sql` / `04_fraud_detect.sql`: aplicam um `MATCH_RECOGNIZE` particionado por cartão e gravam em `fraud_alerts` quando há 3 transações do mesmo cartão em até 60 s (`PATTERN (T{3}) WITHIN INTERVAL '60' SECOND`).
3. `05_tx_enriched_create.sql` / `06_tx_enrich.sql`: fazem o join de `tx_stream` com `desafio.public.accounts` por `account_id` e gravam em `tx_enriched`, em modo upsert com chave `transaction_id`.

**Por que?:**

- `MATCH_RECOGNIZE` não aceita entrada que atualiza ou apaga linhas, então a tabela do Debezium passa por `TO_CHANGELOG` (só inserts; transações são imutáveis) antes da regra.
- O tempo da regra é o tempo do evento (`created_at`), não o de chegada, para que atrasos do pipeline não mudem o resultado.
- **Watermark com folga de 1 ms:** sem folga, eventos com o mesmo `ts` contariam como atrasados; folga maior atrasaria o alerta; folga menor perderia eventos. Evento muito fora de ordem continua sendo ignorado.
- `AFTER MATCH SKIP PAST LAST ROW`: uma rajada gera um alerta, não vários sobrepostos.
- **Enriquecimento com join comum (não temporal):** o join temporal (`FOR SYSTEM_TIME AS OF t.ts`) exigiria versões de `accounts` anteriores ao `ts` da transação; as contas só têm versão a partir do snapshot do CDC, então o temporal fica como caminho a explorar depois. O join é por `account_id` (não `card_id`), porque a conta do cartão pode pertencer a outro cliente. O join comum guarda estado sem TTL por padrão; `06_tx_enrich.sql` define `sql.state-ttl` para limitar esse crescimento.

## Camada 5: Operação

**Observabilidade:** `scripts/metrics.sh` consulta a Metrics API do Confluent Cloud (`received_bytes` e lag por grupo de consumidores), usando uma Cloud API key (`CLOUD_API_KEY`/`CLOUD_API_SECRET`) do `.env`. Como o Flink lê por dentro da plataforma e não há consumidor externo, não se espera grupo de consumidores ativo.

**Segurança:**

| Service account | ACLs (prefixo `desafio`, salvo indicado) | Por quê |
|---|---|---|
| produtor (`desafio-producer`) | `CREATE`, `DESCRIBE`, `WRITE` em tópicos `desafio*`; os mesmos três em `__debezium-heartbeat*` | identidade do conector CDC: só escreve. A ACL do heartbeat é necessária porque o Debezium grava um tópico de batimento fora do prefixo do projeto. |
| consumidor (`desafio-consumer`) | `DESCRIBE` e `READ` em tópicos `desafio*`; `READ` em grupos `desafio*` | identidade só de leitura, pronta para um consumidor externo ou sink connector. |

Os statements Flink rodam com o usuário da conta. Em produção, o RBAC é o padrão recomendado; aqui foram usadas ACLs por prefixo, mais simples para um ambiente com dois papéis.

**Custos:** o conector costuma ser a maior fatia do gasto, porque cobra por task-hora mesmo sem tráfego; desligá-lo assim que a captura termina é a principal alavanca de economia. Os statements Flink cobram CFU enquanto rodam, então pare-os após gerar a evidência. O cluster Basic custa pouco em volume baixo. A cobrança leva alguns dias para fechar no billing.

**Confiabilidade: trecho a trecho**

| Trecho | Garantia / risco | Mitigação |
|---|---|---|
| Postgres (Neon) → conector | `tasks.max=1`; o slot de replicação retém WAL se o conector parar (slot órfão enche o disco) | `heartbeat.interval.ms=60000`; o teardown apaga o slot |
| Conector → Kafka | entrega at-least-once: após reinício o mesmo evento pode chegar duplicado | chave = chave primária; consumidores devem ser idempotentes |
| Tópicos | 1 partição por tópico: ordem por chave, sem paralelismo; cluster Basic é de zona única | volume do projeto é pequeno |
| Schema Registry | `BACKWARD`: campo obrigatório sem default é barrado na entrada | testes de compatibilidade na camada 2 |
| Flink | o alerta depende do watermark (precisa de evento posterior); evento muito fora de ordem é ignorado | watermark de 1 ms; em produção o fluxo contínuo avança o watermark |

## Decisões de arquitetura

| # | Decisão | Alternativa descartada | Por quê / trade-off |
|---|---|---|---|
| 1 | CDC (Debezium) | Polling/batch | o CDC lê o log de transações: captura INSERT/UPDATE/DELETE com baixa latência e sem consultar as tabelas; polling perderia DELETEs e estados intermediários. Custo: depende de replicação lógica, slot e WAL. |
| 2 | 2 service accounts (write/read) | 1 conta com tudo | menor privilégio: se uma chave vazar, o dano é limitado. Custo: mais identidades e ACLs para gerenciar. |
| 3 | Compatibilidade `BACKWARD` | `FULL` | permite atualizar consumidores antes do produtor e barra campo obrigatório sem default. Trade-off: aceita remoção de campo. |
| 4 | `REPLICA IDENTITY FULL` | `DEFAULT` (só chave) | `before` completo em UPDATE/DELETE. Custo: mais bytes no WAL por mudança. |
| 5 | `MATCH_RECOGNIZE` por cartão, `T{3}` em 60 s, tempo de evento | `OVER` com `COUNT`/`LAG`, tempo de processamento | padrão de sequência explícito, estado por cartão, resultado independente do atraso do pipeline. Custo: exige entrada append-only e watermark. |
| 6 | Cluster Basic | Standard/Dedicated | menor custo para volume baixo. Custo: zona única e menos recursos de rede e segurança. |
| 7 | Neon (Postgres gerenciado) | Postgres local em Docker | acessível pela nuvem, como o conector gerenciado exige. Custo: plano Free com compute que pode suspender e exigência de SNI. |
| 8 | Join comum no enriquecimento | Join temporal (`FOR SYSTEM_TIME AS OF`) | as contas só têm versão a partir do snapshot do CDC; o temporal descartaria transações anteriores a esse ponto. Custo: estado sem versão histórica; mitigado com `sql.state-ttl`. |

## Próximos passos



## As camadas do Confluent Cloud neste projeto

O Confluent Cloud não é só Kafka: é um conjunto de serviços gerenciados que se encaixam em volta do log de eventos. Este projeto usa os seguintes, e cada um ocupa um lugar específico no fluxo.

- **Kafka (cluster Basic)** — o log de eventos, espinha dorsal do pipeline. Guarda os tópicos `desafio.public.<tabela>` (CDC) e as tabelas de saída do Flink (`tx_stream`, `fraud_alerts`, `tx_enriched`). Cada tópico tem 1 partição, o que preserva a ordem por chave. É aqui que todo o resto se conecta.

- **Identidade e acesso (service accounts + API keys + ACLs)** — a camada de segurança. Duas service accounts (produtor e consumidor), cada uma com sua API key, e ACLs por prefixo `desafio` que delimitam o que cada identidade pode fazer. É o menor privilégio aplicado na prática: o conector só escreve, o consumidor só lê.

- **Schema Registry (Stream Governance)** — a camada de contrato. Armazena os schemas Avro que o Debezium registra por tabela, valida a evolução sob a regra `BACKWARD` e guarda, no catálogo, as tags de PII/PCI por campo. É o que garante que produtor e consumidores concordem sobre o formato dos dados ao longo do tempo.

- **Kafka Connect (conector gerenciado `PostgresCdcSourceV2`)** — a camada de ingestão. Roda o Debezium como serviço gerenciado (sem operar workers), lê o WAL do Postgres (Neon) e publica as mudanças nos tópicos. É a ponte entre o banco de origem e o Kafka.

- **Flink (compute pool + Flink SQL)** — a camada de processamento de stream. Statements SQL contínuos convertem o changelog do Debezium em append-only, aplicam a regra de fraude (`MATCH_RECOGNIZE`) e enriquecem as transações com dados de conta. Cobra por CFU enquanto roda e lê/escreve diretamente nos tópicos, por dentro da plataforma.

- **Metrics API** — a camada de observabilidade. Exposta via `scripts/metrics.sh` com uma Cloud API key, fornece métricas do cluster (como `received_bytes` e lag de consumidores) para acompanhar throughput e saúde do pipeline.

- **Billing** — a camada de custo. Consolida o gasto por produto (conector, Flink, Kafka). O conector é tipicamente a maior fatia, por cobrar por task-hora mesmo sem tráfego, o que motiva o teardown logo após a captura.

Como se encaixam, de ponta a ponta:

```
Postgres (Neon)
   │  WAL (replicação lógica)
   ▼
Kafka Connect / Debezium ──► Kafka (tópicos CDC) ──► Flink SQL ──► Kafka (tx_stream, fraud_alerts, tx_enriched)
                                   ▲                                         │
                        Schema Registry (contrato Avro + tags)              ▼
                                                                        consumo / próximos passos (IA)

Transversais: Identidade & ACLs (segurança) · Metrics API (observabilidade) · Billing (custo)
```
