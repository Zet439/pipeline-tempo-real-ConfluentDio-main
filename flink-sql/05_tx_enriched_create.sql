-- enriquecimento: tabela de saída (modo upsert, chave = transaction_id).
-- Upsert porque o join com `accounts` (changelog do Debezium) pode revisar uma linha quando a conta muda.
-- A coluna da chave vem primeiro: o DISTRIBUTED BY HASH(transaction_id) exige isso (erro visto no tx_stream).
CREATE TABLE tx_enriched (
  transaction_id BIGINT,
  card_id        BIGINT,
  account_id     BIGINT,
  customer_id    BIGINT,
  account_type   STRING,
  account_status STRING,
  balance        DECIMAL(15, 2),
  merchant_id    BIGINT,
  amount         DECIMAL(15, 2),
  currency       STRING,
  status         STRING,
  ts             TIMESTAMP(3),
  PRIMARY KEY (transaction_id) NOT ENFORCED
) DISTRIBUTED BY HASH(transaction_id) INTO 1 BUCKETS
WITH ('changelog.mode' = 'upsert');
