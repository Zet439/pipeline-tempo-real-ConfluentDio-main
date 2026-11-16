-- Permissões mínimas para o usuário de CDC e a publication que o conector lê.
-- PRÉ-REQUISITO: o role cdc_user já existe, criado COM o atributo REPLICATION pelo painel de Roles do
-- Neon. No Neon, um role criado só por SQL pode não receber esse atributo; por isso crie pelo console.
-- A senha do cdc_user NUNCA vai neste arquivo: fica no .env (CDC_PASSWORD). No .env, use o NOME do
-- usuário (cdc_user), não o hostname do Neon, senão a autenticação falha.
-- Rodar DEPOIS de 00_schema.sql e 02_seed.sql.

GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

DROP PUBLICATION IF EXISTS desafio_pub;
CREATE PUBLICATION desafio_pub
  FOR TABLE customers, accounts, cards, merchants, transactions;
