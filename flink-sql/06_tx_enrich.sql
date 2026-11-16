-- enriquecimento transação × conta (statement contínuo, cobra CFU).
-- Join comum (não temporal): o estado cresce com o número de transações, por isso o TTL de estado abaixo.
-- Por que não temporal: `FOR SYSTEM_TIME AS OF t.ts` exigiria versões de `accounts` anteriores ao ts da
-- transação; as contas só têm versão a partir do snapshot do CDC e as transações do seed são de setembro,
-- então o join temporal descartaria essas linhas. É o caminho seguro; o temporal fica para "próximos passos".
-- O join é por account_id (não card_id): a conta do cartão pode pertencer a outro cliente.
SET 'client.statement-name' = 'tx-enrich';
SET 'sql.state-ttl' = '1 d';
INSERT INTO tx_enriched
SELECT t.transaction_id, t.card_id, t.account_id, a.customer_id, a.account_type,
       a.status AS account_status, a.balance, t.merchant_id, t.amount, t.currency, t.status, t.ts
FROM tx_stream t
JOIN `desafio.public.accounts` a ON t.account_id = a.account_id;
