-- Mart Fact: fct_trades
-- Core analytical fact table representing individual trade execution events

WITH trades AS (
    SELECT * FROM {{ ref('int_trades_deduped') }}
),

assets AS (
    SELECT asset_key, symbol FROM {{ ref('dim_assets') }}
)

SELECT 
    t.trade_surrogate_key,
    t.trade_id,
    a.asset_key,
    t.symbol,
    t.price,
    t.quantity,
    t.quote_quantity,
    t.trade_timestamp,
    t.is_buyer_maker,
    t.date_key,
    t.partition_year,
    t.partition_month,
    t.partition_day
FROM trades t
INNER JOIN assets a ON t.symbol = a.symbol
