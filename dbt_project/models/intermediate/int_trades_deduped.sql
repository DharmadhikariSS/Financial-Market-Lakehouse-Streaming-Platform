-- Intermediate Model: int_trades_deduped
-- Deduplicates replayed trade events and constructs surrogate business keys

WITH source AS (
    SELECT * FROM {{ ref('stg_market_trades') }}
),

ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, trade_id 
            ORDER BY trade_timestamp DESC
        ) AS row_num
    FROM source
),

deduped AS (
    SELECT 
        -- Surrogate Key
        md5(symbol || '-' || CAST(trade_id AS VARCHAR)) AS trade_surrogate_key,
        trade_id,
        symbol,
        price,
        quantity,
        quote_quantity,
        trade_timestamp,
        is_buyer_maker,
        CAST(strftime(trade_timestamp, '%Y%m%d') AS INTEGER) AS date_key,
        partition_year,
        partition_month,
        partition_day
    FROM ranked
    WHERE row_num = 1
)

SELECT * FROM deduped
