-- Mart Dimension: dim_assets
-- Conformed dimension table describing tradable instruments

WITH distinct_symbols AS (
    SELECT DISTINCT symbol FROM {{ ref('int_trades_deduped') }}
),

asset_metadata AS (
    SELECT 
        symbol,
        -- Asset parsing
        CASE 
            WHEN symbol LIKE '%USDT' THEN REPLACE(symbol, 'USDT', '')
            WHEN symbol LIKE '%USDC' THEN REPLACE(symbol, 'USDC', '')
            WHEN symbol LIKE '%BTC' THEN REPLACE(symbol, 'BTC', '')
            ELSE symbol 
        END AS base_asset,
        CASE 
            WHEN symbol LIKE '%USDT' THEN 'USDT'
            WHEN symbol LIKE '%USDC' THEN 'USDC'
            WHEN symbol LIKE '%BTC' THEN 'BTC'
            ELSE 'UNKNOWN' 
        END AS quote_asset,
        'CRYPTOCURRENCY' AS asset_class,
        TRUE AS is_active,
        CURRENT_TIMESTAMP AS effective_timestamp
    FROM distinct_symbols
)

SELECT 
    md5(symbol) AS asset_key,
    symbol,
    base_asset,
    quote_asset,
    asset_class,
    is_active,
    effective_timestamp
FROM asset_metadata
