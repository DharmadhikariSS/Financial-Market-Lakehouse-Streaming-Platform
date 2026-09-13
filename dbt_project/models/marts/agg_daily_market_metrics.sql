-- Mart Analytical Aggregation: agg_daily_market_metrics
-- Pre-aggregated metrics for reporting and executive dashboards

WITH trades AS (
    SELECT * FROM {{ ref('fct_trades') }}
)

SELECT 
    symbol,
    CAST(trade_timestamp AS DATE) AS trade_date,
    COUNT(*) AS total_trades,
    ROUND(SUM(quantity), 6) AS total_base_volume,
    ROUND(SUM(quote_quantity), 2) AS total_quote_volume,
    ROUND(MIN(price), 4) AS low_price,
    ROUND(MAX(price), 4) AS high_price,
    -- Volume Weighted Average Price (VWAP)
    ROUND(SUM(price * quantity) / NULLIF(SUM(quantity), 0), 4) AS vwap,
    -- Market sentiment ratios
    COUNT(*) FILTER (WHERE is_buyer_maker = TRUE) AS buyer_maker_trades,
    COUNT(*) FILTER (WHERE is_buyer_maker = FALSE) AS buyer_taker_trades,
    ROUND(
        COUNT(*) FILTER (WHERE is_buyer_maker = FALSE) * 100.0 / NULLIF(COUNT(*), 0),
        2
    ) AS taker_buy_percentage
FROM trades
GROUP BY 
    symbol,
    CAST(trade_timestamp AS DATE)
ORDER BY 
    trade_date DESC,
    total_quote_volume DESC
