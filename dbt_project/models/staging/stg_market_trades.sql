-- Staging View: stg_market_trades
-- Reads directly from Hive-partitioned raw Parquet files in the lakehouse

WITH raw_parquet AS (
    SELECT 
        trade_id,
        UPPER(TRIM(symbol)) AS symbol,
        CAST(price AS DOUBLE) AS price,
        CAST(quantity AS DOUBLE) AS quantity,
        CAST(quote_quantity AS DOUBLE) AS quote_quantity,
        CAST(trade_timestamp AS TIMESTAMP WITH TIME ZONE) AS trade_timestamp,
        CAST(is_buyer_maker AS BOOLEAN) AS is_buyer_maker,
        CAST(year AS INTEGER) AS partition_year,
        CAST(month AS INTEGER) AS partition_month,
        CAST(day AS INTEGER) AS partition_day
    FROM read_parquet('data/lakehouse/raw/trades/**/*.parquet', hive_partitioning = 1)
    WHERE trade_id IS NOT NULL 
      AND price > 0 
      AND quantity > 0
)

SELECT 
    trade_id,
    symbol,
    price,
    quantity,
    quote_quantity,
    trade_timestamp,
    is_buyer_maker,
    partition_year,
    partition_month,
    partition_day
FROM raw_parquet
