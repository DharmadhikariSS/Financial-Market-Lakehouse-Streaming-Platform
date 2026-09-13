-- ==============================================================================
-- MILESTONE 01: RELATIONAL SCHEMA DEFINITION
-- Compatible with PostgreSQL 16 & DuckDB 1.0+
-- ==============================================================================

-- 1. Schema Isolation
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS staging;

-- 2. Conformed Dimension: Symbols & Trading Pairs
CREATE TABLE IF NOT EXISTS core.dim_symbols (
    symbol VARCHAR(20) PRIMARY KEY,
    base_asset VARCHAR(10) NOT NULL,
    quote_asset VARCHAR(10) NOT NULL,
    tick_size DECIMAL(18, 8) DEFAULT 0.00000001,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Seed baseline symbols if not present
INSERT INTO core.dim_symbols (symbol, base_asset, quote_asset, tick_size)
VALUES 
    ('BTCUSDT', 'BTC', 'USDT', 0.01),
    ('ETHUSDT', 'ETH', 'USDT', 0.01),
    ('SOLUSDT', 'SOL', 'USDT', 0.01)
ON CONFLICT (symbol) DO NOTHING;

-- 3. Core Analytical Fact Table: Market Execution Ledger
CREATE TABLE IF NOT EXISTS core.fact_trades (
    symbol VARCHAR(20) NOT NULL,
    trade_id BIGINT NOT NULL,
    price DECIMAL(18, 8) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    quote_quantity DECIMAL(18, 8) NOT NULL,
    trade_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    is_buyer_maker BOOLEAN NOT NULL DEFAULT FALSE,
    record_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, trade_id)
);

-- Index for temporal range scans and analytical aggregations
CREATE INDEX IF NOT EXISTS idx_fact_trades_timestamp 
ON core.fact_trades (trade_timestamp);

CREATE INDEX IF NOT EXISTS idx_fact_trades_hash 
ON core.fact_trades (record_hash);

-- 4. Staging Landing Table (Unindexed for fast bulk batch ingestion)
CREATE TABLE IF NOT EXISTS staging.stg_raw_trades (
    symbol VARCHAR(50),
    trade_id BIGINT,
    price DOUBLE PRECISION,
    quantity DOUBLE PRECISION,
    quote_quantity DOUBLE PRECISION,
    trade_timestamp TIMESTAMP WITH TIME ZONE,
    is_buyer_maker BOOLEAN,
    record_hash VARCHAR(64),
    batch_id VARCHAR(64),
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
