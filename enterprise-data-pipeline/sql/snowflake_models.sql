-- Snowflake SQL Scripts for Data Modeling

-- ============================================================================
-- SCHEMA SETUP
-- ============================================================================

USE DATABASE CRYPTO_DB;
USE SCHEMA PUBLIC;

-- ============================================================================
-- ANALYTICS VIEWS
-- ============================================================================

-- View: Current Market State
CREATE OR REPLACE VIEW v_current_market_state AS
SELECT 
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    market_cap_category,
    total_volume,
    price_change_percentage_24h,
    price_volatility_24h,
    volume_to_market_cap_ratio,
    distance_from_ath_pct,
    is_price_anomaly,
    is_volume_spike,
    updated_at
FROM silver_crypto_clean
WHERE is_current = TRUE
ORDER BY market_cap_rank;

-- View: Historical Price Changes
CREATE OR REPLACE VIEW v_price_history AS
SELECT 
    coin_id,
    symbol,
    name,
    current_price,
    market_cap,
    valid_from,
    valid_to,
    CASE 
        WHEN valid_to IS NULL THEN DATEDIFF(day, valid_from, CURRENT_TIMESTAMP())
        ELSE DATEDIFF(day, valid_from, valid_to)
    END as days_at_price
FROM silver_crypto_clean
ORDER BY coin_id, valid_from DESC;

-- View: Top Movers (24h)
CREATE OR REPLACE VIEW v_top_movers_24h AS
SELECT 
    symbol,
    name,
    current_price,
    price_change_percentage_24h,
    market_cap,
    total_volume,
    CASE 
        WHEN price_change_percentage_24h > 0 THEN 'GAINER'
        ELSE 'LOSER'
    END as movement_type
FROM silver_crypto_clean
WHERE is_current = TRUE
    AND price_change_percentage_24h IS NOT NULL
ORDER BY ABS(price_change_percentage_24h) DESC
LIMIT 20;

-- View: Market Dominance
CREATE OR REPLACE VIEW v_market_dominance AS
SELECT 
    symbol,
    name,
    market_cap,
    (market_cap / SUM(market_cap) OVER ()) * 100 as market_dominance_pct,
    RANK() OVER (ORDER BY market_cap DESC) as dominance_rank
FROM silver_crypto_clean
WHERE is_current = TRUE
    AND market_cap IS NOT NULL
ORDER BY market_cap DESC;

-- View: Volatility Analysis
CREATE OR REPLACE VIEW v_volatility_analysis AS
SELECT 
    market_cap_category,
    COUNT(*) as num_coins,
    AVG(price_volatility_24h) as avg_volatility,
    STDDEV(price_volatility_24h) as stddev_volatility,
    MIN(price_volatility_24h) as min_volatility,
    MAX(price_volatility_24h) as max_volatility,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_volatility_24h) as median_volatility
FROM silver_crypto_clean
WHERE is_current = TRUE
    AND price_volatility_24h IS NOT NULL
GROUP BY market_cap_category;

-- ============================================================================
-- AGGREGATION TABLES (Materialized)
-- ============================================================================

-- Daily Market Summary
CREATE OR REPLACE TABLE daily_market_summary AS
SELECT 
    DATE(updated_at) as summary_date,
    market_cap_category,
    COUNT(*) as num_coins,
    SUM(market_cap) as total_market_cap,
    SUM(total_volume) as total_volume,
    AVG(current_price) as avg_price,
    AVG(price_change_percentage_24h) as avg_price_change_24h,
    AVG(price_volatility_24h) as avg_volatility,
    COUNT(CASE WHEN is_price_anomaly THEN 1 END) as num_anomalies,
    COUNT(CASE WHEN is_volume_spike THEN 1 END) as num_volume_spikes,
    CURRENT_TIMESTAMP() as created_at
FROM silver_crypto_clean
WHERE is_current = TRUE
GROUP BY DATE(updated_at), market_cap_category;

-- ============================================================================
-- DATA QUALITY MONITORING
-- ============================================================================

-- View: Data Quality Metrics
CREATE OR REPLACE VIEW v_data_quality_metrics AS
SELECT 
    DATE(updated_at) as check_date,
    COUNT(*) as total_records,
    COUNT(CASE WHEN current_price IS NULL THEN 1 END) as null_price_count,
    COUNT(CASE WHEN market_cap IS NULL THEN 1 END) as null_market_cap_count,
    COUNT(CASE WHEN current_price < 0 THEN 1 END) as negative_price_count,
    COUNT(CASE WHEN is_price_anomaly THEN 1 END) as anomaly_count,
    (1 - (COUNT(CASE WHEN current_price IS NULL THEN 1 END)::FLOAT / COUNT(*))) * 100 as price_completeness_pct,
    CURRENT_TIMESTAMP() as checked_at
FROM silver_crypto_clean
WHERE is_current = TRUE
GROUP BY DATE(updated_at);

-- View: Pipeline Execution History
CREATE OR REPLACE VIEW v_pipeline_execution_history AS
SELECT 
    run_id,
    pipeline_name,
    run_date,
    status,
    records_extracted,
    records_processed,
    records_loaded,
    records_failed,
    ROUND(execution_time_seconds / 60, 2) as execution_time_minutes,
    CASE 
        WHEN records_extracted > 0 
        THEN ROUND((records_processed::FLOAT / records_extracted) * 100, 2)
        ELSE 0 
    END as success_rate_pct,
    error_message,
    created_at
FROM pipeline_metadata
ORDER BY run_date DESC;

-- ============================================================================
-- ANALYTICAL FUNCTIONS
-- ============================================================================

-- Function: Calculate Price Momentum
CREATE OR REPLACE FUNCTION calculate_momentum(
    change_24h FLOAT,
    change_7d FLOAT,
    change_30d FLOAT
)
RETURNS VARCHAR
AS
$$
    CASE
        WHEN change_24h > 5 AND change_7d > 10 AND change_30d > 20 THEN 'STRONG_BULLISH'
        WHEN change_24h > 2 AND change_7d > 5 THEN 'BULLISH'
        WHEN change_24h < -5 AND change_7d < -10 AND change_30d < -20 THEN 'STRONG_BEARISH'
        WHEN change_24h < -2 AND change_7d < -5 THEN 'BEARISH'
        ELSE 'NEUTRAL'
    END
$$;

-- ============================================================================
-- PERFORMANCE OPTIMIZATION
-- ============================================================================

-- Add clustering keys for better performance
ALTER TABLE silver_crypto_clean CLUSTER BY (coin_id, valid_from);
ALTER TABLE gold_crypto_metrics CLUSTER BY (metric_date, market_cap_category);

-- ============================================================================
-- INCREMENTAL REFRESH PROCEDURE
-- ============================================================================

CREATE OR REPLACE PROCEDURE sp_refresh_daily_summary()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Delete today's data
    DELETE FROM daily_market_summary 
    WHERE summary_date = CURRENT_DATE();
    
    -- Insert fresh data
    INSERT INTO daily_market_summary
    SELECT 
        DATE(updated_at) as summary_date,
        market_cap_category,
        COUNT(*) as num_coins,
        SUM(market_cap) as total_market_cap,
        SUM(total_volume) as total_volume,
        AVG(current_price) as avg_price,
        AVG(price_change_percentage_24h) as avg_price_change_24h,
        AVG(price_volatility_24h) as avg_volatility,
        COUNT(CASE WHEN is_price_anomaly THEN 1 END) as num_anomalies,
        COUNT(CASE WHEN is_volume_spike THEN 1 END) as num_volume_spikes,
        CURRENT_TIMESTAMP() as created_at
    FROM silver_crypto_clean
    WHERE is_current = TRUE
        AND DATE(updated_at) = CURRENT_DATE()
    GROUP BY DATE(updated_at), market_cap_category;
    
    RETURN 'Daily summary refreshed successfully';
END;
$$;

-- ============================================================================
-- USEFUL QUERIES FOR ANALYSIS
-- ============================================================================

-- Query 1: Top 10 by Market Cap with metrics
/*
SELECT 
    symbol,
    name,
    current_price,
    market_cap,
    market_cap_rank,
    price_change_percentage_24h,
    volume_to_market_cap_ratio,
    distance_from_ath_pct
FROM v_current_market_state
LIMIT 10;
*/

-- Query 2: Anomalies detected
/*
SELECT 
    symbol,
    name,
    current_price,
    price_change_percentage_24h,
    is_price_anomaly,
    is_volume_spike,
    updated_at
FROM v_current_market_state
WHERE is_price_anomaly = TRUE OR is_volume_spike = TRUE
ORDER BY ABS(price_change_percentage_24h) DESC;
*/

-- Query 3: Market category comparison
/*
SELECT 
    market_cap_category,
    num_coins,
    total_market_cap,
    avg_price_change_24h,
    avg_volatility
FROM gold_crypto_metrics
WHERE metric_date = CURRENT_DATE()
ORDER BY total_market_cap DESC;
*/

-- Query 4: Pipeline health check
/*
SELECT *
FROM v_pipeline_execution_history
LIMIT 10;
*/
