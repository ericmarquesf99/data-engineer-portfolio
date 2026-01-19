-- ============================================================================
-- BRONZE LAYER - Raw Data Storage
-- ============================================================================
-- Este script cria o schema BRONZE e a tabela para armazenar dados brutos
-- da API CoinGecko em formato JSON (VARIANT)
--
-- Schema: BRONZE
-- Tabela: BRONZE_CRYPTO_RAW
-- Propósito: Armazenar payload JSON completo extraído da API
-- ============================================================================

USE DATABASE CRYPTO_DB;

-- ============================================================================
-- 1. Criar Schema BRONZE
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Bronze Layer - Raw data from external sources (immutable)';

USE SCHEMA BRONZE;

-- ============================================================================
-- 2. Criar Tabela de Dados Brutos (Raw JSON)
-- ============================================================================

CREATE TABLE IF NOT EXISTS BRONZE_CRYPTO_RAW (
    -- Identificador único do registro
    id STRING DEFAULT UUID_STRING() PRIMARY KEY,
    
    -- Payload JSON completo da API (VARIANT permite queries em JSON)
    payload VARIANT NOT NULL,
    
    -- Metadados de extração
    extracted_at TIMESTAMP_NTZ NOT NULL,
    run_id STRING,
    source_system STRING DEFAULT 'coingecko_api',
    
    -- Metadados de auditoria
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING,
    record_number INTEGER,
    
    -- Flags de processamento
    processed BOOLEAN DEFAULT FALSE,
    processed_at TIMESTAMP_NTZ,
    error_flag BOOLEAN DEFAULT FALSE,
    error_message STRING
)
COMMENT = 'Bronze Layer - Raw cryptocurrency data from CoinGecko API stored as JSON';

-- ============================================================================
-- 3. Adicionar Clustering Key (Otimização de Performance)
-- ============================================================================

ALTER TABLE BRONZE_CRYPTO_RAW 
    CLUSTER BY (TO_DATE(extracted_at), source_system);

-- ============================================================================
-- 4. Criar Índices de Pesquisa
-- ============================================================================

-- Índice para queries por data de extração
CREATE INDEX IF NOT EXISTS idx_extracted_date 
    ON BRONZE_CRYPTO_RAW(TO_DATE(extracted_at));

-- Índice para filtrar registros não processados
CREATE INDEX IF NOT EXISTS idx_processed 
    ON BRONZE_CRYPTO_RAW(processed);

-- ============================================================================
-- 5. Criar Views Auxiliares
-- ============================================================================

-- View: Últimos dados extraídos
CREATE OR REPLACE VIEW v_latest_bronze_extractions AS
SELECT 
    id,
    payload:id::STRING as coin_id,
    payload:symbol::STRING as symbol,
    payload:name::STRING as name,
    payload:current_price::FLOAT as current_price,
    extracted_at,
    run_id,
    processed
FROM BRONZE_CRYPTO_RAW
WHERE extracted_at >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY extracted_at DESC;

-- View: Estatísticas de extração
CREATE OR REPLACE VIEW v_bronze_extraction_stats AS
SELECT 
    TO_DATE(extracted_at) as extraction_date,
    run_id,
    COUNT(*) as total_records,
    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_records,
    COUNT(CASE WHEN error_flag = TRUE THEN 1 END) as error_records,
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction
FROM BRONZE_CRYPTO_RAW
GROUP BY TO_DATE(extracted_at), run_id
ORDER BY extraction_date DESC, run_id;

-- View: Registros pendentes de processamento
CREATE OR REPLACE VIEW v_bronze_pending_records AS
SELECT 
    id,
    payload,
    extracted_at,
    run_id,
    DATEDIFF(hour, extracted_at, CURRENT_TIMESTAMP()) as hours_pending
FROM BRONZE_CRYPTO_RAW
WHERE processed = FALSE 
    AND error_flag = FALSE
ORDER BY extracted_at ASC;

-- ============================================================================
-- 6. Criar Stored Procedure para Marcar Registros como Processados
-- ============================================================================

CREATE OR REPLACE PROCEDURE sp_mark_records_processed(
    p_run_id STRING
)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    UPDATE BRONZE_CRYPTO_RAW
    SET 
        processed = TRUE,
        processed_at = CURRENT_TIMESTAMP()
    WHERE run_id = p_run_id
        AND processed = FALSE;
    
    RETURN 'Records marked as processed for run_id: ' || p_run_id;
END;
$$;

-- ============================================================================
-- 7. Criar Stored Procedure para Limpeza de Dados Antigos
-- ============================================================================

CREATE OR REPLACE PROCEDURE sp_archive_old_bronze_data(
    p_days_to_keep INTEGER DEFAULT 90
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    v_cutoff_date DATE;
    v_deleted_count INTEGER;
BEGIN
    -- Calcular data de corte
    v_cutoff_date := DATEADD(day, -p_days_to_keep, CURRENT_DATE());
    
    -- Deletar registros antigos já processados
    DELETE FROM BRONZE_CRYPTO_RAW
    WHERE TO_DATE(extracted_at) < v_cutoff_date
        AND processed = TRUE;
    
    v_deleted_count := SQLROWCOUNT;
    
    RETURN 'Archived ' || v_deleted_count || ' records older than ' || v_cutoff_date;
END;
$$;

-- ============================================================================
-- 8. Criar Task para Limpeza Automática (Comentado - ativar se necessário)
-- ============================================================================

/*
CREATE OR REPLACE TASK task_cleanup_old_bronze
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    SCHEDULE = 'USING CRON 0 2 * * 0 UTC'  -- Todo domingo às 2h UTC
AS
    CALL sp_archive_old_bronze_data(90);
*/

-- ============================================================================
-- 9. Grants de Permissão (Ajustar conforme necessário)
-- ============================================================================

-- Permitir SELECT para role de leitura
GRANT SELECT ON ALL VIEWS IN SCHEMA BRONZE TO ROLE PUBLIC;
GRANT SELECT ON TABLE BRONZE_CRYPTO_RAW TO ROLE PUBLIC;

-- Permitir INSERT para role de escrita (pipelines)
-- GRANT INSERT, UPDATE ON TABLE BRONZE_CRYPTO_RAW TO ROLE DATA_ENGINEER;

-- ============================================================================
-- 10. Queries de Teste/Validação
-- ============================================================================

-- Verificar estrutura da tabela
DESC TABLE BRONZE_CRYPTO_RAW;

-- Verificar clustering
SHOW TABLES LIKE 'BRONZE_CRYPTO_RAW';

-- Contar registros
SELECT COUNT(*) as total_records FROM BRONZE_CRYPTO_RAW;

-- Ver últimos registros
SELECT 
    id,
    payload:symbol::STRING as symbol,
    extracted_at,
    run_id,
    processed
FROM BRONZE_CRYPTO_RAW
ORDER BY extracted_at DESC
LIMIT 10;

-- Estatísticas por dia
SELECT 
    TO_DATE(extracted_at) as extraction_date,
    COUNT(*) as record_count,
    COUNT(DISTINCT payload:id) as unique_coins,
    MIN(extracted_at) as first_extraction,
    MAX(extracted_at) as last_extraction
FROM BRONZE_CRYPTO_RAW
GROUP BY TO_DATE(extracted_at)
ORDER BY extraction_date DESC;

-- ============================================================================
-- EXEMPLO DE USO NO NOTEBOOK
-- ============================================================================

/*
-- No notebook Python/Databricks:

import snowflake.connector

# Conectar
conn = snowflake.connector.connect(
    account='sua_conta',
    user='seu_usuario',
    password='sua_senha',
    warehouse='SNOWFLAKE_LEARNING_WH',
    database='CRYPTO_DB',
    schema='BRONZE'
)

# Inserir dados
import json
cur = conn.cursor()

for record in api_data:
    payload_json = json.dumps(record)
    cur.execute(
        """
        INSERT INTO BRONZE_CRYPTO_RAW(payload, extracted_at, run_id) 
        VALUES (parse_json(%s), %s, %s)
        """,
        (payload_json, record.get('extracted_at'), run_id)
    )

conn.commit()
cur.close()
conn.close()
*/

-- ============================================================================
-- DOCUMENTAÇÃO ADICIONAL
-- ============================================================================

/*
ESTRUTURA DO PAYLOAD (JSON):
{
    "id": "bitcoin",
    "symbol": "btc",
    "name": "Bitcoin",
    "current_price": 45000.50,
    "market_cap": 850000000000,
    "total_volume": 25000000000,
    "high_24h": 46000.00,
    "low_24h": 44000.00,
    "price_change_24h": 1000.00,
    "price_change_percentage_24h": 2.27,
    "market_cap_rank": 1,
    "circulating_supply": 19500000,
    "total_supply": 21000000,
    "max_supply": 21000000,
    "ath": 69000,
    "ath_date": "2021-11-10T14:24:11.849Z",
    "atl": 67.81,
    "atl_date": "2013-07-06T00:00:00.000Z",
    "extracted_at": "2026-01-19T21:00:00.000Z",
    "source": "coingecko_api"
}

QUERIES EM CAMPOS JSON (VARIANT):
- payload:id::STRING
- payload:current_price::FLOAT
- payload:market_cap::NUMBER
- payload:name::STRING
*/
