# Databricks notebook source
# MAGIC %md
# MAGIC # 📤 Loading Notebook - Snowflake Warehouse
# MAGIC 
# MAGIC **Input:** Silver e Gold DataFrames (temp views)  
# MAGIC **Destino:** Snowflake (Silver e Gold schemas)  
# MAGIC **Método:** Staging tables + MERGE (upsert)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
from datetime import datetime
import json
from pyspark.sql import SparkSession

# Adicionar src ao path
sys.path.append("/Workspace/Users/ericmarques1999@gmail.com/data-engineer-portfolio/enterprise-data-pipeline/src")

from utils.logging_config import StructuredLogger
from utils.config_loader import load_config, get_snowflake_credentials_from_keyvault

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Azure Service Principal
# MAGIC 
# MAGIC Credentials loaded from `config/credentials.yaml` file.

# COMMAND ----------

import os
import yaml

# Load credentials from config file
config_path = "/Workspace/Users/ericmarques1999@gmail.com/data-engineer-portfolio/enterprise-data-pipeline/config/credentials.yaml"

try:
    with open(config_path, 'r') as f:
        credentials = yaml.safe_load(f)
    
    os.environ['AZURE_TENANT_ID'] = credentials['azure']['tenant_id']
    os.environ['AZURE_CLIENT_ID'] = credentials['azure']['client_id']
    os.environ['AZURE_CLIENT_SECRET'] = credentials['azure']['client_secret']
    
    print("✅ Azure Service Principal credentials loaded from config file")
except FileNotFoundError:
    print("❌ Error: config/credentials.yaml not found!")
    print("💡 Copy config/credentials.yaml.example to config/credentials.yaml and fill with your values")
    raise
except Exception as e:
    print(f"❌ Error loading credentials: {e}")
    raise

# COMMAND ----------

# Obter parâmetros
dbutils.widgets.text("run_id", "", "Run ID")

run_id = dbutils.widgets.get("run_id")

# Recuperar credenciais Snowflake do Azure Key Vault
try:
    snowflake_config = get_snowflake_credentials_from_keyvault("kv-crypto-pipeline")
    snowflake_account = snowflake_config['account']
    snowflake_user = snowflake_config['user']
    snowflake_password = snowflake_config['password']
    snowflake_warehouse = snowflake_config['warehouse']
    snowflake_database = snowflake_config['database']
    snowflake_schema = snowflake_config['schema']
    print("✅ Credenciais Snowflake recuperadas do Key Vault")
except Exception as e:
    print(f"❌ Erro ao recuperar credenciais: {e}")
    raise

logger = StructuredLogger("loading")
logger.log_event("loading_notebook_started", {
    "run_id": run_id,
    "target": "snowflake"
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicializar Conexão Snowflake

# COMMAND ----------

import snowflake.connector

# Conectar no Snowflake
conn = snowflake.connector.connect(
    account=snowflake_config['account'],
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema='SILVER'
)
cur = conn.cursor()

print("✅ Conexão Snowflake estabelecida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar Silver Data do Snowflake

# COMMAND ----------

start_time = datetime.now()

try:
    logger.log_event("loading_silver_started", {})
    
    # Ler dados da tabela SILVER do Snowflake
    print("📊 Reading data from SILVER.silver_crypto_clean table...")
    
    cur.execute("USE SCHEMA SILVER")
    cur.execute("SELECT * FROM silver_crypto_clean WHERE is_current = TRUE")
    
    silver_records = cur.fetchall()
    record_count = len(silver_records)
    
    logger.log_event("silver_data_loaded_from_snowflake", {"records": record_count})
    
    print(f"✅ {record_count} registros lidos do Snowflake Silver")
    
except Exception as e:
    logger.log_event("silver_loading_error", {"error": str(e)}, level="ERROR")
    conn.rollback()
    cur.close()
    conn.close()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar Agregações Gold e Carregar

# COMMAND ----------

try:
    logger.log_event("loading_gold_started", {})
    
    # Agregar dados Silver para Gold (métricas por categoria)
    cur.execute("USE SCHEMA SILVER")
    
    # Criar agregações diretamente no Snowflake
    cur.execute("""
        SELECT 
            CASE 
                WHEN market_cap >= 10000000000 THEN 'LARGE_CAP'
                WHEN market_cap >= 1000000000 THEN 'MID_CAP'
                ELSE 'SMALL_CAP'
            END as market_cap_category,
            COUNT(*) as num_coins,
            SUM(market_cap) as total_market_cap,
            AVG(market_cap) as avg_market_cap,
            SUM(total_volume) as total_volume,
            AVG(price_change_percentage_24h) as avg_price_change_24h
        FROM silver_crypto_clean
        WHERE is_current = TRUE
        GROUP BY market_cap_category
    """)
    
    gold_records = cur.fetchall()
    
    logger.log_event("gold_aggregation_completed", {"categories": len(gold_records)})
    
    # Inserir na tabela GOLD
    cur.execute("USE SCHEMA GOLD")
    
    inserted = 0
    for record in gold_records:
        cur.execute("""
            INSERT INTO gold_crypto_metrics (
                metric_date, market_cap_category, num_coins, total_market_cap,
                avg_market_cap, total_volume, avg_price_change_24h, 
                created_at, run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now().date(),
            record[0],  # market_cap_category
            record[1],  # num_coins
            record[2],  # total_market_cap
            record[3],  # avg_market_cap
            record[4],  # total_volume
            record[5],  # avg_price_change_24h
            datetime.now(),
            run_id
        ))
        inserted += 1
    
    conn.commit()
    
    logger.log_event("gold_loaded", {"rows_inserted": inserted})
    
    print(f"✅ Gold Data carregada: {inserted} categorias inseridas")
    
except Exception as e:
    logger.log_event("gold_loading_error", {"error": str(e)}, level="ERROR")
    conn.rollback()
    cur.close()
    conn.close()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registrar Métricas do Pipeline

# COMMAND ----------

try:
    # Usar schema PUBLIC para metadata
    cur.execute("USE SCHEMA PUBLIC")
    
    # Registrar metadados da execução
    execution_time = (datetime.now() - start_time).total_seconds()
    
    cur.execute("""
        INSERT INTO pipeline_metadata (
            run_id, pipeline_name, status, records_extracted, 
            records_processed, records_loaded, execution_time_seconds,
            started_at, completed_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        run_id,
        'crypto_data_pipeline',
        'success',
        record_count,
        record_count,
        record_count + inserted,
        execution_time,
        start_time,
        datetime.now()
    ))
    
    conn.commit()
    
    logger.log_event("pipeline_metadata_logged", {"run_id": run_id})
    
    print("✅ Métricas do pipeline registradas")
    
except Exception as e:
    logger.log_event("metadata_logging_error", {"error": str(e)}, level="WARNING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalização

# COMMAND ----------

cur.close()
conn.close()

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

result = {
    "status": "success",
    "loaded_records": inserted,
    "duration_seconds": duration
}

logger.log_event("loading_completed", result)

print(f"\n⏱️  Duração total: {duration:.2f}s")
print(f"📊 Total carregado: {result['loaded_records']} registros")

# COMMAND ----------

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
