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
# MAGIC ## Configurar Azure Service Principal

# COMMAND ----------

import os

# Configurar credenciais do Service Principal para Azure Key Vault
os.environ['AZURE_TENANT_ID'] = "518d08e5-ea11-4f47-bab2-dbaa4ebbbb76"
os.environ['AZURE_CLIENT_ID'] = "6ef62d52-f175-4c59-b4fc-5b7c59e5384c"
os.environ['AZURE_CLIENT_SECRET'] = "9e951b28-962c-4818-bfe7-396b5cb156c0"

print("🔐 Service Principal configurado para Azure Key Vault")

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
# MAGIC ## Carregar Silver Data

# COMMAND ----------

start_time = datetime.now()

try:
    logger.log_event("loading_silver_started", {})
    
    # Ler DataFrame do notebook de transformação
    silver_df_spark = spark.table("crypto_data_silver")
    
    logger.log_event("silver_data_loaded", {"records": silver_df_spark.count()})
    
    # Usar schema SILVER
    cur.execute("USE SCHEMA SILVER")
    
    # Inserir dados na tabela silver_crypto_clean
    # Converter Spark DataFrame para lista de dicts
    silver_data = [row.asDict() for row in silver_df_spark.collect()]
    
    inserted = 0
    for record in silver_data:
        # Inserir cada registro
        cur.execute("""
            INSERT INTO silver_crypto_clean (
                coin_id, symbol, name, current_price, market_cap, 
                market_cap_rank, total_volume, price_change_percentage_24h,
                updated_at, run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record.get('id'),
            record.get('symbol'),
            record.get('name'),
            record.get('current_price'),
            record.get('market_cap'),
            record.get('market_cap_rank'),
            record.get('total_volume'),
            record.get('price_change_percentage_24h'),
            record.get('processed_at'),
            run_id
        ))
        inserted += 1
    
    conn.commit()
    
    logger.log_event("silver_loaded", {"rows_inserted": inserted})
    
    print(f"✅ Silver Data carregada: {inserted} registros inseridos")
    
except Exception as e:
    logger.log_event("silver_loading_error", {"error": str(e)}, level="ERROR")
    conn.rollback()
    cur.close()
    conn.close()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar Gold Data

# COMMAND ----------

try:
    logger.log_event("loading_gold_started", {})
    
    # Ler DataFrame Gold
    gold_df_spark = spark.table("crypto_data_gold")
    
    logger.log_event("gold_data_loaded", {"records": gold_df_spark.count()})
    
    # Usar schema GOLD
    cur.execute("USE SCHEMA GOLD")
    
    # Inserir dados na tabela gold_crypto_metrics
    gold_data = [row.asDict() for row in gold_df_spark.collect()]
    
    inserted = 0
    for record in gold_data:
        cur.execute("""
            INSERT INTO gold_crypto_metrics (
                metric_date, market_cap_category, num_coins, total_market_cap,
                avg_market_cap, total_volume, created_at, run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now().date(),
            record.get('market_cap_category'),
            record.get('count'),
            record.get('total_market_cap'),
            record.get('avg_market_cap'),
            record.get('total_volume'),
            datetime.now(),
            run_id
        ))
        inserted += 1
    
    conn.commit()
    
    logger.log_event("gold_loaded", {"rows_inserted": inserted})
    
    print(f"✅ Gold Data carregada: {inserted} registros inseridos")
    
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
        silver_df_spark.count(),
        silver_df_spark.count(),
        inserted,
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
