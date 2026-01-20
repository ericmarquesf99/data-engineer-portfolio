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
sys.path.append("/Workspace/Repos/<username>/enterprise-data-pipeline/src")

from loaders.snowflake_loader import SnowflakeLoader
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
# MAGIC ## Inicializar Spark e Snowflake

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# Configurar conexão Snowflake
snowflake_config = {
    "snowflake": {
        "account": snowflake_account,
        "user": snowflake_user,
        "password": snowflake_password,
        "warehouse": snowflake_warehouse,
        "database": snowflake_database,
        "schema": snowflake_schema
    }
}

# Alternativa: Se usar arquivo config
# config = load_config()
# snowflake_config = config

loader = SnowflakeLoader(snowflake_config)

print("✅ Snowflake Loader inicializado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar Silver Data

# COMMAND ----------

start_time = datetime.now()

try:
    logger.log_event("loading_silver_started", {})
    
    # Ler view temporária do notebook de transformação
    silver_df_spark = spark.table("silver_crypto_temp")
    
    # Converter para Pandas para usar snowflake-connector-python
    silver_df = silver_df_spark.toPandas()
    
    logger.log_event("silver_data_converted", {"records": len(silver_df)})
    
    # Carregar para staging e fazer merge
    loader.connect()
    loader.setup_database()  # Garante que tabelas existem
    
    # Staging + Merge
    loader.load_dataframe_to_stage(silver_df, "silver_crypto_clean_stage")
    rows_affected = loader.merge_silver_data()
    
    logger.log_event("silver_loaded", {"rows_affected": rows_affected})
    
    print(f"✅ Silver Data carregada: {rows_affected} linhas afetadas")
    
except Exception as e:
    logger.log_event("silver_loading_error", {"error": str(e)}, level="ERROR")
    loader.disconnect()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar Gold Data

# COMMAND ----------

try:
    logger.log_event("loading_gold_started", {})
    
    # Ler view temporária
    gold_df_spark = spark.table("gold_crypto_temp")
    gold_df = gold_df_spark.toPandas()
    
    logger.log_event("gold_data_converted", {"records": len(gold_df)})
    
    # Staging + Merge
    loader.load_dataframe_to_stage(gold_df, "gold_crypto_metrics_stage")
    rows_affected = loader.merge_gold_data()
    
    logger.log_event("gold_loaded", {"rows_affected": rows_affected})
    
    print(f"✅ Gold Data carregada: {rows_affected} linhas afetadas")
    
except Exception as e:
    logger.log_event("gold_loading_error", {"error": str(e)}, level="ERROR")
    loader.disconnect()
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registrar Métricas do Pipeline

# COMMAND ----------

try:
    # Registrar metadados da execução
    pipeline_metadata = {
        "run_id": run_id,
        "pipeline_name": "crypto_data_pipeline",
        "status": "success",
        "silver_records": len(silver_df),
        "gold_records": len(gold_df),
        "execution_timestamp": datetime.now().isoformat()
    }
    
    loader.log_pipeline_metrics(pipeline_metadata)
    
    logger.log_event("pipeline_metadata_logged", pipeline_metadata)
    
    print("✅ Métricas do pipeline registradas")
    
except Exception as e:
    logger.log_event("metadata_logging_error", {"error": str(e)}, level="WARNING")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalização

# COMMAND ----------

loader.disconnect()

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

result = {
    "status": "success",
    "loaded_records": len(silver_df) + len(gold_df),
    "silver_rows": len(silver_df),
    "gold_rows": len(gold_df),
    "duration_seconds": duration
}

logger.log_event("loading_completed", result)

print(f"\n⏱️  Duração total: {duration:.2f}s")
print(f"📊 Total carregado: {result['loaded_records']} registros")

# COMMAND ----------

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
