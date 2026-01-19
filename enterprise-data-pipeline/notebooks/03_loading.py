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
from utils.config_loader import load_config

# COMMAND ----------

# Obter parâmetros
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("snowflake_account", "", "Snowflake Account")
dbutils.widgets.text("snowflake_user", "", "Snowflake User")
dbutils.widgets.text("snowflake_password", "", "Snowflake Password")
dbutils.widgets.text("snowflake_warehouse", "", "Snowflake Warehouse")
dbutils.widgets.text("snowflake_database", "", "Snowflake Database")
dbutils.widgets.text("snowflake_schema", "", "Snowflake Schema")

run_id = dbutils.widgets.get("run_id")
snowflake_account = dbutils.widgets.get("snowflake_account")
snowflake_user = dbutils.widgets.get("snowflake_user")
snowflake_password = dbutils.widgets.get("snowflake_password")
snowflake_warehouse = dbutils.widgets.get("snowflake_warehouse")
snowflake_database = dbutils.widgets.get("snowflake_database")
snowflake_schema = dbutils.widgets.get("snowflake_schema")

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
