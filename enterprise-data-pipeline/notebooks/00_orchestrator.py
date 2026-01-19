# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Pipeline Orchestrator - Crypto Data
# MAGIC 
# MAGIC **Propósito:** Notebook principal que coordena todo o pipeline ETL  
# MAGIC **Fluxo:** Extraction → Transformation → Loading  
# MAGIC **Orquestração:** Databricks Jobs (scheduled runs)
# MAGIC 
# MAGIC ---
# MAGIC ### 📊 Arquitetura Medallion
# MAGIC - **Bronze**: Dados brutos da API CoinGecko (DBFS)
# MAGIC - **Silver**: Dados limpos e validados (Snowflake)
# MAGIC - **Gold**: Métricas agregadas e insights (Snowflake)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ Setup e Imports

# COMMAND ----------

import sys
import os
from datetime import datetime
import json

# Adicionar src ao path para imports
sys.path.append("/Workspace/Repos/<username>/enterprise-data-pipeline/src")
# Para execução local/testing:
# sys.path.append(os.path.abspath("../src"))

# COMMAND ----------

# Imports dos módulos do pipeline
from extractors.coingecko_extractor import CryptoExtractor
from transformers.spark_processor import SparkProcessor
from loaders.snowflake_loader import SnowflakeLoader
from utils.logging_config import StructuredLogger

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ Configuração

# COMMAND ----------

# Inicializar logger estruturado
logger = StructuredLogger("orchestrator")
run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

logger.log_event("pipeline_started", {"run_id": run_id, "environment": "databricks"})

# COMMAND ----------

# Configuração do ambiente
DBFS_BRONZE_PATH = "dbfs:/mnt/data/bronze/crypto/"
CONFIG_PATH = "/Workspace/Repos/<username>/enterprise-data-pipeline/config/config.yaml"

# Para ambiente Databricks, usar secrets:
SNOWFLAKE_ACCOUNT = dbutils.secrets.get(scope="snowflake", key="account")
SNOWFLAKE_USER = dbutils.secrets.get(scope="snowflake", key="user")
SNOWFLAKE_PASSWORD = dbutils.secrets.get(scope="snowflake", key="password")
SNOWFLAKE_WAREHOUSE = dbutils.secrets.get(scope="snowflake", key="warehouse")
SNOWFLAKE_DATABASE = dbutils.secrets.get(scope="snowflake", key="database")
SNOWFLAKE_SCHEMA = dbutils.secrets.get(scope="snowflake", key="schema")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ Fase de Extração (Bronze)

# COMMAND ----------

logger.log_event("extraction_started", {"target": DBFS_BRONZE_PATH})

try:
    # Executar notebook de extração
    result = dbutils.notebook.run(
        "./01_extraction",
        timeout_seconds=600,
        arguments={"run_id": run_id, "output_path": DBFS_BRONZE_PATH}
    )
    
    result_data = json.loads(result)
    bronze_file_path = result_data["output_file"]
    record_count = result_data["record_count"]
    
    logger.log_event("extraction_completed", {
        "file": bronze_file_path,
        "records": record_count,
        "duration_seconds": result_data.get("duration_seconds")
    })
    
    print(f"✅ Extração completa: {record_count} registros → {bronze_file_path}")
    
except Exception as e:
    logger.log_event("extraction_failed", {"error": str(e)}, level="ERROR")
    dbutils.notebook.exit(json.dumps({"status": "failed", "phase": "extraction", "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4️⃣ Fase de Transformação (Silver)

# COMMAND ----------

logger.log_event("transformation_started", {"input": bronze_file_path})

try:
    # Executar notebook de transformação
    result = dbutils.notebook.run(
        "./02_transformation",
        timeout_seconds=1200,
        arguments={
            "run_id": run_id,
            "input_path": bronze_file_path,
            "environment": "production"
        }
    )
    
    result_data = json.loads(result)
    silver_records = result_data["silver_records"]
    gold_records = result_data["gold_records"]
    
    logger.log_event("transformation_completed", {
        "silver_records": silver_records,
        "gold_records": gold_records,
        "duration_seconds": result_data.get("duration_seconds")
    })
    
    print(f"✅ Transformação completa:")
    print(f"   - Silver: {silver_records} registros")
    print(f"   - Gold: {gold_records} registros")
    
except Exception as e:
    logger.log_event("transformation_failed", {"error": str(e)}, level="ERROR")
    dbutils.notebook.exit(json.dumps({"status": "failed", "phase": "transformation", "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5️⃣ Fase de Carga (Snowflake)

# COMMAND ----------

logger.log_event("loading_started", {"target": "snowflake"})

try:
    # Executar notebook de carga
    result = dbutils.notebook.run(
        "./03_loading",
        timeout_seconds=900,
        arguments={
            "run_id": run_id,
            "snowflake_account": SNOWFLAKE_ACCOUNT,
            "snowflake_user": SNOWFLAKE_USER,
            "snowflake_password": SNOWFLAKE_PASSWORD,
            "snowflake_warehouse": SNOWFLAKE_WAREHOUSE,
            "snowflake_database": SNOWFLAKE_DATABASE,
            "snowflake_schema": SNOWFLAKE_SCHEMA
        }
    )
    
    result_data = json.loads(result)
    loaded_records = result_data["loaded_records"]
    
    logger.log_event("loading_completed", {
        "records_loaded": loaded_records,
        "duration_seconds": result_data.get("duration_seconds")
    })
    
    print(f"✅ Carga completa: {loaded_records} registros no Snowflake")
    
except Exception as e:
    logger.log_event("loading_failed", {"error": str(e)}, level="ERROR")
    dbutils.notebook.exit(json.dumps({"status": "failed", "phase": "loading", "error": str(e)}))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6️⃣ Finalização e Métricas

# COMMAND ----------

# Registrar métricas do pipeline
pipeline_end = datetime.now()
logger.log_event("pipeline_completed", {
    "run_id": run_id,
    "total_records_processed": silver_records,
    "status": "success"
})

# Retornar resultado final
final_result = {
    "status": "success",
    "run_id": run_id,
    "extraction": {"file": bronze_file_path, "records": record_count},
    "transformation": {"silver": silver_records, "gold": gold_records},
    "loading": {"records": loaded_records},
    "timestamp": datetime.now().isoformat()
}

print("\n" + "="*50)
print("🎉 PIPELINE EXECUTADO COM SUCESSO!")
print("="*50)
print(json.dumps(final_result, indent=2))

# COMMAND ----------

# Retornar resultado para Databricks Jobs
dbutils.notebook.exit(json.dumps(final_result))
