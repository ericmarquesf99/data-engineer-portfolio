# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Transformation Notebook - Silver & Gold Layers
# MAGIC 
# MAGIC **Input:** Bronze JSON files (DBFS)  
# MAGIC **Processing:** PySpark com validações e limpeza  
# MAGIC **Output:** DataFrames Silver e Gold (em memória para carga)

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

from transformers.spark_processor import SparkProcessor
from utils.logging_config import StructuredLogger

# COMMAND ----------

# Obter parâmetros
dbutils.widgets.text("run_id", "", "Run ID")
dbutils.widgets.text("input_path", "", "Input Path")
dbutils.widgets.text("environment", "production", "Environment")

run_id = dbutils.widgets.get("run_id")
input_path = dbutils.widgets.get("input_path")
environment = dbutils.widgets.get("environment")

logger = StructuredLogger("transformation")
logger.log_event("transformation_notebook_started", {
    "run_id": run_id,
    "input": input_path,
    "environment": environment
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inicializar Spark Session

# COMMAND ----------

spark = SparkSession.builder \
    .appName("CryptoDataTransformation") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

print(f"✅ Spark Session inicializada: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processar Bronze → Silver

# COMMAND ----------

start_time = datetime.now()

try:
    # Inicializar processador
    processor = SparkProcessor(spark)
    
    logger.log_event("reading_bronze_data", {"path": input_path})
    
    # Ler dados do DBFS
    bronze_df = spark.read.json(input_path)
    
    # Processar para Silver (limpeza e validação)
    logger.log_event("transforming_to_silver", {})
    silver_df = processor.process_bronze_to_silver(bronze_df)
    
    # Validar qualidade dos dados
    validation_results = processor.validate_data_quality(silver_df)
    
    logger.log_event("silver_validation", {
        "total_records": validation_results.get("total_records"),
        "null_checks": validation_results.get("null_checks"),
        "anomalies_detected": validation_results.get("anomalies_detected", 0)
    })
    
    silver_count = silver_df.count()
    
    # Cache para próximos processamentos
    silver_df.cache()
    
    print(f"✅ Silver Layer: {silver_count} registros processados")
    
except Exception as e:
    logger.log_event("silver_transformation_error", {"error": str(e)}, level="ERROR")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Processar Silver → Gold

# COMMAND ----------

try:
    logger.log_event("transforming_to_gold", {})
    
    # Criar agregações para Gold Layer
    gold_df = processor.process_silver_to_gold(silver_df)
    
    gold_count = gold_df.count()
    
    # Cache para carga
    gold_df.cache()
    
    logger.log_event("gold_transformation_completed", {
        "records": gold_count
    })
    
    print(f"✅ Gold Layer: {gold_count} métricas agregadas")
    
    # Mostrar sample
    print("\n📊 Sample Gold Metrics:")
    gold_df.select("coin_id", "avg_price_usd", "market_cap_usd", "volume_24h").show(5, truncate=False)
    
except Exception as e:
    logger.log_event("gold_transformation_error", {"error": str(e)}, level="ERROR")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar DataFrames em Views Temporárias

# COMMAND ----------

# Criar views temporárias para o notebook de loading acessar
silver_df.createOrReplaceTempView("silver_crypto_temp")
gold_df.createOrReplaceTempView("gold_crypto_temp")

logger.log_event("temp_views_created", {
    "silver_view": "silver_crypto_temp",
    "gold_view": "gold_crypto_temp"
})

print("✅ Views temporárias criadas:")
print("   - silver_crypto_temp")
print("   - gold_crypto_temp")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalização

# COMMAND ----------

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

result = {
    "status": "success",
    "silver_records": silver_count,
    "gold_records": gold_count,
    "duration_seconds": duration,
    "validation": validation_results
}

logger.log_event("transformation_completed", result)

print(f"\n⏱️  Duração total: {duration:.2f}s")

# COMMAND ----------

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
