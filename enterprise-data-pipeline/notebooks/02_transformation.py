# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Transformation Notebook - Silver Layer
# MAGIC
# MAGIC **Input:** crypto_data_raw (do notebook 01_extraction)  
# MAGIC **Processing:** Limpeza, validação e transformação  
# MAGIC **Output:** crypto_data_silver (table temporária)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
from datetime import datetime
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# Adicionar src ao path
sys.path.append("/Workspace/Users/ericmarques1999@gmail.com/data-engineer-portfolio/enterprise-data-pipeline/src")

from transformers.spark_processor import SparkProcessor
from utils.logging_config import StructuredLogger
from utils.config_loader import load_config, get_snowflake_credentials_from_keyvault

# COMMAND ----------

# Obter parâmetros
dbutils.widgets.text("run_id", "", "Run ID")

run_id = dbutils.widgets.get("run_id")

logger = StructuredLogger("transformation")
logger.log_event("transformation_notebook_started", {"run_id": run_id})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recuperar Dados do Notebook de Extração

# COMMAND ----------

# Recuperar DataFrame do notebook anterior
df_bronze = spark.table("crypto_data_raw")

print(f"✅ Dados recuperados: {df_bronze.count()} registros")
print(f"\n📊 Colunas disponíveis:")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpeza e Transformação

# COMMAND ----------

start_time = datetime.now()

try:
    # Carregar configuração
    config = load_config()
    
    # Inicializar processador
    processor = SparkProcessor(config)
    
    logger.log_event("transforming_bronze_to_silver", {
        "input_records": df_bronze.count()
    })
    
    # Processamento básico de limpeza
    df_silver = df_bronze \
        .dropDuplicates() \
        .filter(F.col("id").isNotNull()) \
        .filter(F.col("current_price").isNotNull())
    
    # Adicionar coluna de processamento
    df_silver = df_silver.withColumn(
        "processed_at",
        F.lit(datetime.now().isoformat())
    )
    
    # Converter colunas numéricas
    numeric_cols = ["current_price", "market_cap", "total_volume", "market_cap_rank"]
    for col in numeric_cols:
        if col in df_silver.columns:
            df_silver = df_silver.withColumn(col, F.col(col).cast(DoubleType()))
    
    silver_count = df_silver.count()
    
    # Cache para próximos processamentos
    df_silver.cache()
    
    logger.log_event("silver_transformation_completed", {
        "output_records": silver_count
    })
    
    print(f"✅ Silver Layer: {silver_count} registros processados")
    print(f"\n📊 Amostra dos dados:")
    df_silver.select("id", "symbol", "current_price", "market_cap", "processed_at").limit(5).display()
    
except Exception as e:
    logger.log_event("transformation_error", {"error": str(e)}, level="ERROR")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar Gold Layer (Agregações)

# COMMAND ----------

try:
    # Criar agregações por categoria (se existir)
    if "market_cap_rank" in df_silver.columns:
        df_gold = df_silver \
            .groupBy(F.col("symbol")) \
            .agg(
                F.max("current_price").alias("max_price"),
                F.min("current_price").alias("min_price"),
                F.avg("current_price").alias("avg_price"),
                F.max("market_cap").alias("max_market_cap"),
                F.count("*").alias("record_count")
            )
    else:
        df_gold = df_silver
    
    gold_count = df_gold.count()
    
    # Cache para carga
    df_gold.cache()
    
    logger.log_event("gold_aggregation_completed", {
        "records": gold_count
    })
    
    print(f"✅ Gold Layer: {gold_count} registros agregados")
    print(f"\n📊 Amostra das agregações:")
    df_gold.limit(5).display()
    
except Exception as e:
    logger.log_event("gold_aggregation_error", {"error": str(e)}, level="ERROR")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvar DataFrames em Views Temporárias

# COMMAND ----------

# Criar views temporárias para o notebook de loading acessar
df_silver.createOrReplaceTempView("crypto_data_silver")
df_gold.createOrReplaceTempView("crypto_data_gold")

logger.log_event("temp_views_created", {
    "silver_view": "crypto_data_silver",
    "gold_view": "crypto_data_gold"
})

print("✅ Views temporárias criadas:")
print("   - crypto_data_silver")
print("   - crypto_data_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

result = {
    "status": "success",
    "silver_records": silver_count,
    "gold_records": gold_count,
    "duration_seconds": duration
}

logger.log_event("transformation_completed", result)

print(f"\n✅ Transformação completa!")
print(f"   Bronze → Silver: {silver_count} registros")
print(f"   Silver → Gold: {gold_count} agregações")
print(f"   ⏱️  Duração: {duration:.2f}s")

# COMMAND ----------

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
