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

logger = StructuredLogger("transformation")
logger.log_event("transformation_notebook_started", {"run_id": run_id})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recuperar Dados da Tabela Bronze no Snowflake

# COMMAND ----------

# Recuperar credenciais Snowflake do Azure Key Vault
try:
    snowflake_config = get_snowflake_credentials_from_keyvault("kv-crypto-pipeline")
    logger.log_event("snowflake_credentials_loaded", {"vault": "kv-crypto-pipeline"})
    print("✅ Credenciais Snowflake recuperadas do Key Vault")
except Exception as e:
    logger.log_event("keyvault_error", {"error": str(e)}, level="ERROR")
    print(f"❌ Erro ao recuperar credenciais: {e}")
    raise

# COMMAND ----------

import snowflake.connector
import json

# Conectar no Snowflake usando schema BRONZE
conn = snowflake.connector.connect(
    account=snowflake_config['account'],
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    warehouse=snowflake_config['warehouse'],
    database=snowflake_config['database'],
    schema='BRONZE'
)
cur = conn.cursor()

# Garantir schema Bronze
cur.execute("USE SCHEMA BRONZE")

# Buscar dados da tabela BRONZE_CRYPTO_RAW
logger.log_event("reading_bronze_table", {"table": "BRONZE.BRONZE_CRYPTO_RAW"})
print("📊 Reading data from BRONZE.BRONZE_CRYPTO_RAW table...")

cur.execute("""
    SELECT 
        id,
        payload,
        extracted_at,
        run_id,
        source_system,
        created_at
    FROM BRONZE.BRONZE_CRYPTO_RAW
    WHERE processed = FALSE
    ORDER BY created_at DESC
""")

# Converter resultados em lista de dicts
bronze_data = []
for row in cur.fetchall():
    payload_json = json.loads(row[1]) if isinstance(row[1], str) else row[1]
    bronze_data.append(payload_json)

cur.close()
conn.close()

print(f"✅ {len(bronze_data)} registros recuperados da Bronze")

# Converter para Spark DataFrame
df_bronze_pandas = pd.json_normalize(bronze_data)
df_bronze = spark.createDataFrame(df_bronze_pandas)

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
    
    # NOTE: Cache disabled - Databricks Community Edition does not support DataFrame caching
    # df_silver.cache()
    
    logger.log_event("silver_transformation_completed", {
        "output_records": silver_count
    })
    
    print(f"✅ Silver Layer: {silver_count} registros processados")
    print(f"\n📊 Amostra dos dados:")
    df_silver.select("id", "symbol", "current_price", "market_cap", "processed_at").limit(5).display()
    
    # Carregar Silver para Snowflake
    logger.log_event("loading_silver_to_snowflake", {"records": silver_count})
    
    # Usar schema SILVER
    cur.execute("USE SCHEMA SILVER")
    
    # Coletar dados do Spark DataFrame
    silver_data = df_silver.collect()
    
    inserted_silver = 0
    for row in silver_data:
        # Inserir cada registro na tabela silver_crypto_clean
        cur.execute("""
            INSERT INTO silver_crypto_clean (
                coin_id, symbol, name, current_price, market_cap, 
                market_cap_rank, total_volume, price_change_percentage_24h,
                updated_at, run_id, is_current
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, TRUE)
        """, (
            row['id'],
            row['symbol'] if 'symbol' in row else None,
            row['name'] if 'name' in row else None,
            float(row['current_price']) if 'current_price' in row and row['current_price'] else None,
            float(row['market_cap']) if 'market_cap' in row and row['market_cap'] else None,
            int(row['market_cap_rank']) if 'market_cap_rank' in row and row['market_cap_rank'] else None,
            float(row['total_volume']) if 'total_volume' in row and row['total_volume'] else None,
            float(row['price_change_percentage_24h']) if 'price_change_percentage_24h' in row and row['price_change_percentage_24h'] else None,
            row['processed_at'],
            run_id
        ))
        inserted_silver += 1
    
    conn.commit()
    
    logger.log_event("silver_loaded_to_snowflake", {"records": inserted_silver})
    print(f"❄️  Snowflake Silver: {inserted_silver} registros inseridos")
    
except Exception as e:
    logger.log_event("transformation_error", {"error": str(e)}, level="ERROR")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criar Gold Layer (Agregações)

# COMMAND ----------

try:
    # Criar agregações por categoria de market cap
    logger.log_event("creating_gold_aggregations", {})
    
    # Adicionar categorização de market cap
    df_categorized = df_silver.withColumn(
        "market_cap_category",
        F.when(F.col("market_cap") >= 10000000000, "LARGE_CAP")
         .when(F.col("market_cap") >= 1000000000, "MID_CAP")
         .otherwise("SMALL_CAP")
    )
    
    # Agregar por categoria
    df_gold = df_categorized \
        .groupBy("market_cap_category") \
        .agg(
            F.count("*").alias("num_coins"),
            F.sum("market_cap").alias("total_market_cap"),
            F.avg("market_cap").alias("avg_market_cap"),
            F.sum("total_volume").alias("total_volume"),
            F.avg("current_price").alias("avg_price"),
            F.avg("price_change_percentage_24h").alias("avg_price_change_24h")
        )
    
    gold_count = df_gold.count()
    
    # NOTE: Cache disabled - Databricks Community Edition does not support DataFrame caching
    # df_gold.cache()
    
    logger.log_event("gold_aggregation_completed", {
        "categories": gold_count
    })
    
    print(f"✅ Gold Layer: {gold_count} categorias agregadas")
    print(f"\n📊 Amostra das agregações:")
    df_gold.display()
    
    # Carregar Gold para Snowflake
    logger.log_event("loading_gold_to_snowflake", {"categories": gold_count})
    
    # Usar schema GOLD
    cur.execute("USE SCHEMA GOLD")
    
    # Coletar dados agregados
    gold_data = df_gold.collect()
    
    inserted_gold = 0
    for row in gold_data:
        cur.execute("""
            INSERT INTO gold_crypto_metrics (
                metric_date, market_cap_category, num_coins, total_market_cap,
                avg_market_cap, total_volume, avg_price, avg_price_change_24h,
                created_at, run_id
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            datetime.now().date(),
            row['market_cap_category'],
            int(row['num_coins']),
            float(row['total_market_cap']) if row['total_market_cap'] else None,
            float(row['avg_market_cap']) if row['avg_market_cap'] else None,
            float(row['total_volume']) if row['total_volume'] else None,
            float(row['avg_price']) if row['avg_price'] else None,
            float(row['avg_price_change_24h']) if row['avg_price_change_24h'] else None,
            datetime.now(),
            run_id
        ))
        inserted_gold += 1
    
    conn.commit()
    
    logger.log_event("gold_loaded_to_snowflake", {"categories": inserted_gold})
    print(f"❄️  Snowflake Gold: {inserted_gold} categorias inseridas")
    
except Exception as e:
    logger.log_event("gold_aggregation_error", {"error": str(e)}, level="ERROR")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Finalizar

# COMMAND ----------

# Fechar conexão Snowflake
cur.close()
conn.close()

print("✅ Conexão Snowflake fechada")

# Criar views temporárias (opcional, para compatibilidade)
df_silver.createOrReplaceTempView("crypto_data_silver")
df_gold.createOrReplaceTempView("crypto_data_gold")

logger.log_event("temp_views_created", {
    "silver_view": "crypto_data_silver",
    "gold_view": "crypto_data_gold"
})

print("✅ Views temporárias criadas (opcional)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultado Final

# COMMAND ----------

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

result = {
    "status": "success",
    "silver_records": inserted_silver,
    "gold_categories": inserted_gold,
    "duration_seconds": duration
}

logger.log_event("transformation_completed", result)

print(f"\n✅ Transformação completa!")
print(f"   Bronze → Silver: {inserted_silver} registros")
print(f"   Silver → Gold: {inserted_gold} categorias")
print(f"   ⏱️  Duração: {duration:.2f}s")

# COMMAND ----------

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
