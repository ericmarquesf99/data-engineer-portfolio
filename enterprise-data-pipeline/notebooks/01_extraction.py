# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Extraction Notebook - Bronze Layer
# MAGIC
# MAGIC **Fonte:** CoinGecko API v3  
# MAGIC **Destino:** DBFS (Bronze Layer)  
# MAGIC **Formato:** JSON files com timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import sys
import os
from datetime import datetime
import json

# Adicionar src ao path
sys.path.append("/Workspace/Users/ericmarques1999@gmail.com/data-engineer-portfolio/enterprise-data-pipeline/src")

from extractors.coingecko_extractor import APIExtractor
from utils.logging_config import StructuredLogger 
from utils.config_loader import load_config, get_snowflake_credentials_from_keyvault

# COMMAND ----------

# Obter parâmetros do notebook pai
dbutils.widgets.text("run_id", "", "Run ID")

run_id = dbutils.widgets.get("run_id")

logger = StructuredLogger("extraction")
logger.log_event("extraction_notebook_started", {"run_id": run_id})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executar Extração

# COMMAND ----------

start_time = datetime.now()

try:
    # Carregar configuração
    config = load_config()
    
    # Recuperar credenciais Snowflake do Azure Key Vault
    try:
        snowflake_config = get_snowflake_credentials_from_keyvault("kv-crypto-pipeline")
        logger.log_event("snowflake_credentials_loaded", {"vault": "kv-crypto-pipeline"})
    except Exception as e:
        logger.log_event("keyvault_error", {"error": str(e)})
        snowflake_config = None
    
    # Inicializar extrator com config
    extractor = APIExtractor(config)
    
    # Extrair múltiplas páginas de dados
    logger.log_event("fetching_crypto_markets", {"pages": 3, "per_page": 100})
    
    all_data = extractor.extract_multiple_pages(num_pages=3, per_page=100)
    
    # Adicionar metadados
    extraction_metadata = {
        "extraction_timestamp": datetime.now().isoformat(),
        "run_id": run_id,
        "source": "coingecko_api_v3",
        "record_count": len(all_data),
        "snowflake_available": snowflake_config is not None
    }
    
    # Criar DataFrame Spark a partir dos dados
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
    
    # Converter para DataFrame
    df = spark.createDataFrame(all_data)
    
    # Salvar como tabela temporária para uso nos notebooks seguintes
    df.createOrReplaceTempView("crypto_data_raw")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.log_event("extraction_completed", {
        "records": len(all_data),
        "duration_seconds": duration
    })
    
    # Exibir resultado
    print(f"✅ Extração completa: {len(all_data)} registros")
    print(f"⏱️  Duração: {duration:.2f}s")
    print(f"\n📊 DataFrame salvo como: crypto_data_raw")
    
    df.display()
    
    # Retornar resultado
    result = {
        "status": "success",
        "record_count": len(all_data),
        "duration_seconds": duration
    }
    
except Exception as e:
    logger.log_event("extraction_error", {"error": str(e)}, level="ERROR")
    result = {
        "status": "failed",
        "error": str(e)
    }
    raise

# COMMAND ----------

# Retornar resultado
dbutils.notebook.exit(json.dumps(result))
