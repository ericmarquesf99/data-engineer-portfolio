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

# MAGIC %pip install azure-identity azure-keyvault-secrets snowflake-connector-python[pandas] pyyaml tenacity requests

# COMMAND ----------

import sys
import os
from datetime import datetime
import json
import pandas as pd

# Adicionar src ao path
sys.path.append("/Workspace/Users/ericmarques1999@gmail.com/data-engineer-portfolio/enterprise-data-pipeline/src")

from extractors.coingecko_extractor import APIExtractor
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
    # Carregar configuração da API (config.yaml)
    config = load_config()  # Busca automaticamente em config/config.yaml
    
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
    
    # Converter dados para Pandas DataFrame para normalizar tipos
    pdf = pd.json_normalize(all_data)
    
    # Preencher NaN com None para melhor compatibilidade
    pdf = pdf.where(pd.notna(pdf), None)
    
    # Converter para Spark DataFrame (inferirá schema corretamente)
    df = spark.createDataFrame(pdf)
    
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
    
    # Carregar para Snowflake Bronze (raw JSON em VARIANT)
    try:
        if snowflake_config is not None:
            import snowflake.connector
            
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
            
            # Inserir registros na tabela BRONZE_CRYPTO_RAW
            inserted = 0
            for rec in all_data:
                payload_json = json.dumps(rec)
                extracted_at_val = rec.get('extracted_at', extraction_metadata['extraction_timestamp'])
                cur.execute(
                    """
                    INSERT INTO BRONZE_CRYPTO_RAW(payload, extracted_at, run_id, source_system) 
                    SELECT parse_json(%s), %s, %s, 'coingecko_api'
                    """,
                    (payload_json, extracted_at_val, run_id)
                )
                inserted += 1
            
            conn.commit()
            cur.close()
            conn.close()
            
            logger.log_event("bronze_load_completed", {"inserted": inserted, "schema": "BRONZE", "table": "BRONZE_CRYPTO_RAW"})
            print(f"\n❄️  Snowflake Bronze: {inserted} registros inseridos em BRONZE.BRONZE_CRYPTO_RAW")
        else:
            logger.log_event("bronze_load_skipped", {"reason": "no_snowflake_credentials"})
            print("\n⚠️  Snowflake Bronze não executado (credenciais não disponíveis)")
    except Exception as e:
        logger.log_event("bronze_load_error", {"error": str(e)}, level="ERROR")
        print(f"\n❌ Erro ao carregar Bronze no Snowflake: {e}")
    
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
