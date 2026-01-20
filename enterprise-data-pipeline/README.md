# 🚀 Enterprise Data Pipeline: CoinGecko API → Databricks → Snowflake

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5+-orange.svg)
![Databricks](https://img.shields.io/badge/Databricks-Community-red.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud_DW-blue.svg)
![Azure](https://img.shields.io/badge/Azure-Key_Vault-blue.svg)
![Security](https://img.shields.io/badge/Security-Enterprise_Grade-green.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

## 📋 Overview

**Enterprise-grade** data pipeline that extracts cryptocurrency data from CoinGecko API, processes with PySpark on Databricks Community Edition, and loads into Snowflake following the **Medallion Architecture** (Bronze → Silver → Gold).

🎯 **Extracts** data from CoinGecko API  
⚙️ **Processes** with PySpark on Databricks (Bronze → Silver → Gold)  
📊 **Stores** in Snowflake with optimized layers  
🔐 **Secure** with Azure Key Vault for credentials  

### 🌟 Technical Highlights

- ✅ **Medallion Architecture**: Bronze (raw), Silver (cleaned), Gold (aggregated)
- ✅ **Azure Key Vault**: Secure credential management with Service Principal
- ✅ **Type 2 SCD**: Complete change history in Silver layer
- ✅ **Snowflake VARIANT**: Flexible JSON storage in Bronze
- ✅ **Modularity**: Organized code (extractors/transformers/utils)
- ✅ **Structured Logging**: Complete execution tracking
- ✅ **Databricks Community**: Optimized for Free Tier

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│                     CoinGecko API v3                             │
│              (Cryptocurrency Market Data)                        │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   📥 EXTRACTION                                  │
│                 (notebooks/01_extraction.py)                     │
│         • Rate limiting  • Retry logic  • Azure Key Vault       │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│             🥉 BRONZE LAYER (Snowflake)                         │
│          BRONZE.BRONZE_CRYPTO_RAW (VARIANT column)              │
│              Raw JSON, immutable, timestamped                   │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                  ⚙️  TRANSFORMATION                              │
│              (notebooks/02_transformation.py)                    │
│    • PySpark processing  • Quality checks  • Aggregations      │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ├─────────────────┐
                         ▼                 ▼
┌──────────────────────────────┐  ┌─────────────────────────────┐
│   🥈 SILVER LAYER           │  │   🥇 GOLD LAYER            │
│   (Snowflake)                │  │   (Snowflake)              │
│                              │  │                            │
│ SILVER.silver_crypto_clean   │  │ GOLD.gold_crypto_metrics   │
│ • Cleaned & validated        │  │ • Aggregated by category   │
│ • Type 2 SCD (is_current)    │  │ • Ready for BI             │
│ • Quality flags              │  │ • Optimized for analytics  │
└──────────────────────────────┘  └─────────────────────────────┘
```

### 🔑 Layers (Medallion Architecture)

- **🥉 Bronze**: Raw JSON from API → `BRONZE.BRONZE_CRYPTO_RAW` (VARIANT)
- **🥈 Silver**: Clean and validated data → `SILVER.silver_crypto_clean` (Type 2 SCD)
- **🥇 Gold**: Aggregated metrics → `GOLD.gold_crypto_metrics` (KPIs by category)

---

## 📁 Project Structure

```
enterprise-data-pipeline/
├── 📓 notebooks/                    # Databricks notebooks
│   ├── 00_test_keyvault.py         # → Azure Key Vault test
│   ├── 01_extraction.py            # → API extraction → Snowflake Bronze
│   ├── 02_transformation.py        # → Bronze → Silver → Gold
│   └── 03_loading.py               # → Validation and metadata
│
├── 🐍 src/                          # Python modules
│   ├── extractors/
│   │   └── coingecko_extractor.py  # → APIExtractor class
│   ├── transformers/
│   │   └── spark_processor.py      # → SparkProcessor class
│   ├── loaders/
│   │   └── snowflake_loader.py     # → SnowflakeLoader class
│   └── utils/
│       ├── logging_config.py       # → Structured logging
│       └── config_loader.py        # → Azure Key Vault integration
│
├── 🗄️  sql/
│   ├── bronze_schema_setup.sql     # → Bronze layer tables/views
│   └── snowflake_models.sql        # → Silver/Gold tables/views
│
├── ⚙️  config/
│   └── config.yaml                 # → Pipeline configuration
│
├── requirements.txt                 # → Python dependencies
└── README.md                        # → This file
```

---

## 🚀 Quick Start

### 1. Prerequisites

- **Python 3.9+**
- **Databricks Account** (Community Edition)
- **Snowflake Account** (Free trial)
- **Azure Account** (for Key Vault)
- **CoinGecko API** (free)

### 2. Clone and Install

```bash
git clone <repository-url>
cd enterprise-data-pipeline
pip install -r requirements.txt
```

### 3. Configure Azure Key Vault

#### 3.1 Create Key Vault

```bash
az keyvault create \
  --name kv-crypto-pipeline \
  --resource-group rg-data-engineer \
  --location eastus
```

#### 3.2 Create Service Principal

```bash
az ad sp create-for-rbac --name sp-databricks-crypto
# Copy: appId, password, tenant
```

#### 3.3 Add Permissions

```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee <appId> \
  --scope /subscriptions/<sub-id>/resourceGroups/rg-data-engineer/providers/Microsoft.KeyVault/vaults/kv-crypto-pipeline
```

#### 3.4 Add Secrets to Key Vault

**Snowflake credentials:**
```bash
az keyvault secret set --vault-name kv-crypto-pipeline --name snowflake-account --value "your-account"
az keyvault secret set --vault-name kv-crypto-pipeline --name snowflake-user --value "your-username"
az keyvault secret set --vault-name kv-crypto-pipeline --name snowflake-password --value "your-password"
```

**Service Principal credentials:**
```bash
az keyvault secret set --vault-name kv-crypto-pipeline --name azure-tenant-id --value "<your-tenant-id>"
az keyvault secret set --vault-name kv-crypto-pipeline --name azure-client-id --value "<your-client-id>"
az keyvault secret set --vault-name kv-crypto-pipeline --name azure-client-secret --value "<your-client-secret>"
```

### 4. Snowflake Setup

```sql
-- Execute bronze_schema_setup.sql
-- Execute snowflake_models.sql
```

### 5. Configure Credentials

Copy the example credentials file and fill with your values:

```bash
cd enterprise-data-pipeline/config
cp credentials.yaml.example credentials.yaml
# Edit credentials.yaml with your Azure Service Principal values
```

**credentials.yaml:**
```yaml
azure:
  tenant_id: "your-tenant-id"
  client_id: "your-client-id"
  client_secret: "your-client-secret"

key_vault:
  name: "kv-crypto-pipeline"
  url: "https://kv-crypto-pipeline.vault.azure.net/"
```

⚠️ **Important:** This file is in `.gitignore` and will NOT be committed to Git.

### 6. Deploy to Databricks

1. **Upload config folder** including `credentials.yaml` to `/Workspace/Users/<your-email>/data-engineer-portfolio/enterprise-data-pipeline/config/`
2. **Upload notebooks** from `notebooks/` folder to Databricks Workspace
3. **Upload modules** from `src/` folder to `/Workspace/Users/<your-email>/data-engineer-portfolio/enterprise-data-pipeline/src`
4. **Execute notebooks in order:**
   - `00_test_keyvault.py` (test Key Vault connection)
   - `01_extraction.py` (extract data from API → Snowflake Bronze)
   - `02_transformation.py` (transform Bronze → Silver → Gold)
   - `03_loading.py` (validation and metadata)

🔐 **Security:** Credentials are stored in `config/credentials.yaml` (not committed to Git)

---

## 📊 Data Structure

### Bronze Layer

**Table:** `BRONZE.BRONZE_CRYPTO_RAW`

- `id` (STRING): UUID
- `payload` (VARIANT): Complete JSON from API
- `extracted_at` (TIMESTAMP): Extraction timestamp
- `run_id` (STRING): Execution ID
- `processed` (BOOLEAN): Processing flag

### Silver Layer

**Table:** `SILVER.silver_crypto_clean`

- `coin_id`, `symbol`, `name`
- `current_price`, `market_cap`, `total_volume`
- `is_current` (BOOLEAN): Current record
- `valid_from`, `valid_to` (TIMESTAMP): SCD Type 2

### Gold Layer

**Table:** `GOLD.gold_crypto_metrics`

- `metric_date` (DATE)
- `market_cap_category` (STRING): LARGE/MID/SMALL_CAP
- `num_coins`, `total_market_cap`, `avg_market_cap`
- Aggregated metrics by category

---

## 🎮 Usage

### Manual Execution

```bash
# Databricks: execute notebooks in order
1. 01_extraction.py
2. 02_transformation.py
3. 03_loading.py
```

### SQL Queries

```sql
-- View current data (Silver)
SELECT * FROM SILVER.v_current_market_state LIMIT 10;

-- View aggregated metrics (Gold)
SELECT * FROM GOLD.v_market_summary WHERE metric_date = CURRENT_DATE();

-- View Bronze raw
SELECT payload:symbol::STRING, payload:current_price::FLOAT 
FROM BRONZE.BRONZE_CRYPTO_RAW LIMIT 10;
```

---

## 🔧 Configuration

### Azure Key Vault

```python
# config_loader.py automatically detects:
os.environ['AZURE_TENANT_ID']
os.environ['AZURE_CLIENT_ID']
os.environ['AZURE_CLIENT_SECRET']

# Usage:
snowflake_config = get_snowflake_credentials_from_keyvault("kv-crypto-pipeline")
```

### Snowflake Connection

```python
conn = snowflake.connector.connect(
    account=snowflake_config['account'],
    user=snowflake_config['user'],
    password=snowflake_config['password'],
    warehouse='SNOWFLAKE_LEARNING_WH',
    database='CRYPTO_DB'
)
```

---

## 📈 Monitoring

### Quality Views

```sql
SELECT * FROM SILVER.v_data_quality_metrics;
SELECT * FROM SILVER.v_pipeline_execution_history;
SELECT * FROM BRONZE.v_bronze_extraction_stats;
```

### Stored Procedures

```sql
CALL BRONZE.sp_mark_records_processed('run_id');
CALL BRONZE.sp_archive_old_bronze_data(90);
CALL SILVER.sp_refresh_daily_summary();
```

---

## 📚 Technologies

| Category | Technology | Version |
|-----------|-----------|--------|
| Language | Python | 3.9+ |
| Processing | PySpark | 3.5+ |
| Compute | Databricks Community | - |
| Warehouse | Snowflake | Trial |
| Secrets | Azure Key Vault | - |
| API | CoinGecko | v3 |

---

## 🔒 Security

- ✅ Credentials in Azure Key Vault
- ✅ Service Principal for authentication
- ✅ RBAC
- ✅ Secrets not versioned
- ✅ HTTPS connections

---

## 📝 Dependencies

```txt
azure-identity==1.14.0
azure-keyvault-secrets==4.7.0
snowflake-connector-python[pandas]==3.6.0
requests==2.31.0
pyyaml==6.0
tenacity==8.2.3
```

---

## 👤 Author

**Eric M.**  
Data Engineer Portfolio Project

---

## 📄 License

MIT License

---

⭐ **If this project was useful, consider giving it a star!**
