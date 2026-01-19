# 🛠️ Setup Guide

Complete setup instructions for the Enterprise Data Pipeline.

## Prerequisites

### Required Software
- **Python**: 3.9 or higher
- **pip**: Latest version
- **Git**: For version control
- **Databricks account**: Community Edition or higher
- **Snowflake account**: Trial or paid account

### Accounts
1. **Databricks**: [Sign up](https://databricks.com/try-databricks)
2. **Snowflake**: [Sign up](https://signup.snowflake.com/)
3. **CoinGecko API**: Free tier (no signup needed)

## Installation Steps

### 1. Clone Repository

```bash
git clone https://github.com/your-username/enterprise-data-pipeline.git
cd enterprise-data-pipeline
```

### 2. Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file:

```bash
cp config/.env.example .env
```

Edit `.env` with your credentials:

```env
# Database Configuration
DATABASE_TYPE=snowflake

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=your_account_id
SNOWFLAKE_USER=your_username
SNOWFLAKE_AUTHENTICATOR=externalbrowser
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=CRYPTO_DATA_PROD
SNOWFLAKE_SCHEMA=PUBLIC

# Environment
ENVIRONMENT=development  # or staging, production
```

### 5. Snowflake Setup

#### Create Database Objects

```sql
-- Connect to Snowflake and run:
CREATE DATABASE IF NOT EXISTS CRYPTO_DATA_PROD;
CREATE SCHEMA IF NOT EXISTS CRYPTO_DATA_PROD.PUBLIC;
USE CRYPTO_DATA_PROD.PUBLIC;

-- Run the setup script
-- Copy content from sql/snowflake_models.sql and execute
```

Detailed instructions: [Snowflake Setup](SNOWFLAKE_SETUP.md)

### 6. Databricks Setup

#### Upload Code to Databricks

1. **Create Repo in Databricks:**
   - Workspace → Repos → Add Repo
   - Connect to GitHub repository
   - Clone the repo

2. **Upload Notebooks:**
   - Navigate to Repos → your-repo → notebooks/
   - Notebooks should be automatically available

3. **Create Secret Scope:**

```python
# In Databricks notebook:
dbutils.secrets.createScope(scope="snowflake")

# Add secrets:
dbutils.secrets.put(scope="snowflake", key="account", string_value="your_account")
dbutils.secrets.put(scope="snowflake", key="user", string_value="your_user")
dbutils.secrets.put(scope="snowflake", key="password", string_value="your_password")
dbutils.secrets.put(scope="snowflake", key="warehouse", string_value="COMPUTE_WH")
dbutils.secrets.put(scope="snowflake", key="database", string_value="CRYPTO_DATA_PROD")
dbutils.secrets.put(scope="snowflake", key="schema", string_value="PUBLIC")
```

Detailed instructions: [Databricks Guide](DATABRICKS_GUIDE.md)

### 7. Test Connection

#### Test Snowflake Connection

```bash
python test_snowflake_connection.py
```

Expected output:
```
✅ Connected to Snowflake successfully
✅ Warehouse TEST_WH is active
✅ Database CRYPTO_DATA_PROD exists
✅ Snowflake version: X.XX.X
```

#### Test API Extraction

```bash
# In Python:
python -c "from src.extractors.coingecko_extractor import CryptoExtractor; \
    from src.utils.config_loader import load_config; \
    config = load_config(); \
    extractor = CryptoExtractor(config); \
    data = extractor.get_crypto_data('bitcoin'); \
    print('✅ API working!' if data else '❌ API failed')"
```

## Configuration

### Environment-Specific Configs

The pipeline supports multiple environments:

- **Development**: `config/environments/development.yaml`
- **Staging**: `config/environments/staging.yaml`
- **Production**: `config/environments/production.yaml`

Set the environment via:

```bash
export ENVIRONMENT=production  # Linux/Mac
set ENVIRONMENT=production     # Windows
```

### Base Configuration

Edit `config/config.yaml` for base settings:

```yaml
api:
  coingecko:
    base_url: "https://api.coingecko.com/api/v3"
    timeout: 30
    retry_attempts: 3
    rate_limit_per_minute: 50

databricks:
  spark:
    app_name: "CryptoDataPipeline"
    shuffle_partitions: 200
```

## Verification

### Run Unit Tests

```bash
pytest tests/unit/ -v
```

Expected output:
```
tests/unit/test_extractors.py::TestCryptoExtractor::test_initialization PASSED
tests/unit/test_transformers.py::TestSparkProcessor::test_bronze_to_silver PASSED
tests/unit/test_loaders.py::TestSnowflakeLoader::test_connection PASSED
...
============= X passed in Y.YYs =============
```

### Run Full Pipeline (Local)

```bash
python -m src.pipeline_orchestrator
```

Note: This will run locally. For Databricks execution, use notebooks.

## Databricks Job Setup

### Create Job

1. **Workspace → Workflows → Create Job**

2. **Job Configuration:**
   - **Name**: `Crypto Data Pipeline`
   - **Task Type**: Notebook
   - **Notebook Path**: `/Repos/your-username/enterprise-data-pipeline/notebooks/00_orchestrator`
   - **Cluster**: Select or create cluster
   - **Schedule**: Cron expression (e.g., `0 */6 * * *` for every 6 hours)

3. **Cluster Configuration:**
   - **Databricks Runtime**: 13.3 LTS or higher
   - **Node Type**: Standard_DS3_v2 (or equivalent)
   - **Workers**: 2-4 (adjust based on data volume)

4. **Advanced Options:**
   - **Max Retries**: 2
   - **Timeout**: 3600 seconds
   - **Email Notifications**: Add your email

### Test Job

Click "Run Now" to test the job manually.

Monitor execution in the "Runs" tab.

## Troubleshooting

### Common Issues

#### 1. ModuleNotFoundError

**Problem**: Python can't find modules

**Solution**:
```bash
# Ensure you're in the project root
cd enterprise-data-pipeline

# Reinstall dependencies
pip install -r requirements.txt
```

#### 2. Snowflake Connection Failed

**Problem**: Can't connect to Snowflake

**Solution**:
- Verify credentials in `.env`
- Check account identifier format: `account_name.region`
- Ensure externalbrowser authentication works
- Try connection test: `python test_snowflake_connection.py`

#### 3. DBFS Access Denied

**Problem**: Can't write to DBFS

**Solution**:
- Check Databricks permissions
- Verify path format: `dbfs:/mnt/data/...`
- Ensure cluster has access to mount point

#### 4. API Rate Limiting

**Problem**: Too many API requests

**Solution**:
- Check `rate_limit_per_minute` in config
- Reduce number of cryptocurrencies
- Add delays between calls

### Getting Help

- Check [Troubleshooting Guide](TROUBLESHOOTING.md)
- Review [Databricks Guide](DATABRICKS_GUIDE.md)
- Check logs in `logs/` directory

## Next Steps

1. ✅ Complete setup
2. 📖 Read [Architecture Overview](ARCHITECTURE.md)
3. 🚀 Follow [Databricks Guide](DATABRICKS_GUIDE.md)
4. 🧪 Review [Testing Guide](TESTING.md)
5. 🚀 Deploy to production

## Quick Reference

### Commands

```bash
# Activate virtual environment
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Run tests
pytest tests/ -v

# Run with specific environment
ENVIRONMENT=production python -m src.pipeline_orchestrator

# Check code quality
flake8 src/
black src/ --check
```

### File Locations

- **Source code**: `src/`
- **Notebooks**: `notebooks/`
- **Tests**: `tests/`
- **Config**: `config/`
- **SQL**: `sql/`
- **Logs**: `logs/`
- **Docs**: `docs/`

### Important URLs

- **Databricks Workspace**: Your workspace URL
- **Snowflake Console**: https://app.snowflake.com/
- **CoinGecko API**: https://www.coingecko.com/en/api
