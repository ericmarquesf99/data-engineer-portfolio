# 🔧 Troubleshooting Guide

Common issues and solutions for the Enterprise Data Pipeline.

## Table of Contents

- [Setup Issues](#setup-issues)
- [Databricks Issues](#databricks-issues)
- [Snowflake Issues](#snowflake-issues)
- [Execution Issues](#execution-issues)
- [Testing Issues](#testing-issues)
- [Performance Issues](#performance-issues)

---

## Setup Issues

### Issue 1: ModuleNotFoundError

**Symptom:**
```
ModuleNotFoundError: No module named 'src'
```

**Causes:**
- Wrong working directory
- Virtual environment not activated
- Package not installed

**Solutions:**

```bash
# 1. Ensure you're in project root
cd enterprise-data-pipeline
pwd  # Should show .../enterprise-data-pipeline

# 2. Activate virtual environment
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# 3. Reinstall dependencies
pip install -r requirements.txt

# 4. Add to PYTHONPATH (if needed)
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"  # Linux/Mac
set PYTHONPATH=%PYTHONPATH%;%cd%\src          # Windows
```

### Issue 2: ImportError for src modules

**Symptom:**
```python
from src.extractors.coingecko_extractor import CryptoExtractor
ImportError: cannot import name 'CryptoExtractor'
```

**Solutions:**

```python
# Check if __init__.py files exist
ls src/__init__.py
ls src/extractors/__init__.py

# If missing, create them
touch src/__init__.py src/extractors/__init__.py  # Linux/Mac
New-Item -ItemType File src\__init__.py           # Windows

# Verify file was moved correctly
ls src/extractors/coingecko_extractor.py
```

### Issue 3: Configuration file not found

**Symptom:**
```
ConfigurationError: Config file not found
```

**Solutions:**

```bash
# Check if config exists
ls config/config.yaml

# Check if .env exists
ls .env

# Copy example if missing
cp config/.env.example .env

# Verify config path in code
python -c "from pathlib import Path; print(Path('config/config.yaml').absolute())"
```

---

## Databricks Issues

### Issue 1: Notebooks can't find modules

**Symptom:**
```
ModuleNotFoundError in Databricks notebook
```

**Solutions:**

```python
# In first cell of notebook, add:
import sys
sys.path.append("/Workspace/Repos/your-username/enterprise-data-pipeline/src")

# Verify path
print(sys.path)

# Or use dbutils
sys.path.insert(0, dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit('/', 1)[0] + '/../src')
```

### Issue 2: Secret scope not found

**Symptom:**
```
Secret 'snowflake' does not exist
```

**Solutions:**

```python
# List existing scopes
dbutils.secrets.listScopes()

# Create scope if missing
dbutils.secrets.createScope(scope="snowflake")

# Add secrets
dbutils.secrets.put(scope="snowflake", key="account", string_value="your_account")

# Verify
dbutils.secrets.list(scope="snowflake")
```

### Issue 3: DBFS access denied

**Symptom:**
```
PermissionError: Cannot write to dbfs:/mnt/data/
```

**Solutions:**

```python
# Check if path exists
dbutils.fs.ls("dbfs:/mnt/data/")

# Try alternative path
# Use /FileStore/ for testing
path = "dbfs:/FileStore/bronze/crypto/"

# Create directory
dbutils.fs.mkdirs(path)

# Verify permissions
dbutils.fs.ls(path.rsplit('/', 1)[0])
```

### Issue 4: Cluster library installation fails

**Symptom:**
```
Library installation failed: snowflake-connector-python
```

**Solutions:**

1. **Via Cluster UI:**
   - Compute → Your Cluster → Libraries → Install New
   - PyPI: `snowflake-connector-python[pandas]`

2. **Via Notebook:**
   ```python
   %pip install snowflake-connector-python[pandas]
   %pip install pyyaml tenacity
   
   # Restart Python
   dbutils.library.restartPython()
   ```

3. **Check compatibility:**
   - Use Databricks Runtime 13.3 LTS or higher
   - Python 3.9+

### Issue 5: Notebook paths incorrect

**Symptom:**
```
Notebook '/Repos/username/repo/notebooks/01_extraction' not found
```

**Solutions:**

```python
# Get current notebook path
current_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(f"Current path: {current_path}")

# Use correct absolute path
result = dbutils.notebook.run(
    "/Workspace/Repos/your-actual-username/enterprise-data-pipeline/notebooks/01_extraction",
    timeout_seconds=600
)

# Or use relative path from current location
result = dbutils.notebook.run("./01_extraction", timeout_seconds=600)
```

---

## Snowflake Issues

### Issue 1: Connection failed

**Symptom:**
```
snowflake.connector.errors.DatabaseError: Connection failed
```

**Solutions:**

```python
# 1. Check credentials
python test_snowflake_connection.py

# 2. Verify account identifier format
# Correct: ACCOUNT_NAME.REGION (e.g., EYZZSXW-IR02741)
# Not: https://... or just ACCOUNT_NAME

# 3. Check authenticator
# For SSO/Browser: authenticator='externalbrowser'
# For username/password: authenticator='snowflake'

# 4. Test with minimal config
import snowflake.connector

conn = snowflake.connector.connect(
    account='EYZZSXW-IR02741',
    user='ERICMARQUESF',
    authenticator='externalbrowser'
)
print("✅ Connected!")
conn.close()
```

### Issue 2: Table not found

**Symptom:**
```
snowflake.connector.errors.ProgrammingError: Table 'SILVER_CRYPTO_CLEAN' does not exist
```

**Solutions:**

```sql
-- 1. Check current database/schema
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- 2. Use correct database
USE DATABASE CRYPTO_DATA_PROD;
USE SCHEMA PUBLIC;

-- 3. List existing tables
SHOW TABLES;

-- 4. Create tables if missing
-- Run sql/snowflake_models.sql
```

### Issue 3: Merge operation fails

**Symptom:**
```
SQL compilation error: Merge statement failed
```

**Solutions:**

```sql
-- 1. Check staging table exists
SHOW TABLES LIKE '%STAGE%';

-- 2. Verify staging table has data
SELECT COUNT(*) FROM silver_crypto_clean_stage;

-- 3. Check column names match
DESC TABLE silver_crypto_clean_stage;
DESC TABLE silver_crypto_clean;

-- 4. Test merge manually
MERGE INTO silver_crypto_clean AS target
USING silver_crypto_clean_stage AS source
ON target.id = source.id AND target.is_current = TRUE
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

### Issue 4: Write_pandas fails

**Symptom:**
```
ProgrammingError: Table 'X' does not exist or not authorized
```

**Solutions:**

```python
# 1. Ensure table exists (create if not)
cursor.execute("""
CREATE TABLE IF NOT EXISTS silver_crypto_clean_stage (
    id VARCHAR,
    symbol VARCHAR,
    ...
)
""")

# 2. Check permissions
# User must have CREATE TABLE and INSERT privileges

# 3. Use correct database/schema
conn = snowflake.connector.connect(
    account='...',
    user='...',
    warehouse='COMPUTE_WH',
    database='CRYPTO_DATA_PROD',  # Must specify
    schema='PUBLIC'                # Must specify
)

# 4. Try pandas_tools manually
from snowflake.connector.pandas_tools import write_pandas

success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=df,
    table_name='SILVER_CRYPTO_CLEAN_STAGE',
    auto_create_table=True
)
```

---

## Execution Issues

### Issue 1: API rate limiting

**Symptom:**
```
HTTP 429: Too Many Requests
```

**Solutions:**

```yaml
# In config/config.yaml, reduce rate:
api:
  coingecko:
    rate_limit_per_minute: 30  # Reduce from 50

# Or in code:
import time
for crypto_id in crypto_ids:
    data = extractor.get_crypto_data(crypto_id)
    time.sleep(2)  # Add delay
```

### Issue 2: Timeout errors

**Symptom:**
```
TimeoutError: API request timed out
```

**Solutions:**

```yaml
# Increase timeout in config:
api:
  coingecko:
    timeout: 60  # Increase from 30
    retry_attempts: 5  # More retries
```

### Issue 3: Out of memory

**Symptom:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Solutions:**

```python
# 1. Increase Spark memory
spark = SparkSession.builder \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .getOrCreate()

# 2. Process in smaller batches
crypto_ids = ['bitcoin', 'ethereum', ...]
batch_size = 10
for i in range(0, len(crypto_ids), batch_size):
    batch = crypto_ids[i:i+batch_size]
    process_batch(batch)

# 3. Increase cluster size in Databricks
# Compute → Cluster → Edit → Add more workers
```

### Issue 4: Data quality failures

**Symptom:**
```
ValidationError: Data quality check failed
```

**Solutions:**

```python
# 1. Check validation results
validation = processor.validate_data_quality(df)
print(json.dumps(validation, indent=2))

# 2. Adjust thresholds
# In config/environments/production.yaml:
data_quality:
  validation_rules:
    max_null_percentage: 5  # Increase from 1

# 3. Handle missing data
df = df.fillna({
    'price_change_24h': 0,
    'market_cap': 0
})

# 4. Skip problematic records
df = df.filter(F.col('current_price').isNotNull())
```

---

## Testing Issues

### Issue 1: Tests fail with import errors

**Symptom:**
```
ImportError during test execution
```

**Solutions:**

```bash
# 1. Run from project root
cd enterprise-data-pipeline
pytest tests/

# 2. Add src to Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# 3. Install test dependencies
pip install pytest pytest-cov pytest-mock

# 4. Check tests/__init__.py exists
cat tests/__init__.py
```

### Issue 2: Spark tests fail

**Symptom:**
```
java.io.IOException: Failed to connect to /0.0.0.0:0
```

**Solutions:**

```python
# In conftest.py or test file:
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.driver.host", "127.0.0.1") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()
```

### Issue 3: Mocks not working

**Symptom:**
```
Mock object not being called
```

**Solutions:**

```python
# Ensure correct patch path
# ❌ Wrong
@patch('requests.get')

# ✅ Correct
@patch('src.extractors.coingecko_extractor.requests.Session.get')

# Check where module is imported from
import sys
print(sys.modules['src.extractors.coingecko_extractor'].__file__)
```

---

## Performance Issues

### Issue 1: Slow Spark jobs

**Symptom:**
- Jobs taking too long
- Shuffle operations bottleneck

**Solutions:**

```python
# 1. Optimize partitions
spark.conf.set("spark.sql.shuffle.partitions", "200")

# 2. Cache frequently used DataFrames
df = df.cache()
df.count()  # Trigger cache

# 3. Use broadcast for small tables
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_df), "key")

# 4. Avoid collect() on large datasets
# ❌ Bad
data = df.collect()  # Loads all data to driver

# ✅ Good
df.write.parquet("path")  # Distributed write
```

### Issue 2: Slow Snowflake queries

**Symptom:**
- Long query execution times

**Solutions:**

```sql
-- 1. Use clustering keys
ALTER TABLE silver_crypto_clean CLUSTER BY (id, last_updated);

-- 2. Create indexes (if applicable)
CREATE INDEX idx_symbol ON silver_crypto_clean(symbol);

-- 3. Use query acceleration
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

-- 4. Optimize warehouse size
ALTER WAREHOUSE COMPUTE_WH SET WAREHOUSE_SIZE = 'LARGE';

-- 5. Use materialized views
CREATE MATERIALIZED VIEW mv_crypto_summary AS
SELECT coin_id, AVG(current_price) as avg_price
FROM silver_crypto_clean
WHERE is_current = TRUE
GROUP BY coin_id;
```

### Issue 3: High Databricks costs

**Symptom:**
- Unexpectedly high billing

**Solutions:**

```python
# 1. Auto-terminate idle clusters
# Compute → Cluster → Advanced → Auto Termination: 30 minutes

# 2. Use job clusters instead of interactive
# Workflows → Job → New Job → Job Cluster

# 3. Right-size clusters
# Start small: 2 workers (Standard_DS3_v2)
# Scale up only if needed

# 4. Use spot instances (if available)
# Cluster → Advanced → Spot Instances: ON

# 5. Schedule jobs during off-peak
# Run jobs at night/weekends when rates are lower
```

---

## Getting Help

### Check Logs

```python
# Databricks
# Jobs → Your Job → Run → View Output → stderr/stdout

# Local logs
tail -f logs/pipeline_prod.json

# Structured logs query
cat logs/pipeline_prod.json | jq '.event'
```

### Enable Debug Logging

```python
# In notebook
import logging
logging.basicConfig(level=logging.DEBUG)

# In configuration
logging:
  level: "DEBUG"
```

### Contact Support

1. **Databricks**: Support portal or community forum
2. **Snowflake**: Support ticket or community
3. **Project Issues**: GitHub issues (if applicable)

### Useful Commands

```bash
# Check Python version
python --version

# Check installed packages
pip list | grep -i snowflake

# Verify network connectivity
ping api.coingecko.com

# Test Snowflake connection
python test_snowflake_connection.py

# Run single test
pytest tests/unit/test_extractors.py::test_name -v -s

# Check Spark version
python -c "import pyspark; print(pyspark.__version__)"
```

---

## Prevention

### Best Practices

1. **Always test locally first**
2. **Use development environment** for testing
3. **Monitor logs** during execution
4. **Set up alerts** for failures
5. **Document custom changes**
6. **Keep dependencies updated** (but test first)
7. **Use version control** for all changes
8. **Backup configurations** before modifying

### Health Checks

```bash
# Daily checks
pytest tests/ -v  # Run tests
python test_snowflake_connection.py  # Verify Snowflake
databricks jobs list  # Check job status

# Weekly checks
pip list --outdated  # Check for updates
git status  # Verify no uncommitted changes
```
