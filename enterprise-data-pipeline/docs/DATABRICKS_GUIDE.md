# 📓 Databricks Guide

Complete guide for deploying and running the pipeline in Databricks.

## Prerequisites

- Databricks account (Community, Standard, or Premium)
- GitHub repository with your code
- Snowflake credentials

## Setup

### 1. Create Databricks Workspace

If you don't have one:
1. Go to [Databricks](https://databricks.com/try-databricks)
2. Sign up for Community Edition (free) or trial
3. Create a workspace

### 2. Connect GitHub Repository

#### Option A: Databricks Repos (Recommended)

1. **Navigate to Repos:**
   - Sidebar → Workspace → Repos

2. **Add Repo:**
   - Click "Add Repo"
   - Enter GitHub URL: `https://github.com/your-username/enterprise-data-pipeline`
   - Branch: `main`
   - Click "Create Repo"

3. **Verify:**
   - You should see all files including `notebooks/`, `src/`, `config/`

#### Option B: Manual Upload

1. **Upload notebooks individually:**
   - Workspace → Create → Notebook
   - Import → Upload each .py file from `notebooks/`

2. **Upload source code:**
   - Create folder: `src/`
   - Upload all modules

### 3. Create Cluster

1. **Compute → Create Cluster**

2. **Configuration:**
   ```
   Name: crypto-pipeline-cluster
   Mode: Standard
   Databricks Runtime: 13.3 LTS (Scala 2.12, Spark 3.4.1)
   Node Type: 
     - Standard_DS3_v2 (14GB Memory, 4 Cores)
     - Or smallest available for testing
   Workers: 
     - Min: 2
     - Max: 4 (auto-scaling)
   ```

3. **Advanced Options:**
   ```
   Spark Config:
   spark.sql.adaptive.enabled true
   spark.sql.adaptive.coalescePartitions.enabled true
   
   Environment Variables:
   ENVIRONMENT production
   ```

4. **Libraries** (Install these):
   - PyPI: `snowflake-connector-python[pandas]`
   - PyPI: `pyyaml`
   - PyPI: `tenacity`
   - PyPI: `python-dotenv`

5. **Click "Create Cluster"**

### 4. Configure Secrets

Secrets are used to store sensitive credentials.

#### Create Secret Scope

Run in a notebook cell:

```python
# Create scope (one-time setup)
dbutils.secrets.createScope(scope="snowflake")
```

Or use Databricks CLI:

```bash
databricks secrets create-scope --scope snowflake
```

#### Add Secrets

Run in notebook:

```python
# Add each secret
dbutils.secrets.put(scope="snowflake", key="account", string_value="EYZZSXW-IR02741")
dbutils.secrets.put(scope="snowflake", key="user", string_value="ERICMARQUESF")
dbutils.secrets.put(scope="snowflake", key="password", string_value="your_password")
dbutils.secrets.put(scope="snowflake", key="warehouse", string_value="COMPUTE_WH")
dbutils.secrets.put(scope="snowflake", key="database", string_value="CRYPTO_DATA_PROD")
dbutils.secrets.put(scope="snowflake", key="schema", string_value="PUBLIC")
```

Or use CLI:

```bash
databricks secrets put --scope snowflake --key account --string-value "EYZZSXW-IR02741"
databricks secrets put --scope snowflake --key user --string-value "ERICMARQUESF"
# ... etc
```

#### Verify Secrets

```python
# List secrets (values are hidden)
dbutils.secrets.list(scope="snowflake")

# Get secret (for testing)
account = dbutils.secrets.get(scope="snowflake", key="account")
print(f"Account starts with: {account[:3]}...")  # Only show first 3 chars
```

### 5. Update Notebook Paths

Edit notebooks to use your actual paths:

In `notebooks/00_orchestrator.py`, line 20:
```python
# Change:
sys.path.append("/Workspace/Repos/<username>/enterprise-data-pipeline/src")

# To:
sys.path.append("/Workspace/Repos/your-actual-username/enterprise-data-pipeline/src")
```

Repeat for all 4 notebooks.

### 6. Test Notebooks

#### Test 01_extraction.py

1. Open `notebooks/01_extraction.py`
2. Attach to your cluster
3. Set parameters:
   ```python
   dbutils.widgets.text("run_id", "test_001", "Run ID")
   dbutils.widgets.text("output_path", "dbfs:/mnt/data/bronze/crypto/", "Output Path")
   ```
4. Run all cells
5. Verify output in DBFS

#### Test 02_transformation.py

1. Open `notebooks/02_transformation.py`
2. Set parameters:
   ```python
   dbutils.widgets.text("run_id", "test_001", "Run ID")
   dbutils.widgets.text("input_path", "dbfs:/mnt/data/bronze/crypto/crypto_data_XXXXXX.json", "Input Path")
   ```
3. Run all cells
4. Check temp views were created

#### Test 03_loading.py

1. Open `notebooks/03_loading.py`
2. Set Snowflake parameters (or use secrets)
3. Run all cells
4. Verify data in Snowflake

#### Test 00_orchestrator.py (Full Pipeline)

1. Open `notebooks/00_orchestrator.py`
2. Run all cells
3. Should execute all 3 notebooks in sequence

## Create Databricks Job

### 1. Navigate to Workflows

Sidebar → Workflows → Create Job

### 2. Configure Job

**Basic Settings:**
```
Job Name: Crypto Data Pipeline
Task Name: Orchestrate Pipeline
Type: Notebook
```

**Notebook Settings:**
```
Source: Workspace
Path: /Repos/your-username/enterprise-data-pipeline/notebooks/00_orchestrator
```

**Cluster Settings:**
```
Choose existing cluster: crypto-pipeline-cluster
OR
Create new job cluster (recommended for production)
```

**Parameters:**
```
run_id: {{ run_id }} (automatically generated)
```

### 3. Schedule Job

**Schedule Tab:**
```
Schedule Type: Scheduled
Schedule: Cron expression
  - Every 6 hours: 0 */6 * * *
  - Every day at 8am: 0 8 * * *
  - Every hour: 0 * * * *

Timezone: Your timezone
```

**Advanced Scheduling:**
```
Pause Status: Active
Max Concurrent Runs: 1
```

### 4. Alerts & Monitoring

**Email Notifications:**
```
On Start: ✓ (optional)
On Success: ✓
On Failure: ✓
Email: your-email@example.com
```

**Webhook** (optional):
```
URL: Your webhook endpoint
Events: on_success, on_failure
```

### 5. Advanced Configuration

**Timeout:**
```
Timeout: 3600 seconds (1 hour)
```

**Retries:**
```
Max Retries: 2
Retry Interval: 300 seconds (5 minutes)
```

**Dependencies:**
```
Libraries: (should be installed on cluster)
  - snowflake-connector-python[pandas]
  - pyyaml
  - tenacity
```

### 6. Save and Test

1. Click "Create"
2. Click "Run Now" to test
3. Monitor in "Runs" tab

## Monitoring

### View Job Runs

Workflows → Your Job → Runs tab

**For each run you can see:**
- Start/end time
- Duration
- Status (Running, Succeeded, Failed)
- Logs
- Output

### View Logs

Click on a run → View notebook output

**Log Types:**
- stdout: Print statements
- stderr: Errors
- Structured logs: JSON logs from StructuredLogger

### Metrics

In orchestrator notebook output:
```json
{
  "status": "success",
  "run_id": "run_20260119_120000",
  "extraction": {"file": "...", "records": 10},
  "transformation": {"silver": 10, "gold": 10},
  "loading": {"records": 20}
}
```

### Alerts

Configure in Job settings:
- Email on failure
- Slack webhook
- PagerDuty integration

## DBFS (Databricks File System)

### Browse DBFS

Data → Add Data → DBFS

### Common Paths

```
dbfs:/mnt/data/bronze/crypto/  # Bronze layer
dbfs:/FileStore/                # User files
dbfs:/tmp/                      # Temporary files
```

### Access DBFS

In notebook:
```python
# List files
dbutils.fs.ls("dbfs:/mnt/data/bronze/crypto/")

# Read file
df = spark.read.json("dbfs:/mnt/data/bronze/crypto/crypto_data_20260119.json")

# Write file
dbutils.fs.put("dbfs:/path/to/file.txt", "content")

# Delete file
dbutils.fs.rm("dbfs:/path/to/file.txt")
```

## Troubleshooting

### Issue 1: ModuleNotFoundError

**Problem**: Can't import modules

**Solution**:
```python
import sys
sys.path.append("/Workspace/Repos/your-username/enterprise-data-pipeline/src")
```

### Issue 2: Secrets Not Found

**Problem**: `Secret 'snowflake' does not exist`

**Solution**:
- Create secret scope first
- Verify scope name matches in notebooks
- Check permissions

### Issue 3: DBFS Permission Denied

**Problem**: Can't write to DBFS

**Solution**:
- Use `dbfs:/FileStore/` for testing
- Check workspace permissions
- Verify path format

### Issue 4: Cluster Libraries

**Problem**: Missing dependencies

**Solution**:
```python
# Install in notebook (temporary)
%pip install snowflake-connector-python[pandas]

# Or add to cluster configuration (permanent)
```

### Issue 5: Notebook Path Issues

**Problem**: Can't find notebooks for `dbutils.notebook.run()`

**Solution**:
- Use absolute paths: `/Repos/username/repo/notebooks/01_extraction`
- No `.py` extension when calling
- Verify path in workspace

## Best Practices

### 1. Use Repos

Always use Databricks Repos instead of uploading files manually.

**Benefits:**
- Version control
- Easy updates
- Team collaboration

### 2. Use Secrets

Never hardcode credentials.

```python
# ❌ Bad
snowflake_user = "ERICMARQUESF"

# ✅ Good
snowflake_user = dbutils.secrets.get("snowflake", "user")
```

### 3. Use Job Clusters

For production, use job clusters (ephemeral clusters).

**Benefits:**
- Cost-effective
- Isolated environments
- Auto-terminated

### 4. Monitor Costs

Check Databricks billing dashboard regularly.

**Cost Optimization:**
- Use auto-scaling
- Terminate idle clusters
- Use spot instances
- Schedule jobs during off-peak

### 5. Logging

Always use structured logging:

```python
from src.utils.logging_config import StructuredLogger

logger = StructuredLogger("notebook_name")
logger.log_event("event_name", {"key": "value"})
```

## Next Steps

1. ✅ Setup complete
2. 📊 Monitor first job run
3. 🔍 Check Snowflake data
4. 📈 Create dashboards
5. 🚀 Scale as needed

## Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Spark API](https://spark.apache.org/docs/latest/api/python/)
- [DBFS Guide](https://docs.databricks.com/data/databricks-file-system.html)
- [Jobs Guide](https://docs.databricks.com/workflows/jobs/jobs.html)
