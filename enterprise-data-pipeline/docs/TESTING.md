# 🧪 Testing Guide

Comprehensive guide for testing the Enterprise Data Pipeline.

## Test Structure

```
tests/
├── __init__.py
├── unit/                      # Unit tests
│   ├── __init__.py
│   ├── test_extractors.py    # API extraction tests
│   ├── test_transformers.py  # PySpark transformation tests
│   ├── test_loaders.py       # Snowflake loader tests
│   └── test_utils.py         # Utility function tests
└── integration/               # Integration tests
    ├── __init__.py
    └── test_pipeline.py      # End-to-end tests
```

## Running Tests

### Run All Tests

```bash
pytest tests/ -v
```

### Run Unit Tests Only

```bash
pytest tests/unit/ -v
```

### Run Specific Test File

```bash
pytest tests/unit/test_extractors.py -v
```

### Run Specific Test Class

```bash
pytest tests/unit/test_extractors.py::TestCryptoExtractor -v
```

### Run Specific Test

```bash
pytest tests/unit/test_extractors.py::TestCryptoExtractor::test_initialization -v
```

### Run with Coverage

```bash
pytest tests/ --cov=src --cov-report=html
```

View coverage report:
```bash
# Open in browser
open htmlcov/index.html  # Mac
start htmlcov/index.html  # Windows
```

### Run with Markers

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Skip slow tests
pytest -m "not slow"
```

## Test Categories

### 1. Unit Tests

Test individual components in isolation with mocked dependencies.

#### Extractors (`test_extractors.py`)

**Tests:**
- API initialization
- Successful data retrieval
- Rate limiting
- Error handling
- Retry logic
- DBFS saving

**Example:**
```python
@patch('requests.Session.get')
def test_get_crypto_data_success(self, mock_get, extractor_config):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"id": "bitcoin"}
    mock_get.return_value = mock_response
    
    extractor = CryptoExtractor(extractor_config)
    result = extractor.get_crypto_data("bitcoin")
    
    assert result is not None
    assert result["id"] == "bitcoin"
```

#### Transformers (`test_transformers.py`)

**Tests:**
- Bronze to Silver transformation
- Data validation
- Null handling
- Anomaly detection
- Silver to Gold aggregation
- Deduplication

**Example:**
```python
def test_bronze_to_silver_transformation(self, spark, sample_data):
    processor = SparkProcessor(config)
    silver_df = processor.process_bronze_to_silver(sample_data)
    
    assert silver_df.count() == 2
    assert "id" in silver_df.columns
```

#### Loaders (`test_loaders.py`)

**Tests:**
- Snowflake connection
- Database setup
- Staging table loading
- Merge operations
- Metadata logging
- Error handling

**Example:**
```python
@patch('snowflake.connector.connect')
def test_connection_establishment(self, mock_connect):
    loader = SnowflakeLoader(config)
    loader.connect()
    
    mock_connect.assert_called_once()
    assert loader.conn is not None
```

#### Utils (`test_utils.py`)

**Tests:**
- Structured logging
- Config loading
- Data validation
- Schema validation
- Anomaly detection

### 2. Integration Tests

Test complete workflows with real components (but may use test databases).

**File:** `tests/integration/test_pipeline.py`

**Tests:**
- End-to-end pipeline execution
- Multi-phase orchestration
- Error recovery
- Data consistency

## Test Fixtures

### Common Fixtures

```python
@pytest.fixture
def sample_api_response():
    """Sample API response"""
    return {
        "id": "bitcoin",
        "symbol": "btc",
        "current_price": 45000.50
    }

@pytest.fixture(scope="module")
def spark():
    """Spark session for tests"""
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[2]") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def loader_config():
    """Snowflake loader config"""
    return {
        "snowflake": {
            "account": "test_account",
            "user": "test_user"
        }
    }
```

## Mocking

### Mock API Calls

```python
from unittest.mock import Mock, patch

@patch('requests.Session.get')
def test_api_call(mock_get):
    # Setup mock
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"data": "value"}
    mock_get.return_value = mock_response
    
    # Test code
    result = extractor.get_data()
    
    # Assertions
    assert result is not None
    mock_get.assert_called_once()
```

### Mock Snowflake Connection

```python
@patch('snowflake.connector.connect')
def test_database_operation(mock_connect):
    # Setup mock
    mock_conn = Mock()
    mock_cursor = Mock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn
    
    # Test code
    loader.connect()
    loader.execute_query()
    
    # Assertions
    mock_cursor.execute.assert_called()
```

### Mock DBFS Operations

```python
@patch('dbutils.fs.put')
def test_dbfs_save(mock_put):
    extractor.save_to_dbfs(data, "dbfs:/path")
    mock_put.assert_called_once()
```

## Test Data

### Sample Bronze Data

```python
bronze_data = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 45000.50,
        "market_cap": 850000000000
    }
]
```

### Sample Silver Data

```python
silver_data = pd.DataFrame([
    {
        "id": "bitcoin",
        "current_price": 45000.50,
        "is_current": True,
        "valid_from": datetime.now()
    }
])
```

### Sample Gold Data

```python
gold_data = pd.DataFrame([
    {
        "coin_id": "bitcoin",
        "avg_price_usd": 45000.50,
        "market_cap_usd": 850000000000
    }
])
```

## Continuous Integration

### GitHub Actions Workflow

Create `.github/workflows/tests.yml`:

```yaml
name: Tests

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    
    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -r requirements.txt
        pip install pytest pytest-cov
    
    - name: Run tests
      run: |
        pytest tests/unit/ -v --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage.xml
```

## Test Environment Setup

### 1. Install Test Dependencies

```bash
pip install pytest pytest-cov pytest-mock
```

### 2. Configure pytest

File: `pytest.ini` (already created)

```ini
[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
addopts = "-v --strict-markers --tb=short"
```

### 3. Environment Variables

Create `.env.test`:

```env
DATABASE_TYPE=mock
ENVIRONMENT=test
```

Load in tests:

```python
@pytest.fixture(autouse=True)
def load_test_env():
    from dotenv import load_dotenv
    load_dotenv('.env.test')
```

## Writing New Tests

### Test Template

```python
"""
Unit Tests for New Module
==========================
"""

import pytest
from unittest.mock import Mock, patch


@pytest.fixture
def sample_data():
    """Sample data for tests"""
    return {"key": "value"}


class TestNewModule:
    """Test suite for NewModule"""
    
    def test_initialization(self):
        """Test module initialization"""
        module = NewModule()
        assert module is not None
    
    def test_basic_functionality(self, sample_data):
        """Test basic functionality"""
        module = NewModule()
        result = module.process(sample_data)
        
        assert result is not None
        assert "expected_key" in result
    
    @patch('external.dependency')
    def test_with_mock(self, mock_dependency):
        """Test with mocked dependency"""
        mock_dependency.return_value = "mocked"
        
        module = NewModule()
        result = module.call_dependency()
        
        assert result == "mocked"
        mock_dependency.assert_called_once()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

## Best Practices

### 1. Arrange-Act-Assert Pattern

```python
def test_example():
    # Arrange: Setup test data
    data = {"id": "test"}
    
    # Act: Execute the function
    result = process_data(data)
    
    # Assert: Verify the result
    assert result["status"] == "success"
```

### 2. Test One Thing at a Time

```python
# ❌ Bad: Tests multiple things
def test_everything():
    assert module.init() == True
    assert module.process() == "success"
    assert module.validate() == True

# ✅ Good: Separate tests
def test_initialization():
    assert module.init() == True

def test_processing():
    assert module.process() == "success"

def test_validation():
    assert module.validate() == True
```

### 3. Use Descriptive Names

```python
# ❌ Bad
def test_1():
    ...

# ✅ Good
def test_extraction_handles_api_timeout_error():
    ...
```

### 4. Mock External Dependencies

```python
# Always mock:
- API calls (requests.get, requests.post)
- Database connections (snowflake.connector.connect)
- File system operations (dbutils.fs)
- Time-dependent operations (datetime.now)
```

### 5. Clean Up After Tests

```python
@pytest.fixture
def temp_file():
    # Setup
    file = create_temp_file()
    
    yield file
    
    # Teardown
    file.close()
    os.remove(file.name)
```

## Troubleshooting Tests

### Issue 1: Import Errors

**Problem**: `ModuleNotFoundError: No module named 'src'`

**Solution**:
```python
# In tests/__init__.py
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
```

### Issue 2: Spark Tests Failing

**Problem**: Spark session issues

**Solution**:
```python
@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder \
        .appName("Test") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()
```

### Issue 3: Mock Not Working

**Problem**: Mock not being called

**Solution**:
```python
# Ensure correct patch path
@patch('src.extractors.coingecko_extractor.requests.Session.get')
# Not @patch('requests.Session.get')
```

## Coverage Goals

- **Overall**: > 80%
- **Critical paths**: > 90%
- **Utilities**: > 85%
- **Integration**: > 70%

## Next Steps

1. ✅ Run all tests
2. 📊 Check coverage report
3. 🐛 Fix failing tests
4. ✍️ Write tests for new features
5. 🔄 Add to CI/CD pipeline

## Resources

- [Pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Guide](https://docs.python.org/3/library/unittest.mock.html)
- [Coverage.py](https://coverage.readthedocs.io/)
