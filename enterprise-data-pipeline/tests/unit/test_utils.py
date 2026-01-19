"""
Unit Tests for Utilities
=========================

Tests for logging, configuration, and validators.
"""

import pytest
import json
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import pandas as pd
from pyspark.sql import SparkSession


class TestStructuredLogger:
    """Tests for StructuredLogger"""
    
    def test_logger_initialization(self):
        """Test logger initialization"""
        from src.utils.logging_config import StructuredLogger
        
        logger = StructuredLogger("test_logger")
        
        assert logger.name == "test_logger"
        assert logger.logger is not None
    
    def test_set_run_id(self):
        """Test setting run ID"""
        from src.utils.logging_config import StructuredLogger
        
        logger = StructuredLogger("test")
        logger.set_run_id("run_123")
        
        assert logger.run_id == "run_123"
        assert logger.context["run_id"] == "run_123"
    
    def test_add_context(self):
        """Test adding context"""
        from src.utils.logging_config import StructuredLogger
        
        logger = StructuredLogger("test")
        logger.add_context(environment="dev", user="test_user")
        
        assert logger.context["environment"] == "dev"
        assert logger.context["user"] == "test_user"
    
    def test_log_event(self, capsys):
        """Test event logging"""
        from src.utils.logging_config import StructuredLogger
        
        logger = StructuredLogger("test")
        logger.log_event("test_event", {"key": "value"})
        
        captured = capsys.readouterr()
        
        # Should output JSON
        assert "test_event" in captured.out
    
    def test_log_metric(self, capsys):
        """Test metric logging"""
        from src.utils.logging_config import StructuredLogger
        
        logger = StructuredLogger("test")
        logger.log_metric("records_processed", 1000, unit="records")
        
        captured = capsys.readouterr()
        
        assert "metric" in captured.out
        assert "1000" in captured.out


class TestConfigLoader:
    """Tests for configuration loading"""
    
    @patch("builtins.open", new_callable=mock_open, read_data="api:\n  base_url: 'http://test.com'")
    def test_load_config(self, mock_file):
        """Test loading config from file"""
        from src.utils.config_loader import load_config
        
        with patch('pathlib.Path.exists', return_value=True):
            config = load_config("config/config.yaml")
            
            assert config is not None
            assert "api" in config
    
    def test_config_file_not_found(self):
        """Test handling of missing config file"""
        from src.utils.config_loader import load_config, ConfigurationError
        
        with patch('pathlib.Path.exists', return_value=False):
            with pytest.raises(ConfigurationError):
                load_config("nonexistent.yaml")
    
    @patch("builtins.open", new_callable=mock_open, 
           read_data="base:\n  value: 1\ndev:\n  environment: development")
    def test_environment_config_merge(self, mock_file):
        """Test environment config merging"""
        from src.utils.config_loader import get_environment_config
        
        with patch('pathlib.Path.exists', return_value=True):
            base_config = {"base": {"value": 1}}
            
            merged = get_environment_config("development", base_config)
            
            assert merged is not None
    
    def test_validate_required_config(self):
        """Test config validation"""
        from src.utils.config_loader import validate_required_config, ConfigurationError
        
        config = {
            "api": {
                "base_url": "http://test.com",
                "timeout": 30
            }
        }
        
        # Valid config
        assert validate_required_config(config, ["api.base_url", "api.timeout"]) == True
        
        # Missing required key
        with pytest.raises(ConfigurationError):
            validate_required_config(config, ["api.missing_key"])


class TestValidators:
    """Tests for data validators"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session"""
        spark = SparkSession.builder \
            .appName("TestValidators") \
            .master("local[2]") \
            .getOrCreate()
        
        yield spark
        
        spark.stop()
    
    def test_validate_dataframe_success(self, spark):
        """Test successful DataFrame validation"""
        from src.utils.validators import validate_dataframe
        
        data = pd.DataFrame([
            {"id": "1", "name": "test", "value": 100}
        ])
        df = spark.createDataFrame(data)
        
        results = validate_dataframe(
            df,
            required_columns=["id", "name", "value"]
        )
        
        assert results["valid"] == True
        assert len(results["errors"]) == 0
    
    def test_validate_dataframe_missing_columns(self, spark):
        """Test DataFrame validation with missing columns"""
        from src.utils.validators import validate_dataframe
        
        data = pd.DataFrame([
            {"id": "1", "name": "test"}  # Missing 'value'
        ])
        df = spark.createDataFrame(data)
        
        results = validate_dataframe(
            df,
            required_columns=["id", "name", "value"]
        )
        
        assert results["valid"] == False
        assert len(results["errors"]) > 0
    
    def test_check_data_quality(self, spark):
        """Test data quality checks"""
        from src.utils.validators import check_data_quality
        
        data = pd.DataFrame([
            {"id": "1", "value": 100},
            {"id": "2", "value": 200},
        ])
        df = spark.createDataFrame(data)
        
        quality_rules = {
            "required_columns": ["id", "value"],
            "max_null_percentage": 5,
            "numeric_ranges": {
                "value": {"min": 0, "max": 1000}
            }
        }
        
        results = check_data_quality(df, quality_rules)
        
        assert "passed" in results
        assert "checks" in results
        assert "summary" in results
    
    def test_validate_schema(self, spark):
        """Test schema validation"""
        from src.utils.validators import validate_schema
        
        data = pd.DataFrame([
            {"id": "1", "value": 100}
        ])
        df = spark.createDataFrame(data)
        
        expected_schema = {
            "id": "StringType",
            "value": "LongType"
        }
        
        results = validate_schema(df, expected_schema)
        
        assert "valid" in results
        assert "mismatches" in results
    
    def test_detect_anomalies(self):
        """Test anomaly detection"""
        from src.utils.validators import detect_anomalies
        
        data = pd.DataFrame({
            "value": [100, 105, 102, 98, 1000]  # 1000 is an outlier
        })
        
        anomalies = detect_anomalies(data, ["value"], z_threshold=2.0)
        
        assert "value" in anomalies
        assert anomalies["value"]["anomaly_count"] > 0


class TestJSONFormatter:
    """Tests for JSON log formatter"""
    
    def test_json_formatter_output(self):
        """Test JSON formatter produces valid JSON"""
        from src.utils.logging_config import JSONFormatter
        import logging
        
        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None
        )
        
        output = formatter.format(record)
        
        # Should be valid JSON
        parsed = json.loads(output)
        assert parsed["message"] == "Test message"
        assert parsed["level"] == "INFO"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
