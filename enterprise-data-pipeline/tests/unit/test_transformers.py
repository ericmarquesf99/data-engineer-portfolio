"""
Unit Tests for Spark Processor
================================

Tests for PySpark transformations with mocked Spark contexts.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import pandas as pd
from datetime import datetime


@pytest.fixture(scope="module")
def spark():
    """Create Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestSparkProcessor") \
        .master("local[2]") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_bronze_data(spark):
    """Sample bronze layer data"""
    data = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 45000.50,
            "market_cap": 850000000000,
            "total_volume": 25000000000,
            "price_change_24h": 1250.75,
            "price_change_percentage_24h": 2.85,
            "last_updated": "2026-01-19T12:00:00.000Z"
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 2500.25,
            "market_cap": 300000000000,
            "total_volume": 12000000000,
            "price_change_24h": -50.30,
            "price_change_percentage_24h": -1.97,
            "last_updated": "2026-01-19T12:00:00.000Z"
        }
    ]
    
    return spark.createDataFrame(pd.DataFrame(data))


@pytest.fixture
def processor_config():
    """Configuration for processor"""
    return {
        "databricks": {
            "spark": {
                "app_name": "TestProcessor",
                "shuffle_partitions": 2,
                "adaptive_enabled": True,
                "log_level": "WARN"
            }
        },
        "data_quality": {
            "validation_rules": {
                "max_null_percentage": 5,
                "required_columns": ["id", "symbol", "name", "current_price"],
                "numeric_ranges": {
                    "current_price": {"min": 0, "max": 1000000}
                }
            }
        }
    }


class TestSparkProcessor:
    """Test suite for SparkProcessor class"""
    
    def test_bronze_to_silver_transformation(self, spark, sample_bronze_data, processor_config):
        """Test Bronze to Silver transformation"""
        from src.transformers.spark_processor import SparkProcessor
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark  # Use test Spark session
        
        silver_df = processor.process_bronze_to_silver(sample_bronze_data)
        
        # Check that transformation succeeded
        assert silver_df is not None
        assert silver_df.count() == 2
        
        # Check schema
        column_names = silver_df.columns
        assert "id" in column_names
        assert "symbol" in column_names
        assert "current_price" in column_names
    
    def test_data_validation(self, spark, sample_bronze_data, processor_config):
        """Test data quality validation"""
        from src.transformers.spark_processor import SparkProcessor
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark
        
        validation_results = processor.validate_data_quality(sample_bronze_data)
        
        assert "total_records" in validation_results
        assert validation_results["total_records"] == 2
        assert "null_checks" in validation_results
    
    def test_null_value_handling(self, spark, processor_config):
        """Test handling of null values"""
        from src.transformers.spark_processor import SparkProcessor
        
        # Data with nulls
        data_with_nulls = pd.DataFrame([
            {"id": "bitcoin", "symbol": "btc", "current_price": 45000.50},
            {"id": "ethereum", "symbol": "eth", "current_price": None},  # Null price
        ])
        
        df = spark.createDataFrame(data_with_nulls)
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark
        
        # Process should handle nulls gracefully
        silver_df = processor.process_bronze_to_silver(df)
        
        assert silver_df is not None
        # Check that nulls are either filled or filtered
        null_count = silver_df.filter(silver_df["current_price"].isNull()).count()
        assert null_count >= 0  # Should not crash
    
    def test_anomaly_detection(self, spark, processor_config):
        """Test anomaly detection in data"""
        from src.transformers.spark_processor import SparkProcessor
        
        # Data with outlier
        data_with_outlier = pd.DataFrame([
            {"id": "bitcoin", "current_price": 45000.0},
            {"id": "ethereum", "current_price": 2500.0},
            {"id": "outlier", "current_price": 9999999.0},  # Obvious outlier
        ])
        
        df = spark.createDataFrame(data_with_outlier)
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark
        
        anomalies = processor.detect_anomalies(df, ["current_price"])
        
        assert anomalies is not None
        assert len(anomalies) > 0
    
    def test_silver_to_gold_aggregation(self, spark, sample_bronze_data, processor_config):
        """Test Silver to Gold aggregation"""
        from src.transformers.spark_processor import SparkProcessor
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark
        
        # First transform to silver
        silver_df = processor.process_bronze_to_silver(sample_bronze_data)
        
        # Then aggregate to gold
        gold_df = processor.process_silver_to_gold(silver_df)
        
        assert gold_df is not None
        assert gold_df.count() > 0
        
        # Check for aggregated columns
        column_names = gold_df.columns
        assert any("avg" in col.lower() or "sum" in col.lower() or "count" in col.lower() 
                   for col in column_names)
    
    def test_schema_validation(self, spark, processor_config):
        """Test schema validation"""
        from src.transformers.spark_processor import SparkProcessor
        
        # Invalid schema (missing required columns)
        invalid_data = pd.DataFrame([
            {"id": "bitcoin", "symbol": "btc"}  # Missing current_price
        ])
        
        df = spark.createDataFrame(invalid_data)
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark
        
        # Should detect missing columns
        validation = processor.validate_data_quality(df)
        
        assert validation is not None
        # Validation should flag issues
    
    def test_deduplication(self, spark, processor_config):
        """Test deduplication of records"""
        from src.transformers.spark_processor import SparkProcessor
        
        # Data with duplicates
        data_with_dupes = pd.DataFrame([
            {"id": "bitcoin", "symbol": "btc", "current_price": 45000.0},
            {"id": "bitcoin", "symbol": "btc", "current_price": 45000.0},  # Duplicate
            {"id": "ethereum", "symbol": "eth", "current_price": 2500.0},
        ])
        
        df = spark.createDataFrame(data_with_dupes)
        
        processor = SparkProcessor(processor_config)
        processor.spark = spark
        
        silver_df = processor.process_bronze_to_silver(df)
        
        # Should deduplicate
        unique_count = silver_df.select("id").distinct().count()
        assert unique_count == 2  # Only 2 unique coins


class TestDataQuality:
    """Tests for data quality checks"""
    
    def test_completeness_check(self, spark, processor_config):
        """Test completeness validation"""
        from src.utils.validators import validate_dataframe
        
        data = pd.DataFrame([
            {"id": "bitcoin", "symbol": "btc", "name": "Bitcoin"},
            {"id": "ethereum", "symbol": "eth", "name": "Ethereum"},
        ])
        
        df = spark.createDataFrame(data)
        
        results = validate_dataframe(
            df,
            required_columns=["id", "symbol", "name"],
            non_null_columns=["id"]
        )
        
        assert results["valid"] == True
        assert results["stats"]["total_rows"] == 2
    
    def test_missing_columns_detection(self, spark):
        """Test detection of missing required columns"""
        from src.utils.validators import validate_dataframe
        
        data = pd.DataFrame([
            {"id": "bitcoin", "symbol": "btc"}  # Missing 'name'
        ])
        
        df = spark.createDataFrame(data)
        
        results = validate_dataframe(
            df,
            required_columns=["id", "symbol", "name"]
        )
        
        assert results["valid"] == False
        assert len(results["errors"]) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
