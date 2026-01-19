"""
Enterprise Data Pipeline - Source Package
========================================

Cryptocurrency ETL pipeline with Databricks and Snowflake integration.

Modules:
    - extractors: API data extraction (CoinGecko)
    - transformers: PySpark data processing (Bronze -> Silver -> Gold)
    - loaders: Snowflake data loading with merge operations
    - utils: Utility functions (logging, config, validators)
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from .extractors.coingecko_extractor import CryptoExtractor
from .transformers.spark_processor import SparkProcessor
from .loaders.snowflake_loader import SnowflakeLoader

__all__ = [
    "CryptoExtractor",
    "SparkProcessor",
    "SnowflakeLoader",
]
