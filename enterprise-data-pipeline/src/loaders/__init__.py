"""
Loaders Package - Data Warehouse Loading
=========================================

Snowflake data loader with staging and merge operations.
"""

from .snowflake_loader import SnowflakeLoader

__all__ = ["SnowflakeLoader"]
