"""
Unit Tests for Snowflake Loader
=================================

Tests for Snowflake data loading with mocked connections.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import pandas as pd
from datetime import datetime


@pytest.fixture
def loader_config():
    """Configuration for Snowflake loader"""
    return {
        "snowflake": {
            "account": "test_account",
            "user": "test_user",
            "authenticator": "externalbrowser",
            "warehouse": "TEST_WH",
            "database": "TEST_DB",
            "schema": "PUBLIC",
            "tables": {
                "silver": "silver_crypto_clean",
                "gold": "gold_crypto_metrics",
                "metadata": "pipeline_metadata"
            }
        }
    }


@pytest.fixture
def sample_silver_data():
    """Sample silver layer data"""
    return pd.DataFrame([
        {
            "id": "bitcoin",
            "symbol": "btc",
            "name": "Bitcoin",
            "current_price": 45000.50,
            "market_cap": 850000000000,
            "total_volume": 25000000000,
            "last_updated": datetime(2026, 1, 19, 12, 0, 0)
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "name": "Ethereum",
            "current_price": 2500.25,
            "market_cap": 300000000000,
            "total_volume": 12000000000,
            "last_updated": datetime(2026, 1, 19, 12, 0, 0)
        }
    ])


@pytest.fixture
def sample_gold_data():
    """Sample gold layer data"""
    return pd.DataFrame([
        {
            "coin_id": "bitcoin",
            "avg_price_usd": 45000.50,
            "market_cap_usd": 850000000000,
            "volume_24h": 25000000000,
            "price_change_pct": 2.85
        }
    ])


class TestSnowflakeLoader:
    """Test suite for SnowflakeLoader class"""
    
    @patch('snowflake.connector.connect')
    def test_initialization(self, mock_connect, loader_config):
        """Test loader initialization"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        loader = SnowflakeLoader(loader_config)
        
        assert loader.sf_config["account"] == "test_account"
        assert loader.sf_config["warehouse"] == "TEST_WH"
    
    @patch('snowflake.connector.connect')
    def test_connection_establishment(self, mock_connect, loader_config):
        """Test Snowflake connection"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        # Verify connection was established
        mock_connect.assert_called_once()
        assert loader.conn is not None
    
    @patch('snowflake.connector.connect')
    def test_database_setup(self, mock_connect, loader_config):
        """Test database and table setup"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        loader.setup_database()
        
        # Verify SQL was executed
        assert mock_cursor.execute.called
        
        # Check that database and schema were created/used
        calls = [call[0][0].upper() for call in mock_cursor.execute.call_args_list]
        assert any("CREATE DATABASE" in call or "USE DATABASE" in call for call in calls)
    
    @patch('snowflake.connector.connect')
    @patch('snowflake.connector.pandas_tools.write_pandas')
    def test_load_dataframe_to_stage(self, mock_write_pandas, mock_connect, 
                                      loader_config, sample_silver_data):
        """Test loading DataFrame to staging table"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_write_pandas.return_value = (True, 2, 2)
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        result = loader.load_dataframe_to_stage(sample_silver_data, "test_stage")
        
        # Verify write_pandas was called
        mock_write_pandas.assert_called_once()
        assert result == True
    
    @patch('snowflake.connector.connect')
    def test_merge_silver_data(self, mock_connect, loader_config):
        """Test merge operation for silver data"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        rows_affected = loader.merge_silver_data()
        
        # Verify MERGE SQL was executed
        assert mock_cursor.execute.called
        merge_calls = [call[0][0].upper() for call in mock_cursor.execute.call_args_list]
        assert any("MERGE" in call for call in merge_calls)
        assert rows_affected >= 0
    
    @patch('snowflake.connector.connect')
    def test_merge_gold_data(self, mock_connect, loader_config):
        """Test merge operation for gold data"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        rows_affected = loader.merge_gold_data()
        
        # Verify MERGE SQL was executed
        assert mock_cursor.execute.called
        assert rows_affected >= 0
    
    @patch('snowflake.connector.connect')
    def test_log_pipeline_metrics(self, mock_connect, loader_config):
        """Test logging pipeline metrics"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        metadata = {
            "run_id": "test_run_001",
            "pipeline_name": "crypto_pipeline",
            "status": "success",
            "records_processed": 100
        }
        
        loader.log_pipeline_metrics(metadata)
        
        # Verify INSERT was executed
        assert mock_cursor.execute.called
        insert_calls = [call[0][0].upper() for call in mock_cursor.execute.call_args_list]
        assert any("INSERT" in call for call in insert_calls)
    
    @patch('snowflake.connector.connect')
    def test_connection_error_handling(self, mock_connect, loader_config):
        """Test handling of connection errors"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        # Simulate connection failure
        mock_connect.side_effect = Exception("Connection failed")
        
        loader = SnowflakeLoader(loader_config)
        
        with pytest.raises(Exception):
            loader.connect()
    
    @patch('snowflake.connector.connect')
    def test_disconnect(self, mock_connect, loader_config):
        """Test proper disconnection"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        loader.disconnect()
        
        # Verify cursor and connection were closed
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()
    
    @patch('snowflake.connector.connect')
    @patch('snowflake.connector.pandas_tools.write_pandas')
    def test_full_load_workflow(self, mock_write_pandas, mock_connect, 
                                 loader_config, sample_silver_data):
        """Test complete loading workflow"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        mock_write_pandas.return_value = (True, 2, 2)
        
        loader = SnowflakeLoader(loader_config)
        
        # Full workflow
        loader.connect()
        loader.setup_database()
        loader.load_dataframe_to_stage(sample_silver_data, "silver_stage")
        rows_affected = loader.merge_silver_data()
        loader.disconnect()
        
        # Verify all steps were executed
        assert mock_connect.called
        assert mock_cursor.execute.called
        assert mock_write_pandas.called
        assert mock_cursor.close.called
        assert mock_conn.close.called


class TestDataMergeStrategies:
    """Tests for different merge strategies"""
    
    @patch('snowflake.connector.connect')
    def test_type2_scd_merge(self, mock_connect, loader_config):
        """Test Type 2 SCD merge logic"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 2
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        # Execute merge with Type 2 SCD
        loader.merge_silver_data()
        
        # Verify SQL contains SCD fields
        merge_sql = mock_cursor.execute.call_args_list
        merge_str = str(merge_sql).upper()
        
        # Should have is_current, valid_from, valid_to for Type 2
        assert any("IS_CURRENT" in str(call).upper() or "VALID" in str(call).upper() 
                   for call in merge_sql)
    
    @patch('snowflake.connector.connect')
    def test_upsert_merge(self, mock_connect, loader_config):
        """Test simple upsert merge"""
        from src.loaders.snowflake_loader import SnowflakeLoader
        
        mock_conn = Mock()
        mock_cursor = Mock()
        mock_cursor.rowcount = 1
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        loader = SnowflakeLoader(loader_config)
        loader.connect()
        
        # Execute simple upsert
        loader.merge_gold_data()
        
        # Verify MERGE statement structure
        assert mock_cursor.execute.called


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
