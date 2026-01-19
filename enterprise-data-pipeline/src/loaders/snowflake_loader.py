"""
Snowflake Data Loader
Implements incremental loading strategy with merge operations
"""

import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import logging
from typing import Dict, List, Optional
from datetime import datetime
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """
    Snowflake loader with incremental merge capabilities
    Implements Type 2 SCD (Slowly Changing Dimensions) pattern
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Snowflake Loader
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.sf_config = config['snowflake']
        self.conn = None
        self.cursor = None
        
        logger.info("SnowflakeLoader initialized")
    
    def connect(self):
        """Establish connection to Snowflake"""
        logger.info("Connecting to Snowflake")
        
        try:
            self.conn = snowflake.connector.connect(
                account=self.sf_config['account'],
                user=self.sf_config['user'],
                password=self.sf_config['password'],
                warehouse=self.sf_config['warehouse'],
                database=self.sf_config['database'],
                schema=self.sf_config['schema'],
                role=self.sf_config['role']
            )
            
            self.cursor = self.conn.cursor(DictCursor)
            logger.info("Successfully connected to Snowflake")
            
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise
    
    def close(self):
        """Close Snowflake connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Snowflake connection closed")
    
    def setup_database(self):
        """Initialize database, schema, and tables"""
        logger.info("Setting up Snowflake database and tables")
        
        try:
            # Create database and schema if not exists
            self.cursor.execute(f"""
                CREATE DATABASE IF NOT EXISTS {self.sf_config['database']}
            """)
            
            self.cursor.execute(f"""
                CREATE SCHEMA IF NOT EXISTS {self.sf_config['database']}.{self.sf_config['schema']}
            """)
            
            # Create Silver table (main fact table)
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.sf_config['tables']['silver']} (
                    coin_id VARCHAR(100) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    name VARCHAR(255) NOT NULL,
                    current_price FLOAT,
                    market_cap BIGINT,
                    market_cap_rank INTEGER,
                    total_volume BIGINT,
                    high_24h FLOAT,
                    low_24h FLOAT,
                    price_change_24h FLOAT,
                    price_change_percentage_24h FLOAT,
                    price_change_pct_7d FLOAT,
                    price_change_pct_30d FLOAT,
                    circulating_supply FLOAT,
                    total_supply FLOAT,
                    max_supply FLOAT,
                    ath FLOAT,
                    ath_date TIMESTAMP_NTZ,
                    atl FLOAT,
                    atl_date TIMESTAMP_NTZ,
                    last_updated TIMESTAMP_NTZ,
                    extracted_at TIMESTAMP_NTZ,
                    price_volatility_24h FLOAT,
                    market_cap_category VARCHAR(50),
                    volume_to_market_cap_ratio FLOAT,
                    supply_percentage FLOAT,
                    distance_from_ath_pct FLOAT,
                    distance_from_atl_pct FLOAT,
                    is_price_anomaly BOOLEAN,
                    is_volume_spike BOOLEAN,
                    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    is_current BOOLEAN DEFAULT TRUE,
                    valid_from TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    valid_to TIMESTAMP_NTZ,
                    PRIMARY KEY (coin_id, valid_from)
                )
            """)
            
            # Create Gold table (aggregated metrics)
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.sf_config['tables']['gold']} (
                    metric_date DATE NOT NULL,
                    market_cap_category VARCHAR(50) NOT NULL,
                    num_coins INTEGER,
                    total_market_cap BIGINT,
                    total_volume_24h BIGINT,
                    avg_price FLOAT,
                    avg_price_change_24h FLOAT,
                    avg_volatility FLOAT,
                    avg_volume_ratio FLOAT,
                    max_market_cap BIGINT,
                    min_market_cap BIGINT,
                    metric_timestamp TIMESTAMP_NTZ,
                    calculation_type VARCHAR(100),
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (metric_date, market_cap_category)
                )
            """)
            
            # Create metadata table for pipeline tracking
            self.cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.sf_config['tables']['metadata']} (
                    run_id VARCHAR(100) PRIMARY KEY,
                    pipeline_name VARCHAR(255),
                    run_date TIMESTAMP_NTZ,
                    status VARCHAR(50),
                    records_extracted INTEGER,
                    records_processed INTEGER,
                    records_loaded INTEGER,
                    records_failed INTEGER,
                    execution_time_seconds FLOAT,
                    error_message VARCHAR(5000),
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            logger.info("Database setup completed successfully")
            
        except Exception as e:
            logger.error(f"Failed to setup database: {e}")
            raise
    
    def load_dataframe_to_stage(self, df: pd.DataFrame, stage_table: str):
        """
        Load pandas DataFrame to staging table
        
        Args:
            df: Pandas DataFrame
            stage_table: Staging table name
        """
        logger.info(f"Loading {len(df)} records to staging table: {stage_table}")
        
        try:
            # Drop staging table if exists
            self.cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")
            
            # Use write_pandas for efficient bulk loading
            success, nchunks, nrows, _ = write_pandas(
                conn=self.conn,
                df=df,
                table_name=stage_table,
                auto_create_table=True,
                overwrite=True
            )
            
            logger.info(f"Loaded {nrows} rows in {nchunks} chunks to staging")
            
        except Exception as e:
            logger.error(f"Failed to load to staging: {e}")
            raise
    
    def merge_silver_data(self, stage_table: str):
        """
        Perform incremental merge from staging to silver table
        Implements Type 2 SCD pattern
        
        Args:
            stage_table: Staging table name
        """
        logger.info("Performing incremental merge to silver table")
        
        target_table = self.sf_config['tables']['silver']
        merge_key = self.sf_config['loading']['merge_key']
        
        try:
            # Type 2 SCD Merge: Close old records and insert new ones
            merge_query = f"""
            MERGE INTO {target_table} AS target
            USING {stage_table} AS source
            ON target.{merge_key} = source.{merge_key} AND target.is_current = TRUE
            
            WHEN MATCHED AND (
                target.current_price != source.current_price OR
                target.market_cap != source.market_cap OR
                target.total_volume != source.total_volume
            ) THEN UPDATE SET
                target.is_current = FALSE,
                target.valid_to = CURRENT_TIMESTAMP()
            
            WHEN NOT MATCHED THEN INSERT (
                coin_id, symbol, name, current_price, market_cap, market_cap_rank,
                total_volume, high_24h, low_24h, price_change_24h,
                price_change_percentage_24h, price_change_pct_7d, price_change_pct_30d,
                circulating_supply, total_supply, max_supply, ath, ath_date,
                atl, atl_date, last_updated, extracted_at, price_volatility_24h,
                market_cap_category, volume_to_market_cap_ratio, supply_percentage,
                distance_from_ath_pct, distance_from_atl_pct, is_price_anomaly,
                is_volume_spike, updated_at, is_current, valid_from
            ) VALUES (
                source.coin_id, source.symbol, source.name, source.current_price,
                source.market_cap, source.market_cap_rank, source.total_volume,
                source.high_24h, source.low_24h, source.price_change_24h,
                source.price_change_percentage_24h, source.price_change_pct_7d,
                source.price_change_pct_30d, source.circulating_supply,
                source.total_supply, source.max_supply, source.ath, source.ath_date,
                source.atl, source.atl_date, source.last_updated, source.extracted_at,
                source.price_volatility_24h, source.market_cap_category,
                source.volume_to_market_cap_ratio, source.supply_percentage,
                source.distance_from_ath_pct, source.distance_from_atl_pct,
                source.is_price_anomaly, source.is_volume_spike,
                CURRENT_TIMESTAMP(), TRUE, CURRENT_TIMESTAMP()
            )
            """
            
            self.cursor.execute(merge_query)
            rows_merged = self.cursor.rowcount
            
            # Insert new versions for updated records
            insert_query = f"""
            INSERT INTO {target_table} (
                coin_id, symbol, name, current_price, market_cap, market_cap_rank,
                total_volume, high_24h, low_24h, price_change_24h,
                price_change_percentage_24h, price_change_pct_7d, price_change_pct_30d,
                circulating_supply, total_supply, max_supply, ath, ath_date,
                atl, atl_date, last_updated, extracted_at, price_volatility_24h,
                market_cap_category, volume_to_market_cap_ratio, supply_percentage,
                distance_from_ath_pct, distance_from_atl_pct, is_price_anomaly,
                is_volume_spike, updated_at, is_current, valid_from
            )
            SELECT 
                s.coin_id, s.symbol, s.name, s.current_price, s.market_cap,
                s.market_cap_rank, s.total_volume, s.high_24h, s.low_24h,
                s.price_change_24h, s.price_change_percentage_24h,
                s.price_change_pct_7d, s.price_change_pct_30d,
                s.circulating_supply, s.total_supply, s.max_supply,
                s.ath, s.ath_date, s.atl, s.atl_date, s.last_updated,
                s.extracted_at, s.price_volatility_24h, s.market_cap_category,
                s.volume_to_market_cap_ratio, s.supply_percentage,
                s.distance_from_ath_pct, s.distance_from_atl_pct,
                s.is_price_anomaly, s.is_volume_spike,
                CURRENT_TIMESTAMP(), TRUE, CURRENT_TIMESTAMP()
            FROM {stage_table} s
            INNER JOIN {target_table} t
                ON s.{merge_key} = t.{merge_key}
                AND t.is_current = FALSE
                AND t.valid_to = (
                    SELECT MAX(valid_to) 
                    FROM {target_table} 
                    WHERE {merge_key} = s.{merge_key}
                )
            """
            
            self.cursor.execute(insert_query)
            rows_inserted = self.cursor.rowcount
            
            self.conn.commit()
            
            logger.info(f"Merge completed - Merged: {rows_merged}, Inserted: {rows_inserted}")
            
        except Exception as e:
            logger.error(f"Failed to merge data: {e}")
            self.conn.rollback()
            raise
    
    def merge_gold_data(self, stage_table: str):
        """
        Merge aggregated metrics to gold table
        
        Args:
            stage_table: Staging table name
        """
        logger.info("Merging data to gold table")
        
        target_table = self.sf_config['tables']['gold']
        
        try:
            merge_query = f"""
            MERGE INTO {target_table} AS target
            USING {stage_table} AS source
            ON target.metric_date = source.metric_date 
                AND target.market_cap_category = source.market_cap_category
            
            WHEN MATCHED THEN UPDATE SET
                target.num_coins = source.num_coins,
                target.total_market_cap = source.total_market_cap,
                target.total_volume_24h = source.total_volume_24h,
                target.avg_price = source.avg_price,
                target.avg_price_change_24h = source.avg_price_change_24h,
                target.avg_volatility = source.avg_volatility,
                target.avg_volume_ratio = source.avg_volume_ratio,
                target.max_market_cap = source.max_market_cap,
                target.min_market_cap = source.min_market_cap,
                target.metric_timestamp = source.metric_timestamp,
                target.calculation_type = source.calculation_type
            
            WHEN NOT MATCHED THEN INSERT (
                metric_date, market_cap_category, num_coins, total_market_cap,
                total_volume_24h, avg_price, avg_price_change_24h, avg_volatility,
                avg_volume_ratio, max_market_cap, min_market_cap, metric_timestamp,
                calculation_type
            ) VALUES (
                source.metric_date, source.market_cap_category, source.num_coins,
                source.total_market_cap, source.total_volume_24h, source.avg_price,
                source.avg_price_change_24h, source.avg_volatility, source.avg_volume_ratio,
                source.max_market_cap, source.min_market_cap, source.metric_timestamp,
                source.calculation_type
            )
            """
            
            self.cursor.execute(merge_query)
            rows_affected = self.cursor.rowcount
            
            self.conn.commit()
            
            logger.info(f"Gold merge completed - Rows affected: {rows_affected}")
            
        except Exception as e:
            logger.error(f"Failed to merge gold data: {e}")
            self.conn.rollback()
            raise
    
    def log_pipeline_run(self, run_metadata: Dict):
        """
        Log pipeline execution metadata
        
        Args:
            run_metadata: Dictionary with run information
        """
        logger.info("Logging pipeline run metadata")
        
        try:
            table = self.sf_config['tables']['metadata']
            
            insert_query = f"""
            INSERT INTO {table} (
                run_id, pipeline_name, run_date, status, records_extracted,
                records_processed, records_loaded, records_failed,
                execution_time_seconds, error_message
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """
            
            self.cursor.execute(insert_query, (
                run_metadata['run_id'],
                run_metadata['pipeline_name'],
                run_metadata['run_date'],
                run_metadata['status'],
                run_metadata.get('records_extracted', 0),
                run_metadata.get('records_processed', 0),
                run_metadata.get('records_loaded', 0),
                run_metadata.get('records_failed', 0),
                run_metadata.get('execution_time', 0),
                run_metadata.get('error_message', '')
            ))
            
            self.conn.commit()
            logger.info("Pipeline run logged successfully")
            
        except Exception as e:
            logger.error(f"Failed to log pipeline run: {e}")


def main():
    """Main execution function"""
    import yaml
    
    # Load configuration
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    loader = SnowflakeLoader(config)
    
    try:
        loader.connect()
        loader.setup_database()
        
        print("✅ Snowflake setup completed successfully")
        
    finally:
        loader.close()


if __name__ == "__main__":
    main()
