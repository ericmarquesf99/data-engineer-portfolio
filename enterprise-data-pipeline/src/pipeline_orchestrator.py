"""
Main Pipeline Orchestrator
Coordinates the entire ETL process with logging and monitoring
"""

import os
import sys
import yaml
import logging
import time
import traceback
from datetime import datetime
from typing import Dict, Optional
import pandas as pd

# Add src to path
sys.path.append(os.path.dirname(__file__))

from api_extractor import APIExtractor
from spark_processor import SparkProcessor
from snowflake_loader import SnowflakeLoader


class PipelineOrchestrator:
    """
    Main pipeline orchestrator with comprehensive logging and error handling
    """
    
    def __init__(self, config_path: str = '../config/config.yaml'):
        """
        Initialize pipeline orchestrator
        
        Args:
            config_path: Path to configuration file
        """
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        
        self.logger.info("Initializing Pipeline Orchestrator")
        self.config = self.load_config(config_path)
        
        # Pipeline state
        self.run_id = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        self.start_time = None
        self.metrics = {
            'records_extracted': 0,
            'records_processed': 0,
            'records_valid': 0,
            'records_invalid': 0,
            'records_loaded_silver': 0,
            'records_loaded_gold': 0
        }
        
        self.logger.info(f"Pipeline Run ID: {self.run_id}")
    
    def setup_logging(self):
        """Setup comprehensive logging"""
        log_dir = '../logs'
        os.makedirs(log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(log_dir, f'pipeline_{timestamp}.log')
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
    
    def load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        self.logger.info("Configuration loaded successfully")
        return config
    
    def extract_phase(self) -> str:
        """
        Phase 1: Extract data from API
        
        Returns:
            Path to extracted data file
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 1: EXTRACTION")
        self.logger.info("=" * 80)
        
        try:
            extractor = APIExtractor(self.config)
            
            # Extract multiple pages
            num_pages = 3
            per_page = 100
            
            self.logger.info(f"Extracting {num_pages} pages with {per_page} records each")
            data = extractor.extract_multiple_pages(num_pages=num_pages, per_page=per_page)
            
            self.metrics['records_extracted'] = len(data)
            self.logger.info(f"✅ Extraction completed: {len(data)} records")
            
            # Save to file
            output_path = f'../logs/crypto_data_{self.run_id}.json'
            extractor.save_to_json(data, output_path)
            
            return output_path
            
        except Exception as e:
            self.logger.error(f"❌ Extraction failed: {e}")
            self.logger.error(traceback.format_exc())
            raise
    
    def process_phase(self, data_path: str) -> tuple:
        """
        Phase 2: Process data with PySpark
        
        Args:
            data_path: Path to raw data
            
        Returns:
            Tuple of (silver_df, gold_df)
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 2: PROCESSING")
        self.logger.info("=" * 80)
        
        processor = None
        
        try:
            processor = SparkProcessor(self.config)
            
            # Load bronze data
            self.logger.info("Loading bronze data...")
            bronze_df = processor.load_bronze_data(data_path)
            
            # Validate data
            self.logger.info("Validating data...")
            valid_df, invalid_df = processor.validate_data(bronze_df)
            
            self.metrics['records_valid'] = valid_df.count()
            self.metrics['records_invalid'] = invalid_df.count()
            
            self.logger.info(f"Valid records: {self.metrics['records_valid']}")
            self.logger.info(f"Invalid records: {self.metrics['records_invalid']}")
            
            # Transform to silver
            self.logger.info("Transforming to Silver layer...")
            silver_df = processor.transform_to_silver(valid_df)
            silver_df = processor.detect_anomalies(silver_df)
            
            # Convert to Pandas for Snowflake loading
            silver_pd = silver_df.toPandas()
            self.metrics['records_processed'] = len(silver_pd)
            
            # Transform to gold
            self.logger.info("Transforming to Gold layer...")
            gold_df = processor.transform_to_gold(silver_df)
            gold_pd = gold_df.toPandas()
            
            self.logger.info(f"✅ Processing completed")
            self.logger.info(f"   Silver records: {len(silver_pd)}")
            self.logger.info(f"   Gold records: {len(gold_pd)}")
            
            return silver_pd, gold_pd
            
        except Exception as e:
            self.logger.error(f"❌ Processing failed: {e}")
            self.logger.error(traceback.format_exc())
            raise
        
        finally:
            if processor:
                processor.stop()
    
    def load_phase(self, silver_df: pd.DataFrame, gold_df: pd.DataFrame):
        """
        Phase 3: Load data to Snowflake
        
        Args:
            silver_df: Silver layer DataFrame
            gold_df: Gold layer DataFrame
        """
        self.logger.info("=" * 80)
        self.logger.info("PHASE 3: LOADING")
        self.logger.info("=" * 80)
        
        loader = None
        
        try:
            loader = SnowflakeLoader(self.config)
            loader.connect()
            
            # Setup tables
            self.logger.info("Setting up Snowflake tables...")
            loader.setup_database()
            
            # Load silver data via staging + merge
            self.logger.info("Loading Silver data to Snowflake (staging -> merge)...")
            silver_stage = "stg_silver_load"
            loader.load_dataframe_to_stage(silver_df, silver_stage)
            loader.merge_silver_data(silver_stage)
            
            self.metrics['records_loaded_silver'] = len(silver_df)
            self.logger.info(f"✅ Silver data loaded: {len(silver_df)} records")
            
            # Load gold data via staging + merge
            self.logger.info("Loading Gold data to Snowflake (staging -> merge)...")
            gold_stage = "stg_gold_load"
            loader.load_dataframe_to_stage(gold_df, gold_stage)
            loader.merge_gold_data(gold_stage)
            
            self.metrics['records_loaded_gold'] = len(gold_df)
            self.logger.info(f"✅ Gold data loaded: {len(gold_df)} records")
            
        except Exception as e:
            self.logger.error(f"❌ Loading failed: {e}")
            self.logger.error(traceback.format_exc())
            raise
        
        finally:
            if loader:
                loader.close()
    
    def log_pipeline_metrics(self, status: str, error_msg: str = ''):
        """
        Log final pipeline metrics to Snowflake
        
        Args:
            status: Pipeline status (SUCCESS/FAILED)
            error_msg: Error message if failed
        """
        self.logger.info("=" * 80)
        self.logger.info("LOGGING PIPELINE METRICS")
        self.logger.info("=" * 80)
        
        loader = None
        
        try:
            execution_time = (datetime.utcnow() - self.start_time).total_seconds()
            
            run_metadata = {
                'run_id': self.run_id,
                'pipeline_name': 'enterprise_crypto_pipeline',
                'run_date': self.start_time,
                'status': status,
                'records_extracted': self.metrics['records_extracted'],
                'records_processed': self.metrics['records_processed'],
                'records_loaded': self.metrics['records_loaded_silver'],
                'records_failed': self.metrics['records_invalid'],
                'execution_time': execution_time,
                'error_message': error_msg[:5000] if error_msg else ''
            }
            
            loader = SnowflakeLoader(self.config)
            loader.connect()
            loader.log_pipeline_run(run_metadata)
            
            self.logger.info("✅ Pipeline metrics logged successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to log pipeline metrics: {e}")
            # Don't raise - this is not critical
        
        finally:
            if loader:
                loader.close()
    
    def print_summary(self):
        """Print pipeline execution summary"""
        duration = (datetime.utcnow() - self.start_time).total_seconds()
        
        summary = f"""
        {'=' * 80}
        PIPELINE EXECUTION SUMMARY
        {'=' * 80}
        
        Run ID: {self.run_id}
        Start Time: {self.start_time}
        Duration: {duration:.2f} seconds
        
        METRICS:
        --------
        Records Extracted:     {self.metrics['records_extracted']:,}
        Records Valid:         {self.metrics['records_valid']:,}
        Records Invalid:       {self.metrics['records_invalid']:,}
        Records Processed:     {self.metrics['records_processed']:,}
        Records Loaded (Silver): {self.metrics['records_loaded_silver']:,}
        Records Loaded (Gold):   {self.metrics['records_loaded_gold']:,}
        
        Data Quality Score:    {(self.metrics['records_valid'] / max(self.metrics['records_extracted'], 1) * 100):.2f}%
        
        {'=' * 80}
        """
        
        print(summary)
        self.logger.info(summary)
    
    def run(self):
        """Execute the complete pipeline"""
        self.start_time = datetime.utcnow()
        self.logger.info("=" * 80)
        self.logger.info("STARTING ENTERPRISE DATA PIPELINE")
        self.logger.info("=" * 80)
        
        try:
            # Phase 1: Extract
            data_path = self.extract_phase()
            
            # Phase 2: Process
            silver_df, gold_df = self.process_phase(data_path)
            
            # Phase 3: Load
            self.load_phase(silver_df, gold_df)
            
            # Log success
            self.log_pipeline_metrics(status='SUCCESS')
            
            # Print summary
            self.print_summary()
            
            self.logger.info("=" * 80)
            self.logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            self.logger.error("=" * 80)
            self.logger.error("❌ PIPELINE FAILED")
            self.logger.error("=" * 80)
            self.logger.error(f"Error: {e}")
            self.logger.error(traceback.format_exc())
            
            # Log failure
            self.log_pipeline_metrics(status='FAILED', error_msg=str(e))
            
            return False


def main():
    """Main entry point"""
    pipeline = PipelineOrchestrator()
    success = pipeline.run()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
