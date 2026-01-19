"""
PySpark Data Processor
Processes cryptocurrency data with validations, transformations, and quality checks
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
import logging
from typing import Dict, List, Tuple
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SparkProcessor:
    """
    PySpark processor for cryptocurrency data
    Implements Bronze -> Silver -> Gold medallion architecture
    """
    
    def __init__(self, config: Dict):
        """
        Initialize Spark Processor
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.spark = self._create_spark_session()
        self.validation_rules = config['data_quality']['validation_rules']
        
        logger.info("SparkProcessor initialized successfully")
    
    def _create_spark_session(self) -> SparkSession:
        """Create and configure Spark session"""
        logger.info("Creating Spark session")
        
        spark = SparkSession.builder \
            .appName(self.config['databricks']['spark']['app_name']) \
            .config("spark.sql.shuffle.partitions", 
                   self.config['databricks']['spark']['shuffle_partitions']) \
            .config("spark.sql.adaptive.enabled", 
                   str(self.config['databricks']['spark']['adaptive_enabled']).lower()) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"Spark version: {spark.version}")
        return spark
    
    def define_schema(self) -> StructType:
        """Define schema for cryptocurrency data"""
        return StructType([
            StructField("id", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("name", StringType(), False),
            StructField("current_price", DoubleType(), True),
            StructField("market_cap", LongType(), True),
            StructField("market_cap_rank", IntegerType(), True),
            StructField("total_volume", LongType(), True),
            StructField("high_24h", DoubleType(), True),
            StructField("low_24h", DoubleType(), True),
            StructField("price_change_24h", DoubleType(), True),
            StructField("price_change_percentage_24h", DoubleType(), True),
            StructField("price_change_percentage_7d_in_currency", DoubleType(), True),
            StructField("price_change_percentage_30d_in_currency", DoubleType(), True),
            StructField("circulating_supply", DoubleType(), True),
            StructField("total_supply", DoubleType(), True),
            StructField("max_supply", DoubleType(), True),
            StructField("ath", DoubleType(), True),
            StructField("ath_date", StringType(), True),
            StructField("atl", DoubleType(), True),
            StructField("atl_date", StringType(), True),
            StructField("last_updated", StringType(), True),
            StructField("extracted_at", StringType(), False),
            StructField("source", StringType(), False),
            StructField("extraction_page", IntegerType(), True)
        ])
    
    def load_bronze_data(self, input_path: str) -> DataFrame:
        """
        Load raw data into Bronze layer
        
        Args:
            input_path: Path to raw JSON data
            
        Returns:
            Bronze DataFrame
        """
        logger.info(f"Loading bronze data from: {input_path}")
        
        try:
            schema = self.define_schema()
            
            df = self.spark.read \
                .schema(schema) \
                .option("multiLine", "true") \
                .json(input_path)
            
            # Add bronze layer metadata
            df = df.withColumn("bronze_loaded_at", F.current_timestamp()) \
                   .withColumn("bronze_batch_id", F.lit(datetime.utcnow().strftime('%Y%m%d%H%M%S')))
            
            record_count = df.count()
            logger.info(f"Bronze data loaded: {record_count} records")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to load bronze data: {e}")
            raise
    
    def validate_data(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Apply data quality validations
        
        Args:
            df: Input DataFrame
            
        Returns:
            Tuple of (valid_df, invalid_df)
        """
        logger.info("Starting data validation")
        
        # Initialize validation flag
        df = df.withColumn("is_valid", F.lit(True))
        df = df.withColumn("validation_errors", F.array())
        
        # Check for nulls in critical columns
        for rule in self.validation_rules:
            if rule['type'] == 'not_null':
                for col in rule['columns']:
                    df = df.withColumn(
                        "is_valid",
                        F.when(F.col(col).isNull(), False).otherwise(F.col("is_valid"))
                    )
                    df = df.withColumn(
                        "validation_errors",
                        F.when(
                            F.col(col).isNull(),
                            F.array_union(F.col("validation_errors"), F.array(F.lit(f"{col}_is_null")))
                        ).otherwise(F.col("validation_errors"))
                    )
        
        # Check price range
        df = df.withColumn(
            "is_valid",
            F.when(
                (F.col("current_price") < 0) | (F.col("current_price") > 1000000),
                False
            ).otherwise(F.col("is_valid"))
        )
        
        # Check positive values
        for col in ["market_cap", "total_volume"]:
            df = df.withColumn(
                "is_valid",
                F.when(F.col(col) < 0, False).otherwise(F.col("is_valid"))
            )
        
        # Separate valid and invalid records
        valid_df = df.filter(F.col("is_valid") == True).drop("is_valid", "validation_errors")
        invalid_df = df.filter(F.col("is_valid") == False)
        
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        
        logger.info(f"Validation complete - Valid: {valid_count}, Invalid: {invalid_count}")
        
        return valid_df, invalid_df
    
    def transform_to_silver(self, df: DataFrame) -> DataFrame:
        """
        Transform data to Silver layer (cleaned and enriched)
        
        Args:
            df: Bronze DataFrame
            
        Returns:
            Silver DataFrame
        """
        logger.info("Transforming to Silver layer")
        
        try:
            silver_df = df.select(
                F.col("id").alias("coin_id"),
                F.col("symbol"),
                F.col("name"),
                F.col("current_price"),
                F.col("market_cap"),
                F.col("market_cap_rank"),
                F.col("total_volume"),
                F.col("high_24h"),
                F.col("low_24h"),
                F.col("price_change_24h"),
                F.col("price_change_percentage_24h"),
                F.col("price_change_percentage_7d_in_currency").alias("price_change_pct_7d"),
                F.col("price_change_percentage_30d_in_currency").alias("price_change_pct_30d"),
                F.col("circulating_supply"),
                F.col("total_supply"),
                F.col("max_supply"),
                F.col("ath"),
                F.to_timestamp("ath_date").alias("ath_date"),
                F.col("atl"),
                F.to_timestamp("atl_date").alias("atl_date"),
                F.to_timestamp("last_updated").alias("last_updated"),
                F.to_timestamp("extracted_at").alias("extracted_at")
            )
            
            # Add derived columns
            silver_df = silver_df \
                .withColumn("price_volatility_24h",
                           (F.col("high_24h") - F.col("low_24h")) / F.col("low_24h") * 100) \
                .withColumn("market_cap_category",
                           F.when(F.col("market_cap") > 10000000000, "Large Cap")
                            .when(F.col("market_cap") > 1000000000, "Mid Cap")
                            .otherwise("Small Cap")) \
                .withColumn("volume_to_market_cap_ratio",
                           F.col("total_volume") / F.col("market_cap")) \
                .withColumn("supply_percentage",
                           F.when(F.col("max_supply").isNotNull(),
                                 F.col("circulating_supply") / F.col("max_supply") * 100)
                            .otherwise(None)) \
                .withColumn("distance_from_ath_pct",
                           (F.col("ath") - F.col("current_price")) / F.col("ath") * 100) \
                .withColumn("distance_from_atl_pct",
                           (F.col("current_price") - F.col("atl")) / F.col("atl") * 100)
            
            # Add Silver metadata
            silver_df = silver_df \
                .withColumn("silver_processed_at", F.current_timestamp()) \
                .withColumn("updated_at", F.current_timestamp())
            
            record_count = silver_df.count()
            logger.info(f"Silver transformation complete: {record_count} records")
            
            return silver_df
            
        except Exception as e:
            logger.error(f"Failed to transform to silver: {e}")
            raise
    
    def detect_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies in the data
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with anomaly flags
        """
        logger.info("Detecting anomalies")
        
        # Calculate statistics
        price_threshold = self.config['data_quality']['anomaly_detection']['price_change_threshold']
        
        df = df.withColumn(
            "is_price_anomaly",
            F.when(
                F.abs(F.col("price_change_percentage_24h")) > (price_threshold * 100),
                True
            ).otherwise(False)
        )
        
        # Volume spike detection
        window = Window.partitionBy("market_cap_category")
        
        df = df.withColumn(
            "avg_volume",
            F.avg("total_volume").over(window)
        ).withColumn(
            "stddev_volume",
            F.stddev("total_volume").over(window)
        ).withColumn(
            "is_volume_spike",
            F.when(
                F.col("total_volume") > (F.col("avg_volume") + 3 * F.col("stddev_volume")),
                True
            ).otherwise(False)
        ).drop("avg_volume", "stddev_volume")
        
        anomaly_count = df.filter(
            (F.col("is_price_anomaly") == True) | (F.col("is_volume_spike") == True)
        ).count()
        
        logger.info(f"Anomalies detected: {anomaly_count}")
        
        return df
    
    def transform_to_gold(self, df: DataFrame) -> DataFrame:
        """
        Transform to Gold layer (aggregated metrics)
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Gold DataFrame with aggregated metrics
        """
        logger.info("Transforming to Gold layer")
        
        try:
            # Market overview metrics
            gold_df = df.groupBy("market_cap_category").agg(
                F.count("*").alias("num_coins"),
                F.sum("market_cap").alias("total_market_cap"),
                F.sum("total_volume").alias("total_volume_24h"),
                F.avg("current_price").alias("avg_price"),
                F.avg("price_change_percentage_24h").alias("avg_price_change_24h"),
                F.avg("price_volatility_24h").alias("avg_volatility"),
                F.avg("volume_to_market_cap_ratio").alias("avg_volume_ratio"),
                F.max("market_cap").alias("max_market_cap"),
                F.min("market_cap").alias("min_market_cap")
            )
            
            # Add metadata
            gold_df = gold_df \
                .withColumn("metric_date", F.current_date()) \
                .withColumn("metric_timestamp", F.current_timestamp()) \
                .withColumn("calculation_type", F.lit("market_category_metrics"))
            
            record_count = gold_df.count()
            logger.info(f"Gold transformation complete: {record_count} aggregated records")
            
            return gold_df
            
        except Exception as e:
            logger.error(f"Failed to transform to gold: {e}")
            raise
    
    def write_delta_table(self, df: DataFrame, path: str, mode: str = "append"):
        """
        Write DataFrame to Delta table
        
        Args:
            df: DataFrame to write
            path: Output path
            mode: Write mode (append, overwrite)
        """
        logger.info(f"Writing to Delta table: {path}")
        
        try:
            df.write \
                .format("delta") \
                .mode(mode) \
                .option("overwriteSchema", "true") \
                .save(path)
            
            logger.info("Delta table write completed")
            
        except Exception as e:
            logger.error(f"Failed to write Delta table: {e}")
            raise
    
    def optimize_table(self, path: str):
        """
        Optimize Delta table (compaction and Z-ordering)
        
        Args:
            path: Path to Delta table
        """
        logger.info(f"Optimizing Delta table: {path}")
        
        try:
            self.spark.sql(f"OPTIMIZE delta.`{path}` ZORDER BY (coin_id, updated_at)")
            logger.info("Table optimization completed")
            
        except Exception as e:
            logger.error(f"Failed to optimize table: {e}")
            # Don't raise - optimization is best effort
    
    def stop(self):
        """Stop Spark session"""
        logger.info("Stopping Spark session")
        self.spark.stop()


def main():
    """Main execution function"""
    import yaml
    
    # Load configuration
    with open('../config/config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    # Initialize processor
    processor = SparkProcessor(config)
    
    try:
        # Process data through medallion architecture
        input_path = "../logs/crypto_data_*.json"
        
        # Bronze layer
        bronze_df = processor.load_bronze_data(input_path)
        
        # Validate
        valid_df, invalid_df = processor.validate_data(bronze_df)
        
        # Silver layer
        silver_df = processor.transform_to_silver(valid_df)
        silver_df = processor.detect_anomalies(silver_df)
        
        # Gold layer
        gold_df = processor.transform_to_gold(silver_df)
        
        # Display sample results
        print("\n=== Silver Layer Sample ===")
        silver_df.select("coin_id", "symbol", "current_price", "market_cap_category").show(10)
        
        print("\n=== Gold Layer Metrics ===")
        gold_df.show()
        
        print(f"\n✅ Processing completed successfully")
        
    finally:
        processor.stop()


if __name__ == "__main__":
    main()
