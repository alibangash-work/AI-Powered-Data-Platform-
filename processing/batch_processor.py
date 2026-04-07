"""
Batch Processing Module

This module performs batch processing to transform Bronze layer data to Silver and Gold layers.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, sum, to_date
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
BRONZE_PATH = os.getenv('BRONZE_LAYER_PATH', './data/bronze')
SILVER_PATH = os.getenv('SILVER_LAYER_PATH', './data/silver')
GOLD_PATH = os.getenv('GOLD_LAYER_PATH', './data/gold')

def create_spark_session():
    """
    Create and configure Spark session with Delta support.

    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName("BatchProcessor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.master", os.getenv('SPARK_MASTER_URL', 'local[*]')) \
        .getOrCreate()

def process_to_silver(spark, bronze_path, silver_path):
    """
    Process Bronze data to Silver layer: clean and standardize.

    Args:
        spark (SparkSession): Spark session
        bronze_path (str): Path to Bronze layer
        silver_path (str): Path to Silver layer
    """
    logger.info("Processing Bronze to Silver...")

    bronze_df = spark.read.format("delta").load(bronze_path)

    # Clean data: drop nulls, standardize category, add date column
    silver_df = bronze_df.dropna() \
        .withColumn("category", lower(col("category"))) \
        .withColumn("date", to_date(col("timestamp")))

    # Write to Silver with partitioning by date
    silver_df.write \
        .format("delta") \
        .mode("overwrite") \
        .partitionBy("date") \
        .save(silver_path)

    logger.info(f"Silver layer updated at {silver_path}")

def process_to_gold(spark, silver_path, gold_path):
    """
    Process Silver data to Gold layer: aggregate metrics.

    Args:
        spark (SparkSession): Spark session
        silver_path (str): Path to Silver layer
        gold_path (str): Path to Gold layer
    """
    logger.info("Processing Silver to Gold...")

    silver_df = spark.read.format("delta").load(silver_path)

    # Aggregate: total sales by category
    gold_df = silver_df.groupBy("category") \
        .agg(sum(col("price") * col("quantity")).alias("total_sales"))

    # Write to Gold
    gold_df.write \
        .format("delta") \
        .mode("overwrite") \
        .save(gold_path)

    logger.info(f"Gold layer updated at {gold_path}")

def main():
    """
    Main function to run batch processing.
    """
    try:
        spark = create_spark_session()
        logger.info("Spark session created.")

        process_to_silver(spark, BRONZE_PATH, SILVER_PATH)
        process_to_gold(spark, SILVER_PATH, GOLD_PATH)

        logger.info("Batch processing completed.")

    except Exception as e:
        logger.error(f"Error in batch processor: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()