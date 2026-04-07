"""
Streaming Processing Module

This module uses Spark Structured Streaming to consume data from Kafka and write to the Bronze layer Delta table.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'transactions'
BRONZE_PATH = os.getenv('BRONZE_LAYER_PATH', './data/bronze')
CHECKPOINT_PATH = os.path.join(BRONZE_PATH, '_checkpoints')

def create_spark_session():
    """
    Create and configure Spark session with Delta support.

    Returns:
        SparkSession: Configured Spark session
    """
    return SparkSession.builder \
        .appName("StreamingProcessor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.master", os.getenv('SPARK_MASTER_URL', 'local[*]')) \
        .getOrCreate()

def define_schema():
    """
    Define the schema for transaction data.

    Returns:
        StructType: Schema for the data
    """
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])

def main():
    """
    Main function to run the streaming processor.
    """
    try:
        spark = create_spark_session()
        logger.info("Spark session created.")

        schema = define_schema()

        # Read from Kafka
        df = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
            .option("subscribe", TOPIC) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON value
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Write to Bronze Delta table
        query = parsed_df.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", CHECKPOINT_PATH) \
            .start(BRONZE_PATH)

        logger.info(f"Streaming query started. Writing to {BRONZE_PATH}")
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in streaming processor: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()