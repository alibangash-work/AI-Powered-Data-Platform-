"""
RAG System - Build Embeddings

This module reads data from the Gold layer, generates embeddings using OpenAI, and stores them in FAISS.
"""

import os
from pyspark.sql import SparkSession
from langchain.vectorstores import FAISS
from langchain.embeddings import OpenAIEmbeddings
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
GOLD_PATH = os.getenv('GOLD_LAYER_PATH', './data/gold')
FAISS_INDEX_PATH = os.getenv('FAISS_INDEX_PATH', './rag/faiss_index')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

def create_spark_session():
    """
    Create Spark session for reading Delta tables.

    Returns:
        SparkSession: Spark session
    """
    return SparkSession.builder \
        .appName("BuildEmbeddings") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.master", "local[*]") \
        .getOrCreate()

def generate_texts_from_gold(gold_df):
    """
    Generate text summaries from Gold layer data.

    Args:
        gold_df (DataFrame): Gold layer DataFrame

    Returns:
        list: List of text strings
    """
    texts = []
    for row in gold_df.collect():
        text = f"Total sales for category {row.category} is {row.total_sales:.2f}"
        texts.append(text)
    return texts

def build_and_save_embeddings(texts, index_path):
    """
    Build FAISS index from texts and save locally.

    Args:
        texts (list): List of text strings
        index_path (str): Path to save FAISS index
    """
    if not OPENAI_API_KEY:
        raise ValueError("OPENAI_API_KEY not set")

    embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
    vectorstore = FAISS.from_texts(texts, embeddings)
    vectorstore.save_local(index_path)
    logger.info(f"FAISS index saved to {index_path}")

def main():
    """
    Main function to build embeddings.
    """
    try:
        spark = create_spark_session()
        logger.info("Spark session created.")

        # Read Gold layer
        gold_df = spark.read.format("delta").load(GOLD_PATH)
        logger.info("Gold layer data loaded.")

        # Generate texts
        texts = generate_texts_from_gold(gold_df)
        logger.info(f"Generated {len(texts)} text summaries.")

        # Build and save embeddings
        build_and_save_embeddings(texts, FAISS_INDEX_PATH)

        logger.info("Embeddings built and saved.")

    except Exception as e:
        logger.error(f"Error building embeddings: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()