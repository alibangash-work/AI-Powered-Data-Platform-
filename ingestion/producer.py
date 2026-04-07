"""
Data Ingestion Module

This module simulates real-time e-commerce transaction data and produces it to Kafka topics.
"""

import json
import time
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
TOPIC = 'transactions'

# Initialize Faker
fake = Faker()

def generate_transaction():
    """
    Generate a fake e-commerce transaction.

    Returns:
        dict: Transaction data
    """
    return {
        'user_id': fake.uuid4(),
        'product_id': fake.uuid4(),
        'category': fake.random_element(['electronics', 'clothing', 'books', 'home', 'sports']),
        'price': round(fake.pyfloat(min_value=10.0, max_value=1000.0, right_digits=2), 2),
        'quantity': fake.random_int(min=1, max=5),
        'timestamp': datetime.now().isoformat()
    }

def main():
    """
    Main function to produce transactions to Kafka.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_SERVERS],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info("Kafka producer started. Sending transactions...")

        while True:
            transaction = generate_transaction()
            producer.send(TOPIC, transaction)
            logger.info(f"Sent transaction: {transaction}")
            time.sleep(1)  # Simulate real-time interval

    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    except Exception as e:
        logger.error(f"Error in producer: {e}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()