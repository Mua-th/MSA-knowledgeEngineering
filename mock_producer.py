import os
from kafka import KafkaProducer, errors
import time
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

def create_producer(retries=10, retry_interval=5):
    """Create a Kafka producer with retry mechanism"""
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                api_version=(0, 10, 1),
                request_timeout_ms=5000,
                value_serializer=lambda x: str(x).encode('utf-8')
            )
            logger.info("Successfully connected to Kafka broker")
            return producer
        except errors.NoBrokersAvailable as e:
            if attempt == retries - 1:
                logger.error(f"Failed to connect to Kafka after {retries} attempts: {str(e)}")
                sys.exit(1)
            logger.warning(f"No Kafka brokers available, retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)

def main():
    producer = create_producer()
    
    print("Enter messages to send to Kafka. Type 'exit' to quit.")
    
    try:
        while True:
            key = input("Enter key: ")
            if key.lower() == 'exit':
                break
            value = input("Enter value: ")
            if value.lower() == 'exit':
                break

            producer.send('text_data', key=key.encode('utf-8'), value=value.encode('utf-8'))
            logger.info(f"Sent message with key: {key} and value: {value}")
            time.sleep(1)
        producer.flush()
        logger.info("Producer closed.")
    except errors.KafkaError as e:
        logger.error(f"Error producing message: {str(e)}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()