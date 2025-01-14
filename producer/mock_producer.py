import os
from kafka import KafkaProducer
import time
import logging
import sys
import socket

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def create_producer(retries=5, retry_interval=5):
    """Create a Kafka producer with retry mechanism"""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    logger.info(f"Connecting to Kafka at {bootstrap_servers}")
    
    # Add host entry to resolve kafka
    try:
        kafka_ip = socket.gethostbyname('kafka')
        logger.info(f"Resolved kafka to {kafka_ip}")
    except socket.gaierror as e:
        logger.warning(f"Could not resolve kafka: {e}")
    
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                key_serializer=str.encode,
                value_serializer=str.encode,
                security_protocol="PLAINTEXT",
                api_version=(2,8,0),
                reconnect_backoff_ms=1000,
                reconnect_backoff_max_ms=5000,
                request_timeout_ms=30000
            )
            logger.info("Successfully connected to Kafka")
            return producer
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"Failed to connect to Kafka after {retries} attempts: {str(e)}")
                sys.exit(1)
            logger.warning(f"Connection attempt {attempt + 1} failed, retrying in {retry_interval}s...")
            time.sleep(retry_interval)

def main():
    producer = create_producer()
    
    print("Enter messages to send to Kafka. Type 'exit' to quit.")
    topic = os.getenv("KAFKA_TOPIC", "textdata")
    
    try:
        while True:
            logger.info("Waiting for user input for key")
            logger.debug("Prompting user for key input")
            key = input("Enter key (or 'exit' to quit): ")
            if key.lower() == 'exit':
                break
                
            logger.info(f"User entered key: {key}")
            logger.debug("Prompting user for value input")
            value = input("Enter value: ")
            if value.lower() == 'exit':
                break

            logger.info(f"Sending message to topic {topic} with key {key} and value {value}")
            future = producer.send(topic, key=key, value=value)
            try:
                record_metadata = future.get(timeout=10)
                logger.info(f"Message sent - Topic: {record_metadata.topic}, "
                          f"Partition: {record_metadata.partition}, "
                          f"Offset: {record_metadata.offset}")
            except Exception as e:
                logger.error(f"Failed to send message: {str(e)}")

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close(timeout=5)
        logger.info("Producer closed.")

if __name__ == "__main__":
    main()