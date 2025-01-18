import os
from kafka import KafkaProducer
import time
import logging
import sys
import socket
import json
import random
from market_data_generator import MarketDataGenerator

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

def generate_text_from_record(record):
    """Convert market data record to natural text"""
    templates = [
        "{company} {event_type} shows {trend} signals with sentiment {sentiment}. Related companies: {related}.",
        "Market Update: {company} in {sector} sector {event_type}. Sentiment is {sentiment_desc} at {sentiment}.",
        "Latest from {sector}: {company} announces {event_type}. Market trending {trend} with {sentiment_desc} outlook.",
        "{event_type} for {company} indicates {sentiment_desc} market response. Volume at {volume}.",
        "{company} {event_type} impacts {related}. Market sentiment {sentiment_desc} with {trend} indicators."
    ]
    
    # Get sentiment description
    sentiment_val = record['sentiment']['score']
    if sentiment_val >= 0.6:
        sentiment_desc = "very positive"
    elif sentiment_val >= 0.2:
        sentiment_desc = "positive"
    elif sentiment_val >= -0.2:
        sentiment_desc = "neutral"
    elif sentiment_val >= -0.6:
        sentiment_desc = "negative"
    else:
        sentiment_desc = "very negative"

    text = random.choice(templates).format(
        company=record['company'],
        event_type=record['event_type'].lower(),
        trend=record['market_indicators']['trend'].lower(),
        sentiment=round(record['sentiment']['score'], 2),
        sentiment_desc=sentiment_desc,
        sector=record['sector'],
        related=", ".join(record['related_entities']),
        volume=format(record['market_indicators']['volume'], ",")
    )
    
    return text

def main():
    producer = create_producer()
    generator = MarketDataGenerator()
    topic = os.getenv("KAFKA_TOPIC", "textdata")
    batch_size = int(os.getenv("BATCH_SIZE", "100"))
    interval = float(os.getenv("INTERVAL_SECONDS", "1.0"))
    
    logger.info(f"Starting automated text data generation: batch_size={batch_size}, interval={interval}s")
    
    try:
        while True:
            # Generate and send a batch of records
            records = generator.generate_batch(batch_size)
            
            for record in records:
                try:
                    # Convert market data to text
                    text_data = generate_text_from_record(record)
                    
                    # Send text data
                    future = producer.send(topic, value=text_data)
                    record_metadata = future.get(timeout=10)
                    logger.info(f"Sent text data for {record['company']} - "
                              f"Topic: {record_metadata.topic}, "
                              f"Partition: {record_metadata.partition}, "
                              f"Offset: {record_metadata.offset}")
                    logger.info(f"Text: {text_data}")
                    time.sleep(interval)
                    
                except Exception as e:
                    logger.error(f"Failed to send message: {str(e)}")
            
            producer.flush()
            logger.info(f"Batch complete - {batch_size} records sent")

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.close(timeout=5)
        logger.info("Producer closed.")

if __name__ == "__main__":
    main()