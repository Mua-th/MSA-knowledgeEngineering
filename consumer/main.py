import os
import time
import logging
from typing import List, Dict
from datetime import datetime
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import json
from collections import defaultdict
import redis
from functools import lru_cache
from analysis.market_analyzer import MarketAnalyzer
from inference.market_rules import MarketRuleEngine
from knowledge.ontology import MarketOntology

# Enhanced logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MessageBatch:
    def __init__(self, max_size=1, max_wait_seconds=30):
        self.messages = []
        self.max_size = max_size
        self.max_wait_seconds = max_wait_seconds
        self.last_process_time = time.time()

    def add(self, message):
        self.messages.append(message)

    def should_process(self):
        return (len(self.messages) >= self.max_size or
                time.time() - self.last_process_time >= self.max_wait_seconds)

    def clear(self):
        self.messages = []
        self.last_process_time = time.time()


class Neo4jConnection:
    def __init__(self, uri, user, password, database="neo4j"):
        self._driver = None
        self.uri = uri
        self.user = user
        self.password = password
        self.database = database
        self.connect()

    def connect(self):
        for _ in range(10):  # Retry up to 10 times
            try:
                self._driver = GraphDatabase.driver(self.uri, auth=(self.user, self.password))
                logger.info("Connected to Neo4j")
                break
            except ServiceUnavailable:
                logger.warning("Neo4j service unavailable, retrying in 5 seconds...")
                time.sleep(5)
        else:
            raise Exception("Failed to connect to Neo4j after multiple attempts")

    def close(self):
        if self._driver:
            self._driver.close()

    def query(self, query, parameters=None):
        logger.info(f"Executing query: {query} with parameters: {parameters}")
        with self._driver.session(database=self.database) as session:
            result = session.run(query, parameters)
            return [record for record in result]

    def batch_create_nodes(self, batch_data: List[Dict]):
        query = """
        UNWIND $batch AS item
        MERGE (d:Document {id: item.id})
        SET d += item.properties
        WITH d, item
        UNWIND item.keywords AS keyword
        MERGE (k:Keyword {word: keyword})
        MERGE (d)-[:CONTAINS]->(k)
        """
        logger.info(f"Executing batch create with {len(batch_data)} items")
        logger.debug(f"Batch data: {json.dumps(batch_data, indent=2)}")
        return self.query(query, parameters={'batch': batch_data})

    def create_semantic_relationships(self, analysis_results: List[Dict]):
        query = """
        UNWIND $results AS result
        MATCH (d:Document {id: result.id})
        SET d.sentiment = result.sentiment,
            d.entity_count = result.entity_count,
            d.processed_at = result.processed_at
        """
        return self.query(query, parameters={'results': analysis_results})

# Connect to Neo4j
uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")  # Changed default to neo4j:7687
user = os.getenv("NEO4J_USER", "neo4j")
password = os.getenv("NEO4J_PASSWORD", "password")
database = os.getenv("NEO4J_DATABASE", "neo4j")

conn = Neo4jConnection(uri, user, password, database)

class KafkaConnection:
    def __init__(self, bootstrap_servers, topic, group_id):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.consumer = None
        self.connect()

    def connect(self):
        for attempt in range(10):  # Retry up to 10 times
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.bootstrap_servers],
                    api_version=(0, 10, 1),
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    group_id=self.group_id
                )
                logger.info("Successfully connected to Kafka")
                break
            except Exception as e:
                logger.warning(f"Failed to connect to Kafka (attempt {attempt + 1}/10): {str(e)}")
                if attempt < 9:  # Don't sleep on the last attempt
                    time.sleep(5)
        else:
            raise Exception("Failed to connect to Kafka after multiple attempts")

    def close(self):
        if self.consumer:
            self.consumer.close()

# Kafka connection setup
bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")  # Changed default to kafka:29092
topic = "textdata"
group_id = 'my-group'

kafka_conn = KafkaConnection(bootstrap_servers, topic, group_id)

def process_batch(batch: MessageBatch, conn: Neo4jConnection, analyzer: MarketAnalyzer, rule_engine: MarketRuleEngine, ontology: MarketOntology):
    if not batch.messages:
        logger.info("No messages to process in batch")
        return

    logger.info(f"Processing batch of {len(batch.messages)} messages")
    batch_data = []
    analysis_results = []

    for message in batch.messages:
        try:
            text = message.value.decode('utf-8')
            doc_id = message.key.decode('utf-8') if message.key else str(message.offset)
            
            logger.debug(f"Processing message {doc_id}: {text[:100]}...")
            
            # Analyze text with validation
            analysis = analyzer.analyze_market_context(text)
            if not analysis:
                logger.error(f"Empty analysis result for message {doc_id}")
                continue

            # Validate required fields
            required_fields = ['overall_sentiment', 'market_confidence', 'keywords', 'sectors']
            if not all(field in analysis for field in required_fields):
                logger.error(f"Missing required fields in analysis: {[f for f in required_fields if f not in analysis]}")
                continue
            
            # Store in Neo4j with safe access
            batch_data.append({
                'id': doc_id,
                'properties': {
                    'text': text,
                    'created_at': datetime.now().isoformat(),
                    'sentiment': analysis.get('overall_sentiment', 0),
                    'market_confidence': analysis.get('market_confidence', 0),
                    'word_count': len(analysis.get('keywords', []))
                },
                'keywords': list(analysis.get('sectors', {}).keys())
            })

            # Process rules with validation
            try:
                recommendations = rule_engine.process_analysis(analysis)
            except Exception as e:
                logger.error(f"Error processing rules: {e}")
                recommendations = []

            # Update ontology with confidence threshold
            for sector, data in analysis.get('sectors', {}).items():
                if data.get('confidence', 0) > 0.3:
                    try:
                        ontology.add_market_knowledge(
                            "Event", f"Analysis_{doc_id}",
                            "relatesToSector",
                            "Sector", sector
                        )
                    except Exception as e:
                        logger.error(f"Error adding sector knowledge: {e}")

            for rec in recommendations:
                try:
                    ontology.add_market_knowledge(
                        "Event", f"Analysis_{doc_id}",
                        "hasRecommendation",
                        "Action", rec.get('action', 'unknown')
                    )
                except Exception as e:
                    logger.error(f"Error adding recommendation: {e}")

        except Exception as e:
            logger.error(f"Error processing message {doc_id}: {e}", exc_info=True)
            continue

    # Batch persist with transaction
    try:
        if batch_data:
            logger.info(f"Persisting {len(batch_data)} records")
            conn.batch_create_nodes(batch_data)
            ontology.save_to_neo4j()
            logger.info("Batch successfully persisted")
    except Exception as e:
        logger.error(f"Error persisting batch: {e}", exc_info=True)

    batch.clear()
def main():
    logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs
    analyzer = MarketAnalyzer()
    rule_engine = MarketRuleEngine()
    ontology = MarketOntology()
    batch = MessageBatch(max_size=3)  # Process one message at a time for testing
    
    kafka_conn = KafkaConnection(bootstrap_servers, topic, group_id)
    
    try:
        logger.info("Starting message consumption...")
        for message in kafka_conn.consumer:
            logger.info(f"Received message: {message.value[:100]}")
            batch.add(message)
            
            if batch.should_process():
                process_batch(batch, conn, analyzer, rule_engine, ontology)
                
    except KeyboardInterrupt:
        logger.info("Gracefully shutting down...")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        process_batch(batch, conn, analyzer, rule_engine, ontology)
        kafka_conn.close()
        conn.close()

if __name__ == "__main__":
    main()