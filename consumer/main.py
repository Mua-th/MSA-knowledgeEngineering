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
        queries = [
            # Create Document nodes
            """
            UNWIND $batch AS item
            MERGE (d:Document {id: item.id})
            SET d += item.properties
            """,
            
            # Create Entity nodes and relationships
            """
            UNWIND $batch AS item
            MATCH (d:Document {id: item.id})
            UNWIND item.entities AS entity
            MERGE (e:Entity {
                id: entity.text,
                type: entity.type
            })
            MERGE (d)-[:MENTIONS {
                confidence: entity.confidence
            }]->(e)
            """,
            
            # Create Sector nodes and relationships
            """
            UNWIND $batch AS item
            MATCH (d:Document {id: item.id})
            UNWIND item.sectors AS sector
            MERGE (s:Sector {name: sector.name})
            MERGE (d)-[:BELONGS_TO_SECTOR {
                confidence: sector.confidence
            }]->(s)
            WITH d, sector, s
            MATCH (e:Entity {id: sector.related_entity})
            MERGE (e)-[:IN_SECTOR]->(s)
            """,
            
            # Create Sentiment nodes and relationships
            """
            UNWIND $batch AS item
            MATCH (d:Document {id: item.id})
            WHERE item.properties.sentiment IS NOT NULL
            MERGE (s:Sentiment {
                level: CASE 
                    WHEN item.properties.sentiment > 0.5 THEN 'POSITIVE'
                    WHEN item.properties.sentiment < -0.5 THEN 'NEGATIVE'
                    ELSE 'NEUTRAL'
                END
            })
            MERGE (d)-[:HAS_SENTIMENT {
                score: item.properties.sentiment,
                confidence: item.properties.sentiment_confidence
            }]->(s)
            """,

            # Create Market Trend nodes and relationships
            """
            UNWIND $batch AS item
            MATCH (d:Document {id: item.id})
            UNWIND item.trends AS trend
            MERGE (t:MarketTrend {
                type: trend.type,
                direction: trend.direction
            })
            MERGE (d)-[:INDICATES_TREND {
                confidence: trend.confidence,
                timestamp: timestamp()
            }]->(t)
            """
        ]
        
        try:
            for query in queries:
                self.query(query, parameters={'batch': batch_data})
                logger.info(f"Executed query successfully")
                
        except Exception as e:
            logger.error(f"Error in batch creation: {e}")
            raise

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

def process_batch(batch: MessageBatch, conn: Neo4jConnection, analyzer: MarketAnalyzer, 
                 rule_engine: MarketRuleEngine, ontology: MarketOntology):
    batch_data = []
    
    for message in batch.messages:
        try:
            text = message.value.decode('utf-8')
            doc_id = message.key.decode('utf-8') if message.key else str(message.offset)
            
            analysis = analyzer.analyze_market_context(text)
            
            # Document node and base properties
            document_data = {
                'id': doc_id,
                'properties': {
                    'text': text,
                    'created_at': datetime.now().isoformat(),
                    'sentiment': analysis['overall_sentiment'],
                    'market_confidence': analysis['market_confidence'],
                    'word_count': len(analysis['keywords']),
                    'sentiment_confidence': analysis.get('sentiment_confidence', 0.5)
                },
                'keywords': list(analysis.get('sectors', {}).keys()),
                'entities': [
                    {
                        'text': entity[0],
                        'type': entity[1],
                        'confidence': 0.8
                    } for entity in analysis['entities']
                ],
                'sectors': [
                    {
                        'name': sector,
                        'confidence': data['confidence'],
                        'related_entity': next((e[0] for e in analysis['entities'] if e[1] == 'ORG'), None)
                    } for sector, data in analysis['sectors'].items()
                ],
                'sentiment': {
                    'score': analysis['overall_sentiment'],
                    'confidence': analysis.get('sentiment_confidence', 0.5)
                }
            }
            
            batch_data.append(document_data)
        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)

    try:
        if batch_data:
            logger.info(f"Persisting {len(batch_data)} records")
            
            # Create document nodes
            conn.query("""
            UNWIND $batch AS item 
            MERGE (d:Document {id: item.id})
            SET d += item.properties
            """, parameters={'batch': batch_data})
            
            # Create entity relationships
            conn.query("""
            UNWIND $batch AS item 
            MATCH (d:Document {id: item.id})
            UNWIND item.entities AS entity
            MERGE (e:Entity {
                id: entity.text,
                type: entity.type
            })
            MERGE (d)-[:MENTIONS {
                confidence: entity.confidence
            }]->(e)
            """, parameters={'batch': batch_data})
            
            # Create sector relationships
            conn.query("""
            UNWIND $batch AS item 
            MATCH (d:Document {id: item.id})
            UNWIND item.sectors AS sector
            MERGE (s:Sector {name: sector.name})
            MERGE (d)-[:BELONGS_TO_SECTOR {
                confidence: sector.confidence
            }]->(s)
            WITH d, sector, s
            MATCH (e:Entity {id: sector.related_entity})
            MERGE (e)-[:IN_SECTOR]->(s)
            """, parameters={'batch': batch_data})
            
            # Create sentiment relationships
            conn.query("""
            UNWIND $batch AS item 
            MATCH (d:Document {id: item.id})
            WHERE item.properties.sentiment IS NOT NULL
            MERGE (s:Sentiment {
                level: CASE
                    WHEN item.properties.sentiment > 0.5 THEN 'POSITIVE'
                    WHEN item.properties.sentiment < -0.5 THEN 'NEGATIVE'
                    ELSE 'NEUTRAL'
                END
            })
            MERGE (d)-[:HAS_SENTIMENT {
                score: item.properties.sentiment,
                confidence: item.sentiment.confidence
            }]->(s)
            """, parameters={'batch': batch_data})
            
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