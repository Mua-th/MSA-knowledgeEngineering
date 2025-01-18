import os
import time
import logging
from typing import List, Dict
from datetime import datetime
import uuid  # Add this import
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

    def initialize_schema(self):
        """Initialize Neo4j constraints and indexes"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Document) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (e:Entity) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sector) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:MarketTrend) REQUIRE t.type IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Sentiment) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:Action) REQUIRE a.type IS UNIQUE"
        ]
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS FOR (e:Entity) ON (e.type)",
            "CREATE INDEX IF NOT EXISTS FOR (d:Document) ON (d.timestamp)",
            "CREATE INDEX IF NOT EXISTS FOR (s:Sentiment) ON (s.level)"
        ]
        
        for query in constraints + indexes:
            self.query(query)

    def validate_graph(self):
        """Validate graph structure and relationships"""
        validation_queries = [
            # Check node counts
            """
            MATCH (n) 
            RETURN labels(n) as type, count(*) as count
            """,
            
            # Check relationship types
            """
            MATCH ()-[r]->() 
            RETURN type(r) as type, count(*) as count
            """,
            
            # Verify entity-sector connections
            """
            MATCH (e:Entity)-[r:IN_SECTOR]->(s:Sector)
            RETURN e.id, s.name, r.confidence
            """
        ]
        
        results = {}
        for query in validation_queries:
            results[query] = self.query(query)
        return results

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
    
    # Add mappings
    COMPANY_SECTORS = {
        'Amazon': 'Technology',
        'Microsoft': 'Technology',
        'NVIDIA': 'Technology',
        'Intel': 'Technology',
        'Netflix': 'Communications',
        'AMD': 'Technology',
        'Apple': 'Technology',
        'Tesla': 'Automotive',
        'Google': 'Technology',
        'Facebook': 'Communications'
    }
    
    ACTION_KEYWORDS = {
        'launch': 'PRODUCT_LAUNCH',
        'merger': 'MERGER',
        'acquisition': 'ACQUISITION',
        'expansion': 'EXPANSION',
        'award': 'AWARD',
        'change': 'MANAGEMENT_CHANGE',
        'earnings': 'EARNINGS_REPORT',
        'regulatory': 'REGULATORY_NEWS'
    }
    
    for message in batch.messages:
        try:
            text = message.value.decode('utf-8')
            doc_id = f"doc_{int(time.time())}_{uuid.uuid4().hex[:8]}"
            
            # Extract companies mentioned in text
            companies = [company for company in COMPANY_SECTORS.keys() 
                       if company in text]
            
            # Calculate simple sentiment
            positive_words = ['positive', 'very positive', 'bullish']
            negative_words = ['negative', 'very negative', 'bearish']
            
            sentiment_score = 0
            for word in positive_words:
                if word in text.lower():
                    sentiment_score += 0.3
            for word in negative_words:
                if word in text.lower():
                    sentiment_score -= 0.3
            
            # Create Document node
            create_doc_query = """
            CREATE (d:Document {
                id: $id,
                timestamp: $timestamp,
                text: $text,
                sentiment_score: $sentiment_score,
                confidence: $confidence
            })
            RETURN d
            """
            
            conn.query(create_doc_query, {
                'id': doc_id,
                'timestamp': datetime.now().isoformat(),
                'text': text,
                'sentiment_score': sentiment_score,
                'confidence': 0.8
            })

            # Create and link entities and sectors
            entity_sector_query = """
            MATCH (d:Document {id: $doc_id})
            MERGE (e:Entity {name: $entity_name})
            ON CREATE SET e.type = 'Company'
            MERGE (s:Sector {name: $sector_name})
            
            MERGE (d)-[m:MENTIONS]->(e)
            ON CREATE SET m.confidence = $confidence
            
            MERGE (e)-[is:IN_SECTOR]->(s)
            ON CREATE SET is.confidence = $confidence
            """
            
            # Process each company found in text
            for company in companies:
                conn.query(entity_sector_query, {
                    'doc_id': doc_id,
                    'entity_name': company,
                    'sector_name': COMPANY_SECTORS[company],
                    'confidence': 0.8
                })

            # Create and link sentiment
            sentiment_query = """
            MATCH (d:Document {id: $doc_id})
            CREATE (s:Sentiment {
                level: $level,
                score: $score,
                id: $sentiment_id
            })
            CREATE (d)-[r:HAS_SENTIMENT {confidence: $confidence}]->(s)
            """

            sentiment_level = 'POSITIVE' if sentiment_score > 0.3 else 'NEGATIVE' if sentiment_score < -0.3 else 'NEUTRAL'
            
            conn.query(sentiment_query, {
                'doc_id': doc_id,
                'level': sentiment_level,
                'score': sentiment_score,
                'sentiment_id': f"sentiment_{doc_id}",
                'confidence': 0.8
            })

            # Process actions
            text_lower = text.lower()
            for keyword, action_type in ACTION_KEYWORDS.items():
                if keyword in text_lower:
                    # Find the company performing the action
                    for company in companies:
                        if company.lower() in text_lower:
                            conn.query("""
                            MATCH (d:Document {id: $doc_id})
                            MERGE (a:Action {type: $action_type})
                            CREATE (d)-[r:HAS_ACTION {
                                confidence: $confidence,
                                timestamp: datetime()
                            }]->(a)
                            WITH d, a
                            MATCH (e:Entity {name: $entity_name})
                            CREATE (e)-[p:PERFORMS {
                                timestamp: datetime(),
                                confidence: $confidence
                            }]->(a)
                            """, {
                                'doc_id': doc_id,
                                'action_type': action_type,
                                'confidence': 0.8,
                                'entity_name': company
                            })
                            
                            # Create impact relationships
                            impacted_companies = [c for c in companies if c != company]
                            if impacted_companies:
                                for impacted in impacted_companies:
                                    conn.query("""
                                    MATCH (a:Action {type: $action_type})
                                    MATCH (e:Entity {name: $target_entity})
                                    MERGE (a)-[i:IMPACTS {
                                        confidence: $confidence,
                                        timestamp: datetime()
                                    }]->(e)
                                    """, {
                                        'action_type': action_type,
                                        'target_entity': impacted,
                                        'confidence': 0.7
                                    })

        except Exception as e:
            logger.error(f"Error processing message: {e}", exc_info=True)
            continue

    batch.clear()

def main():
    logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs
    analyzer = MarketAnalyzer()
    rule_engine = MarketRuleEngine()
    ontology = MarketOntology()
    batch = MessageBatch(max_size=3)  # Process one message at a time for testing
    
    kafka_conn = KafkaConnection(bootstrap_servers, topic, group_id)
    
    # Initialize Neo4j schema
    conn.initialize_schema()
    
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