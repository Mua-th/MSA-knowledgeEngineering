import os
import time
import logging
from typing import List, Dict
from datetime import datetime
from kafka import KafkaConsumer
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
import spacy
from textblob import TextBlob
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

class TextAnalyzer:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.cache = redis.Redis(host='redis', port=6379, db=0)

    @lru_cache(maxsize=1000)
    def analyze_text(self, text: str) -> Dict:
        # Check cache first
        cache_key = f"text_analysis_{hash(text)}"
        cached_result = self.cache.get(cache_key)
        if cached_result:
            return json.loads(cached_result)

        doc = self.nlp(text)
        blob = TextBlob(text)

        analysis = {
            'keywords': [token.lemma_ for token in doc if token.is_alpha and not token.is_stop],
            'entities': [(ent.text, ent.label_) for ent in doc.ents],
            'sentiment': {
                'polarity': blob.sentiment.polarity,
                'subjectivity': blob.sentiment.subjectivity
            },
            'summary': ' '.join([sent.text for sent in doc.sents][:2]),
            'word_count': len([token for token in doc if token.is_alpha]),
            'processed_at': datetime.now().isoformat()
        }

        # Cache the result
        self.cache.setex(cache_key, 3600, json.dumps(analysis))  # Cache for 1 hour
        return analysis

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

# def create_semantic_network(document_id, keywords):
#     # Create nodes for each keyword
#     for keyword in keywords:
#         conn.query("MERGE (k:Keyword {word: $word})", parameters={"word": keyword})

#     # Create relationships between keywords
#     for i in range(len(keywords)):
#         for j in range(i + 1, len(keywords)):
#             conn.query("""
#             MATCH (k1:Keyword {word: $word1}), (k2:Keyword {word: $word2})
#             MERGE (k1)-[:CO_OCCURS_WITH]->(k2)
#             """, parameters={"word1": keywords[i], "word2": keywords[j]})

def process_batch(batch: MessageBatch, conn: Neo4jConnection, analyzer: MarketAnalyzer, rule_engine: MarketRuleEngine, ontology: MarketOntology):
    if not batch.messages:
        return

    logger.info(f"Processing batch of {len(batch.messages)} messages")
    batch_data = []
    analysis_results = []

    for message in batch.messages:
        try:
            text = message.value.decode('utf-8')
            doc_id = message.key.decode('utf-8') if message.key else str(message.offset)
            
            logger.info(f"Processing message {doc_id}: {text[:100]}...")
            
            # Perform text analysis
            analysis = analyzer.analyze_market_context(text)
            
            logger.debug(f"Analysis result for {doc_id}: {json.dumps(analysis, indent=2)}")
            
            batch_data.append({
                'id': doc_id,
                'properties': {
                    'text': text,
                    'created_at': datetime.now().isoformat(),
                    'sentiment': analysis.get('overall_sentiment', 0),
                    'word_count': len(text.split())
                },
                'keywords': analysis.get('sectors', {}).keys()  # Use sectors as keywords
            })

            analysis_results.append({
                'id': doc_id,
                'sentiment': analysis['overall_sentiment'],
                'entity_count': len(analysis['entities']),
                'processed_at': analysis['processed_at']
            })

            # Process analysis results with rules
            recommendations = rule_engine.process_analysis(analysis)
            for recommendation in recommendations:
                ontology.add_market_knowledge(
                    subject_type="Event",
                    subject_name=f"MarketAnalysis_{doc_id}",
                    predicate="hasRecommendation",
                    object_type="Recommendation",
                    object_name=recommendation["action"]
                )

        except Exception as e:
            logger.error(f"Error processing message: {e}")
            continue

    try:
        conn.batch_create_nodes(batch_data)
        conn.create_semantic_relationships(analysis_results)
        logger.info(f"Successfully processed batch of {len(batch_data)} messages")
    except Exception as e:
        logger.error(f"Error writing to Neo4j: {e}")

    batch.clear()

def main():
    logger.setLevel(logging.INFO)  # Set to DEBUG for more detailed logs
    analyzer = MarketAnalyzer()
    rule_engine = MarketRuleEngine()
    ontology = MarketOntology()
    batch = MessageBatch(max_size=1)  # Process one message at a time for testing
    
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