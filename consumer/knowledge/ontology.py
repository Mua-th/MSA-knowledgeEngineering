import logging
from typing import Dict, List, Optional
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL
import json
import time
from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

class MarketOntology:
    def __init__(self):
        self.g = Graph()
        self.market = Namespace("http://market.org/ontology#")
        self.g.bind("market", self.market)
        
        # Enhanced concept definitions
        self.concepts = {
            'Product': self.market.Product,
            'Brand': self.market.Brand,
            'Sentiment': self.market.Sentiment,
            'MarketTrend': self.market.MarketTrend,
            'Event': self.market.Event,
            'Sector': self.market.Sector,
            'Action': self.market.Action,
            'Consumer': self.market.Consumer,
            'Opinion': self.market.Opinion,
            'MarketIndicator': self.market.MarketIndicator,
            'PriceLevel': self.market.PriceLevel,
            'DemandLevel': self.market.DemandLevel,
            'Entity': self.market.Entity,
            'Analysis': self.market.Analysis
        }
        
        # Action types
        self.action_types = {
            'BRAND_ALERT',
            'MARKET_SIGNAL',
            'TREND_ALERT',
            'SENTIMENT_SHIFT',
            'DEMAND_CHANGE',
            'PRICE_MOVEMENT'
        }
        
        # Define sentiment levels
        self.sentiment_levels = {
            'VeryPositive': 0.8,
            'Positive': 0.3,
            'Neutral': 0.0,
            'Negative': -0.3,
            'VeryNegative': -0.8
        }
        
        self._initialize_ontology()
        
        # Setup Neo4j connection
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password"
        self._driver = GraphDatabase.driver(
            self.neo4j_uri, 
            auth=(self.neo4j_user, self.neo4j_password)
        )

    def _initialize_ontology(self):
        # Define base classes
        for concept in self.concepts.values():
            self.g.add((concept, RDF.type, OWL.Class))

        # Define all relationships
        relationships = [
            (self.market.hasSentiment, 'Product', 'Sentiment'),
            (self.market.influences, 'Event', 'MarketTrend'),
            (self.market.relatedTo, 'Product', 'Product'),
            (self.market.belongsToSector, 'Product', 'Sector'),
            (self.market.hasOpinion, 'Consumer', 'Product'),
            (self.market.indicatesMarketTrend, 'MarketIndicator', 'MarketTrend'),
            (self.market.hasPriceLevel, 'Product', 'PriceLevel'),
            (self.market.hasDemandLevel, 'Product', 'DemandLevel'),
            (self.market.affectsBrand, 'Opinion', 'Brand'),
            (self.market.leadsToAction, 'Sentiment', 'Event'),
            (self.market.hasAction, 'Event', 'Action'),
            (self.market.triggersAction, 'MarketTrend', 'Action'),
            (self.market.requiresAction, 'Sentiment', 'Action'),
            (self.market.analyzedIn, 'Entity', 'Analysis'),
            (self.market.mentionsEntity, 'Event', 'Entity')
        ]

        for rel, domain, range_ in relationships:
            self.g.add((rel, RDF.type, OWL.ObjectProperty))
            self.g.add((rel, RDFS.domain, self.concepts[domain]))
            self.g.add((rel, RDFS.range, self.concepts[range_]))

    def add_market_knowledge(self, subject_type: str, subject_name: str, 
                           predicate: str, object_type: str, object_name: str) -> bool:
        try:
            if object_type == 'Action' and object_name not in self.action_types:
                logger.error(f"Invalid action type: {object_name}")
                return False

            subject = self.market[f"{subject_type}_{subject_name}"]
            object_ = self.market[f"{object_type}_{object_name}"]
            
            self.g.add((subject, RDF.type, self.concepts[subject_type]))
            self.g.add((object_, RDF.type, self.concepts[object_type]))
            self.g.add((subject, self.market[predicate], object_))
            return True
            
        except Exception as e:
            logger.error(f"Error adding market knowledge: {e}")
            return False

    def add_sentiment_knowledge(self, product: str, sentiment_value: float, 
                              confidence: float = 0.0) -> bool:
        try:
            sentiment_level = 'Neutral'
            for level, threshold in self.sentiment_levels.items():
                if sentiment_value >= threshold:
                    sentiment_level = level
                    break
            
            sentiment_id = f"Sentiment_{product}_{int(time.time())}"
            
            # Add basic sentiment relationship
            self.add_market_knowledge(
                "Sentiment", sentiment_id,
                "relatesToProduct",
                "Product", product
            )
            
            # Add sentiment level
            self.add_market_knowledge(
                "Sentiment", sentiment_id,
                "hasSentimentLevel",
                "SentimentLevel", sentiment_level
            )
            
            # Add market implications
            if confidence > 0.7:
                if sentiment_level in ['VeryPositive', 'Positive']:
                    self.add_market_knowledge(
                        "MarketTrend", f"Trend_{product}",
                        "indicatesIncreasedDemand",
                        "Product", product
                    )
                elif sentiment_level in ['VeryNegative', 'Negative']:
                    self.add_market_knowledge(
                        "MarketTrend", f"Trend_{product}",
                        "indicatesDecreasedDemand",
                        "Product", product
                    )
            return True
            
        except Exception as e:
            logger.error(f"Error adding sentiment knowledge: {e}")
            return False

    def add_entity_knowledge(self, entity_type: str, entity_name: str, 
                           properties: Dict) -> bool:
        try:
            entity_id = f"{entity_type}_{entity_name}"
            
            # Add entity node
            self.add_market_knowledge(
                "Entity", entity_id,
                "hasType",
                "EntityType", entity_type
            )
            
            # Add properties
            for prop_name, prop_value in properties.items():
                self.g.add((
                    self.market[entity_id],
                    self.market[f"has{prop_name}"],
                    Literal(prop_value)
                ))
            return True
            
        except Exception as e:
            logger.error(f"Error adding entity knowledge: {e}")
            return False

    def clean_label(self, text: str) -> str:
        cleaned = ''.join(c for c in text if c.isalnum() or c == '_')
        while cleaned and cleaned[0].isdigit():
            cleaned = cleaned[1:]
        return cleaned or 'Default'

    def save_to_neo4j(self) -> bool:
        try:
            with self._driver.session() as session:
                # Clear existing data
                session.run("MATCH (n) DETACH DELETE n")
                
                # Add nodes and relationships
                for s, p, o in self.g:
                    subject_parts = str(s).split('/')[-1].split('#')[-1].split('_')
                    object_parts = str(o).split('/')[-1].split('#')[-1].split('_')
                    
                    subject_type = self.clean_label(subject_parts[0])
                    object_type = self.clean_label(object_parts[0])
                    predicate = self.clean_label(str(p).split('#')[-1])
                    
                    query = """
                    MERGE (s:%s {id: $subject})
                    MERGE (o:%s {id: $object})
                    MERGE (s)-[:%s]->(o)
                    """ % (subject_type, object_type, predicate)
                    
                    try:
                        session.run(query, {
                            'subject': str(s),
                            'object': str(o)
                        })
                    except Exception as e:
                        logger.error(f"Failed to execute query: {query}")
                        logger.error(f"Error: {e}")
                        continue
                        
                return True
                
        except Exception as e:
            logger.error(f"Error saving to Neo4j: {e}")
            return False

    def export_knowledge(self) -> str:
        return self.g.serialize(format='json-ld')

    def close(self):
        if self._driver:
            self._driver.close()