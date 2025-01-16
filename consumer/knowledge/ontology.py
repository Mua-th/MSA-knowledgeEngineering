from typing import Dict, List
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL
import json
from neo4j import GraphDatabase


class MarketOntology:
    def __init__(self):
        self.g = Graph()
        self.market = Namespace("https://market-analyzer.org/ontology/v1#")
        self.g.bind("market", self.market)
        
        # Define concepts first
        self.concepts = {
            'Product': self.market.Product,
            'Brand': self.market.Brand,
            'Sentiment': self.market.Sentiment,
            'MarketTrend': self.market.MarketTrend,
            'Event': self.market.Event,
        }
        
        # Initialize ontology
        self._initialize_ontology()
        
        # Setup Neo4j connection
        self.neo4j_uri = "bolt://localhost:7687"
        self.neo4j_user = "neo4j"
        self.neo4j_password = "password"
        self._driver = GraphDatabase.driver(
            self.neo4j_uri, 
            auth=(self.neo4j_user, self.neo4j_password)
        )

        
    def save_to_neo4j(self):
        try:
            with self._driver.session() as session:
                # Clear existing data
                session.run("MATCH (n) DETACH DELETE n")
                
                # Create constraints
                for concept in self.concepts:
                    session.run(f"CREATE CONSTRAINT IF NOT EXISTS FOR (n:{concept}) REQUIRE n.id IS UNIQUE")

                # Add nodes and relationships
                for s, p, o in self.g:
                    subject_type = str(s).split('/')[-1].split('_')[0]
                    object_type = str(o).split('/')[-1].split('_')[0]
                    predicate = str(p).split('#')[-1]
                    
                    query = """
                    MERGE (s:%s {id: $subject})
                    MERGE (o:%s {id: $object})
                    CREATE (s)-[:%s]->(o)
                    """ % (subject_type, object_type, predicate)
                    
                    session.run(query, {
                        'subject': str(s),
                        'object': str(o)
                    })
                return True
        except Exception as e:
            print(f"Error saving to Neo4j: {e}")
            return False


    def _initialize_ontology(self):
        # Define classes
        for concept in self.concepts.values():
            self.g.add((concept, RDF.type, OWL.Class))

        # Define relationships
        self.g.add((self.market.hasSentiment, RDF.type, OWL.ObjectProperty))
        self.g.add((self.market.influences, RDF.type, OWL.ObjectProperty))
        self.g.add((self.market.relatedTo, RDF.type, OWL.ObjectProperty))

    def add_market_knowledge(self, subject_type: str, subject_name: str, 
                           predicate: str, object_type: str, object_name: str):
        subject = self.market[f"{subject_type}_{subject_name}"]
        object_ = self.market[f"{object_type}_{object_name}"]
        
        self.g.add((subject, RDF.type, self.concepts[subject_type]))
        self.g.add((object_, RDF.type, self.concepts[object_type]))
        self.g.add((subject, self.market[predicate], object_))

    def export_knowledge(self) -> str:
        return self.g.serialize(format='json-ld')
