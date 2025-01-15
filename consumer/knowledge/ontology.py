from typing import Dict, List
from rdflib import Graph, Namespace, Literal, URIRef
from rdflib.namespace import RDF, RDFS, OWL
import json

class MarketOntology:
    def __init__(self):
        self.g = Graph()
        self.market = Namespace("http://example.org/market#")
        self.g.bind("market", self.market)

        # Define main concepts
        self.concepts = {
            'Product': self.market.Product,
            'Brand': self.market.Brand,
            'Sentiment': self.market.Sentiment,
            'MarketTrend': self.market.MarketTrend,
            'Event': self.market.Event,
        }

        self._initialize_ontology()

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
