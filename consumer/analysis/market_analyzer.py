from typing import Dict, List, Optional, Tuple
from datetime import datetime
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from collections import defaultdict
import time
import logging

# Configure logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class MarketAnalyzer:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.analyzer = SentimentIntensityAnalyzer()
        self.market_sectors = self._load_market_sectors()
        self.product_terms = self._load_product_terms()
        self.entity_types = {
            'ORG': 'Organization',
            'PRODUCT': 'Product', 
            'EVENT': 'Event',
            'GPE': 'Location'
        }
        self.min_confidence_threshold = 0.3

    def _load_market_sectors(self) -> Dict[str, List[str]]:
        return {
            'tech': ['technology', 'software', 'AI', 'digital', 'tech', 'innovation'],
            'finance': ['banking', 'finance', 'market', 'stocks', 'investment'],
            'retail': ['retail', 'sales', 'consumer', 'shopping', 'ecommerce'],
            'auto': ['automotive', 'cars', 'vehicles', 'ev', 'electric vehicle'],
            'energy': ['energy', 'oil', 'gas', 'renewable', 'power']
        }

    def _load_product_terms(self) -> Dict[str, List[str]]:
        return {
            'price': ['price', 'cost', 'value', 'expensive', 'cheap'],
            'quality': ['quality', 'performance', 'reliability', 'durability'],
            'features': ['feature', 'functionality', 'capability', 'specification'],
            'service': ['service', 'support', 'customer service', 'maintenance']
        }

    def validate_entity(self, entity: spacy.tokens.Span) -> Optional[Tuple[str, str, float]]:
        """Validate and normalize entity"""
        if entity.label_ not in self.entity_types:
            return None
            
        confidence = entity._.confidence if hasattr(entity._, 'confidence') else 0.5
        if confidence < self.min_confidence_threshold:
            return None
            
        return (
            entity.text,
            self.entity_types[entity.label_],
            confidence
        )

    def analyze_market_context(self, text: str) -> Dict:
        if not text:
            return self._empty_analysis()

        doc = self.nlp(text)
        sentiment = self.analyzer.polarity_scores(text)

        # Enhanced sector detection for entertainment
        sectors = defaultdict(lambda: {"count": 0, "confidence": 0})
        entertainment_terms = ['streaming', 'subscriber', 'entertainment']
        
        for token in doc:
            token_text = token.text.lower()
            # Check entertainment sector
            if token_text in entertainment_terms:
                sectors['entertainment']["count"] += 1
                sectors['entertainment']["confidence"] = sectors['entertainment']["count"] / len(doc)

        analysis = {
            "sectors": dict(sectors),
            "overall_sentiment": sentiment['compound'],
            "market_confidence": self._calculate_market_confidence(doc),
            "entities": [(ent.text, self.entity_types.get(ent.label_, 'OTHER')) 
                        for ent in doc.ents if ent.label_ in self.entity_types],
            "keywords": [token.text.lower() for token in doc 
                        if token.is_alpha and not token.is_stop],
            "sentiment_confidence": abs(sentiment['compound'])
        }

        return analysis

    def _analyze_sectors(self, doc) -> Dict:
        sectors = defaultdict(lambda: {"count": 0, "confidence": 0})
        for token in doc:
            for sector, terms in self.market_sectors.items():
                if token.text.lower() in terms:
                    sectors[sector]["count"] += 1
                    sectors[sector]["confidence"] = sectors[sector]["count"] / len(doc)
        return sectors

    def _analyze_aspects(self, doc) -> Dict:
        aspects = defaultdict(lambda: {"count": 0, "sentiment": 0, "mentions": []})
        for sent in doc.sents:
            for aspect, terms in self.product_terms.items():
                if any(term in sent.text.lower() for term in terms):
                    sent_sentiment = self.analyzer.polarity_scores(sent.text)['compound']
                    aspects[aspect]["count"] += 1
                    aspects[aspect]["sentiment"] += sent_sentiment
                    aspects[aspect]["mentions"].append({
                        "text": sent.text,
                        "sentiment": sent_sentiment
                    })
        return aspects

    def _analyze_market_trends(self, doc, sentiment_score: float) -> List[Dict]:
        """Analyze market trends from text"""
        trends = []
        
        # Price trend indicators
        price_indicators = ['increase', 'decrease', 'rise', 'fall', 'grew', 'dropped']
        
        # Demand trend indicators  
        demand_indicators = ['demand', 'popular', 'adoption', 'sales', 'revenue']
        
        for sent in doc.sents:
            sent_text = sent.text.lower()
            
            # Check for price trends
            if any(ind in sent_text for ind in price_indicators):
                trends.append({
                    'type': 'PRICE_TREND',
                    'direction': 'UP' if sentiment_score > 0 else 'DOWN',
                    'confidence': abs(sentiment_score),
                    'text': sent.text
                })
                
            # Check for demand trends    
            if any(ind in sent_text for ind in demand_indicators):
                trends.append({
                    'type': 'DEMAND_TREND',
                    'direction': 'UP' if sentiment_score > 0 else 'DOWN', 
                    'confidence': abs(sentiment_score),
                    'text': sent.text
                })

        return trends

    def _calculate_market_confidence(self, doc) -> float:
        # Enhanced confidence calculation
        market_terms = set()
        sector_matches = defaultdict(int)
        
        for token in doc:
            token_text = token.text.lower()
            for sector, terms in self.market_sectors.items():
                if token_text in terms:
                    market_terms.add(token_text)
                    sector_matches[sector] += 1
        
        # Calculate confidence factors
        term_coverage = len(market_terms) / len(doc)
        sector_strength = max(sector_matches.values()) / len(doc) if sector_matches else 0
        entity_presence = len([e for e in doc.ents if e.label_ in ['ORG', 'PRODUCT']]) > 0
        
        # Weighted confidence score
        confidence = (
            term_coverage * 0.4 +
            sector_strength * 0.4 +
            entity_presence * 0.2
        )
        
        return min(1.0, confidence * 1.5)  # Scale up slightly to match expected range

    def _validate_analysis(self, analysis: Dict) -> bool:
        """Validate analysis results"""
        try:
            # Required fields validation
            required_fields = ['document_id', 'entities', 'sectors', 'sentiment']
            if not all(field in analysis for field in required_fields):
                logger.error(f"Missing required fields in analysis: {[f for f in required_fields if f not in analysis]}")
                return False
                
            # Entity validation
            if not analysis['entities']:
                logger.warning("No valid entities found in text")
                return False
                
            # Sentiment validation
            if not analysis['sentiment']['is_valid']:
                logger.warning("Sentiment score below threshold")
                return False
                
            # Sector validation
            if not analysis['sectors']:
                logger.warning("No sectors identified")
                return False
                
            return True
            
        except Exception as e:
            logger.error(f"Analysis validation error: {e}")
            return False

    def _empty_analysis(self) -> Dict:
        return {
            "sectors": {},
            "aspects": {},
            "overall_sentiment": 0,
            "market_confidence": 0,
            "entities": [],
            "keywords": [],
            "summary": "",
            "processed_at": datetime.now().isoformat()
        }