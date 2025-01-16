from typing import Dict, List
from datetime import datetime
import spacy
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from collections import defaultdict

class MarketAnalyzer:
    def __init__(self):
        self.nlp = spacy.load("en_core_web_sm")
        self.analyzer = SentimentIntensityAnalyzer()
        self.market_sectors = self._load_market_sectors()
        self.product_terms = self._load_product_terms()

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

    def analyze_market_context(self, text: str) -> Dict:
        if not text:
            return self._empty_analysis()

        doc = self.nlp(text)
        sentiment = self.analyzer.polarity_scores(text)

        sectors = self._analyze_sectors(doc)
        aspects = self._analyze_aspects(doc)
        
        analysis = {
            "sectors": dict(sectors),
            "aspects": {k: dict(v) for k, v in aspects.items()},
            "overall_sentiment": sentiment['compound'],
            "market_confidence": self._calculate_market_confidence(doc),
            "entities": [(ent.text, ent.label_) for ent in doc.ents],
            "keywords": [token.text.lower() for token in doc if token.is_alpha and not token.is_stop],
            "summary": ' '.join([sent.text for sent in doc.sents][:2]),
            "processed_at": datetime.now().isoformat()
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

    def _calculate_market_confidence(self, doc) -> float:
        market_terms = sum(1 for token in doc if token.text.lower() in 
                         [term for terms in self.market_sectors.values() for term in terms])
        return min(1.0, market_terms / len(doc))

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