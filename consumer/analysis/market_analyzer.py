from typing import Dict, List
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
        # This would typically load from a configuration file
        return {
            "tech": ["software", "hardware", "AI", "cloud"],
            "retail": ["store", "shop", "sales", "consumer"],
            "finance": ["bank", "investment", "trading", "market"]
        }

    def _load_product_terms(self) -> Dict[str, List[str]]:
        return {
            "features": ["quality", "performance", "reliability"],
            "pricing": ["expensive", "cheap", "value", "cost"],
            "service": ["support", "customer service", "assistance"]
        }

    def analyze_market_context(self, text: str) -> Dict:
        doc = self.nlp(text)
        sentiment = self.analyzer.polarity_scores(text)

        # Market sector analysis
        sectors = defaultdict(int)
        for token in doc:
            for sector, terms in self.market_sectors.items():
                if token.text.lower() in terms:
                    sectors[sector] += 1

        # Product aspect analysis
        aspects = defaultdict(lambda: {"count": 0, "sentiment": 0})
        for sentence in doc.sents:
            for aspect, terms in self.product_terms.items():
                if any(term in sentence.text.lower() for term in terms):
                    aspects[aspect]["count"] += 1
                    aspects[aspect]["sentiment"] += self.analyzer.polarity_scores(sentence.text)['compound']

        return {
            "sectors": dict(sectors),
            "aspects": {k: dict(v) for k, v in aspects.items()},
            "overall_sentiment": sentiment['compound'],
            "market_confidence": self._calculate_market_confidence(doc)
        }

    def _calculate_market_confidence(self, doc) -> float:
        confidence_terms = {
            "positive": ["growth", "increase", "improve", "success"],
            "negative": ["decline", "decrease", "fail", "loss"]
        }
        
        score = 0
        for token in doc:
            if token.text.lower() in confidence_terms["positive"]:
                score += 1
            elif token.text.lower() in confidence_terms["negative"]:
                score -= 1
        
        return score / (len(doc) or 1)  # Normalize by document length