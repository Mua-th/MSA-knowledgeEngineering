import random
import json
from datetime import datetime, timedelta
import uuid

class MarketDataGenerator:
    def __init__(self):
        self.companies = [
            "Apple", "Microsoft", "Amazon", "Google", "Facebook", 
            "Tesla", "Netflix", "Intel", "AMD", "NVIDIA"
        ]
        
        self.sectors = [
            "Technology", "Healthcare", "Finance", "Energy", 
            "Consumer Goods", "Industrial", "Communications"
        ]
        
        self.events = [
            "Earnings Report", "Product Launch", "Management Change",
            "Merger Announcement", "Market Expansion", "Regulatory News",
            "Industry Award", "Partnership Announcement"
        ]
        
        self.trends = ["Bullish", "Bearish", "Neutral", "Volatile"]
        
        # Base sentiment for companies (will fluctuate around these)
        self.company_sentiment = {
            company: random.uniform(-0.2, 0.8) 
            for company in self.companies
        }

    def generate_record(self):
        # Pick a company and its sector
        company = random.choice(self.companies)
        sector = random.choice(self.sectors)
        
        # Get base sentiment and add some random variation
        base_sentiment = self.company_sentiment[company]
        sentiment_variation = random.uniform(-0.3, 0.3)
        sentiment = max(min(base_sentiment + sentiment_variation, 1.0), -1.0)
        
        # Update base sentiment with small drift
        self.company_sentiment[company] += random.uniform(-0.1, 0.1)
        self.company_sentiment[company] = max(min(self.company_sentiment[company], 0.9), -0.9)

        # Generate related entities
        related_entities = random.sample(self.companies, random.randint(1, 3))
        related_entities.remove(company) if company in related_entities else None

        # Create event
        event = {
            "id": str(uuid.uuid4()),
            "timestamp": datetime.now().isoformat(),
            "company": company,
            "sector": sector,
            "event_type": random.choice(self.events),
            "sentiment": {
                "score": round(sentiment, 3),
                "confidence": round(random.uniform(0.5, 0.95), 2)
            },
            "market_indicators": {
                "trend": random.choice(self.trends),
                "volatility": round(random.uniform(0, 1), 2),
                "volume": random.randint(10000, 1000000)
            },
            "related_entities": related_entities,
            "metadata": {
                "source": "market_data_generator",
                "version": "1.0",
                "reliability_score": round(random.uniform(0.7, 1.0), 2)
            }
        }
        
        return event

    def generate_batch(self, num_records=100):
        return [self.generate_record() for _ in range(num_records)]
