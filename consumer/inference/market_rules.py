import yaml
import logging
from typing import Dict, List
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)

class MarketRuleEngine:
    def __init__(self):
        self.rules = self._load_rules()
        self.historical_data = []

    def _load_rules(self) -> List[Dict]:
        rules_path = os.path.join(os.path.dirname(__file__), '..', 'market_rules.yaml')
        try:
            with open(rules_path, 'r') as f:
                config = yaml.safe_load(f)
                return config['rules']
        except Exception as e:
            logger.error(f"Failed to load rules: {e}")
            return self._get_default_rules()

    def _get_default_rules(self) -> List[Dict]:
        return [
            {
                "name": "Default Positive",
                "condition": {
                    "sentiment_polarity": ">0.5",
                    "confidence": ">0.6"
                },
                "actions": [{
                    "type": "MARKET_SIGNAL",
                    "signal": "POSITIVE"
                }]
            }
        ]

    def _evaluate_condition(self, condition: Dict, analysis: Dict) -> bool:
        try:
            # Get values with safe access
            sentiment = analysis.get('overall_sentiment', 0)  # Changed from nested dict
            confidence = analysis.get('confidence', 0)
            market_confidence = analysis.get('market_confidence', 0)

            # Evaluate conditions
            for key, value in condition.items():
                if key == 'sentiment_polarity':
                    if not self._compare_value(sentiment, value):
                        return False
                elif key == 'confidence':
                    if not self._compare_value(confidence, value):
                        return False
                elif key == 'market_confidence':
                    if not self._compare_value(market_confidence, value):
                        return False

            return True
        except Exception as e:
            logger.error(f"Error evaluating condition: {e}")
            return False

    def _compare_value(self, actual: float, condition: str) -> bool:
        try:
            op = condition[0]
            value = float(condition[1:])
            
            if op == '>':
                return actual > value
            elif op == '<':
                return actual < value
            elif op == '=':
                return abs(actual - value) < 0.01
                
            return False
        except Exception:
            return False

    def process_analysis(self, analysis_result: Dict) -> List[Dict]:
        recommendations = []
        try:
            # Store analysis result in history
            self.historical_data.append({
                "timestamp": datetime.now().isoformat(),
                "analysis": analysis_result
            })

            # Process each rule
            for rule in self.rules:
                if self._evaluate_condition(rule["condition"], analysis_result):
                    # Fix action access - rule["actions"] is a list
                    action = rule["actions"][0] if isinstance(rule["actions"], list) else rule["actions"]
                    
                    recommendations.append({
                        "action": action["type"] if isinstance(action, dict) else action,
                        "confidence": self._calculate_confidence(analysis_result),
                        "timestamp": datetime.now().isoformat()
                    })

        except Exception as e:
            logger.error(f"Error processing rules: {e}")
            return []

        return recommendations

    def _calculate_confidence(self, analysis: Dict) -> float:
        try:
            # Calculate confidence based on historical data and current analysis
            recent_data = [d for d in self.historical_data 
                          if datetime.fromisoformat(d["timestamp"]) > 
                             datetime.now() - timedelta(days=7)]
            
            if not recent_data:
                return 0.5

            # Get sentiment directly from overall_sentiment
            current_sentiment = analysis.get('overall_sentiment', 0)
            historical_sentiments = [
                d['analysis'].get('overall_sentiment', 0) 
                for d in recent_data
            ]
            
            if not historical_sentiments:
                return 0.6  # Default confidence if no historical data
                
            avg_historical = sum(historical_sentiments) / len(historical_sentiments)
            sentiment_stability = 1 - abs(current_sentiment - avg_historical)

            return min(1.0, sentiment_stability * 0.7 + len(recent_data) / 100 * 0.3)
            
        except Exception as e:
            logger.error(f"Error calculating confidence: {e}")
            return 0.5

