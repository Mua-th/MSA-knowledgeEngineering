from typing import Dict, List
import json
from datetime import datetime, timedelta

class MarketRuleEngine:
    def __init__(self):
        self.rules = self._load_rules()
        self.historical_data = []

    def _load_rules(self) -> List[Dict]:
        # This would typically load from a configuration file
        return [
            {
                "condition": lambda x: x["sentiment"]["polarity"] < -0.5 and 
                                     x["market_confidence"] < -0.3,
                "action": "ALERT_NEGATIVE_TREND",
                "recommendation": "Consider defensive market position"
            },
            {
                "condition": lambda x: x["sentiment"]["polarity"] > 0.5 and 
                                     len(x["aspects"]["features"]) > 2,
                "action": "OPPORTUNITY_DETECTED",
                "recommendation": "Consider increasing market exposure"
            }
        ]

    def process_analysis(self, analysis_result: Dict) -> List[Dict]:
        recommendations = []
        
        # Store historical data
        self.historical_data.append({
            "timestamp": datetime.now().isoformat(),
            "analysis": analysis_result
        })

        # Apply rules
        for rule in self.rules:
            if rule["condition"](analysis_result):
                recommendations.append({
                    "action": rule["action"],
                    "recommendation": rule["recommendation"],
                    "confidence": self._calculate_confidence(analysis_result),
                    "timestamp": datetime.now().isoformat()
                })

        return recommendations

    def _calculate_confidence(self, analysis: Dict) -> float:
        # Calculate confidence based on historical data and current analysis
        recent_data = [d for d in self.historical_data 
                      if datetime.fromisoformat(d["timestamp"]) > 
                         datetime.now() - timedelta(days=7)]
        
        if not recent_data:
            return 0.5

        sentiment_stability = 1 - abs(
            analysis["sentiment"]["polarity"] - 
            sum(d["analysis"]["sentiment"]["polarity"] for d in recent_data) / len(recent_data)
        )

        return min(1.0, sentiment_stability * 0.7 + len(recent_data) / 100 * 0.3)
