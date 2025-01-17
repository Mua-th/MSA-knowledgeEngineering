import logging
from knowledge.ontology import MarketOntology
from analysis.market_analyzer import MarketAnalyzer
from inference.market_rules import MarketRuleEngine
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_complete_system():
    """Test the complete market analysis and knowledge system"""
    
    # Initialize components
    logger.info("Initializing system components...")
    analyzer = MarketAnalyzer()
    rule_engine = MarketRuleEngine()
    ontology = MarketOntology()
    
    # Test data
    test_texts = [
        {
            "text": "Apple's new product launch was incredibly successful. The market response is very positive and sales are exceeding expectations.",
            "expected_sentiment": "positive",
            "product": "iPhone",
            "sector": "tech"
        },
        {
            "text": "Tesla stock drops 5% amid growing concerns about production delays and quality issues.",
            "expected_sentiment": "negative",
            "product": "Tesla",
            "sector": "automotive"
        },
        {
            "text": "Microsoft's cloud services revenue shows strong growth as more companies adopt digital transformation.",
            "expected_sentiment": "positive",
            "product": "Azure",
            "sector": "tech"
        }
    ]
    
    try:
        # Process each test case
        for test_case in test_texts:
            logger.info(f"\nProcessing test case: {test_case['text'][:50]}...")
            
            # 1. Analyze text
            analysis = analyzer.analyze_market_context(test_case['text'])
            logger.info(f"Analysis results: {json.dumps(analysis, indent=2)}")
            
            # Verify required fields exist
            required_fields = ['overall_sentiment', 'market_confidence', 'sectors']
            missing_fields = [f for f in required_fields if f not in analysis]
            if missing_fields:
                logger.error(f"Missing required fields: {missing_fields}")
                continue
            
            # 2. Apply rules with error handling
            try:
                recommendations = rule_engine.process_analysis(analysis)
                logger.info(f"Rule recommendations: {json.dumps(recommendations, indent=2)}")
            except Exception as e:
                logger.error(f"Error processing rules: {e}")
                continue
            
            # 3. Update ontology with safe sentiment access
            try:
                ontology.add_market_knowledge(
                    subject_type="Product",
                    subject_name=test_case['product'],
                    predicate="belongsToSector",
                    object_type="Sector",
                    object_name=test_case['sector']
                )
                
                # 4. Add sentiment from overall_sentiment
                ontology.add_sentiment_knowledge(
                    product=test_case['product'],
                    sentiment_value=analysis['overall_sentiment'],
                    confidence=analysis.get('market_confidence', 0.5)
                )
            except Exception as e:
                logger.error(f"Error updating ontology: {e}")
                continue
                
        # 5. Save to Neo4j
        logger.info("\nSaving knowledge to Neo4j...")
        if ontology.save_to_neo4j():
            logger.info("✅ Successfully saved to Neo4j")
        else:
            logger.error("❌ Failed to save to Neo4j")
            
        # 6. Export and display knowledge
        logger.info("\nFinal knowledge graph:")
        knowledge = json.loads(ontology.export_knowledge())
        print(json.dumps(knowledge, indent=2))
        
        return True
            
    except Exception as e:
        logger.error(f"Test failed with error: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    print("\n🧪 Testing Market Analysis and Knowledge System")
    print("=============================================")
    
    if test_complete_system():
        print("\n✅ System test completed successfully!")
    else:
        print("\n❌ System test failed!")
