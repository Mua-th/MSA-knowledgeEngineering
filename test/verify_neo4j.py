from knowledge.ontology import MarketOntology
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_neo4j_connection():
    try:
        # Create ontology instance
        onto = MarketOntology()
        
        # Add some test data
        test_data = [
            ("Event", "TestEvent1", "influences", "MarketTrend", "Bullish"),
            ("Product", "TestProduct1", "hasSentiment", "Sentiment", "Positive"),
            ("Brand", "TestBrand1", "relatedTo", "Product", "TestProduct1")
        ]
        
        # Add each test entry
        for subject_type, subject_name, predicate, object_type, object_name in test_data:
            onto.add_market_knowledge(
                subject_type=subject_type,
                subject_name=subject_name,
                predicate=predicate,
                object_type=object_type,
                object_name=object_name
            )
            logger.info(f"Added test data: {subject_type}_{subject_name} -{predicate}-> {object_type}_{object_name}")
        
        # Try to save to Neo4j
        logger.info("Attempting to save to Neo4j...")
        success = onto.save_to_neo4j()
        
        if success:
            logger.info("✅ Successfully saved to Neo4j!")
            
            # Verify data in Neo4j
            try:
                with onto._driver.session() as session:
                    # Count nodes
                    result = session.run("MATCH (n) RETURN count(n) as count")
                    node_count = result.single()["count"]
                    
                    # Count relationships
                    result = session.run("MATCH ()-[r]->() RETURN count(r) as count")
                    rel_count = result.single()["count"]
                    
                    logger.info(f"Neo4j contains {node_count} nodes and {rel_count} relationships")
                    
                    # Sample query to show actual data
                    result = session.run("""
                    MATCH (n)-[r]->(m)
                    RETURN n.id as source, type(r) as relationship, m.id as target
                    LIMIT 5
                    """)
                    
                    logger.info("Sample of relationships in Neo4j:")
                    for record in result:
                        logger.info(f"{record['source']} -{record['relationship']}-> {record['target']}")
                        
            except Exception as e:
                logger.error(f"Error verifying data: {e}")
        else:
            logger.error("❌ Failed to save to Neo4j")
            
    except Exception as e:
        logger.error(f"❌ Test failed with error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    if test_neo4j_connection():
        print("\n✅ Neo4j connection test passed!")
    else:
        print("\n❌ Neo4j connection test failed!")
