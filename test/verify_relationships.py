import logging
from neo4j import GraphDatabase
import networkx as nx
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RelationshipVerifier:
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self.driver.close()
        
    def verify_all_relationships(self):
        """Check all relationships in the graph"""
        with self.driver.session() as session:
            # 1. Get overall statistics
            logger.info("\n=== Graph Statistics ===")
            result = session.run("""
            MATCH (n) 
            RETURN 
                count(n) as node_count,
                size([x IN labels(n) | 1]) as label_count
            """)
            stats = result.single()
            logger.info(f"Total nodes: {stats['node_count']}")
            logger.info(f"Total labels: {stats['label_count']}")
            
            # 2. Check relationship types and counts
            logger.info("\n=== Relationship Types ===")
            result = session.run("""
            MATCH ()-[r]->()
            RETURN type(r) as type, count(r) as count
            ORDER BY count DESC
            """)
            for record in result:
                logger.info(f"{record['type']}: {record['count']} relationships")
                
            # 3. Check node types and their relationships
            logger.info("\n=== Node Types and Their Relationships ===")
            result = session.run("""
            MATCH (n)-[r]->(m)
            RETURN DISTINCT 
                labels(n)[0] as source_type,
                type(r) as relationship,
                labels(m)[0] as target_type,
                count(*) as count
            ORDER BY count DESC
            """)
            for record in result:
                logger.info(f"{record['source_type']} -[{record['relationship']}]-> {record['target_type']} ({record['count']})")
                
            # 4. Check for isolated nodes
            logger.info("\n=== Isolated Nodes ===")
            result = session.run("""
            MATCH (n)
            WHERE NOT (n)--()
            RETURN labels(n)[0] as type, count(*) as count
            """)
            for record in result:
                logger.info(f"Isolated {record['type']}: {record['count']} nodes")
                
    def verify_sentiment_paths(self):
        """Check sentiment analysis paths"""
        logger.info("\n=== Sentiment Analysis Paths ===")
        with self.driver.session() as session:
            result = session.run("""
            MATCH path = (d:Document)-[r1:HAS_SENTIMENT]->(s:Sentiment)
            OPTIONAL MATCH (d)-[r2:BELONGS_TO_SECTOR]->(sec:Sector)
            RETURN d.text as text, 
                   d.sentiment as sentiment_value,
                   s.level as sentiment_level,
                   collect(sec.name) as sectors
            LIMIT 5
            """)
            for record in result:
                logger.info(f"\nDocument: {record['text']}")
                logger.info(f"Sentiment: {record['sentiment_value']} ({record['sentiment_level']})")
                logger.info(f"Sectors: {record['sectors']}")

    def visualize_knowledge_graph(self):
        """Create a visualization of the knowledge graph"""
        G = nx.DiGraph()
        
        with self.driver.session() as session:
            # Get all relationships
            result = session.run("""
            MATCH (n)-[r]->(m)
            RETURN 
                id(n) as source_id,
                labels(n)[0] as source_type,
                type(r) as relationship,
                id(m) as target_id,
                labels(m)[0] as target_type
            """)
            
            for record in result:
                source = f"{record['source_type']}_{record['source_id']}"
                target = f"{record['target_type']}_{record['target_id']}"
                G.add_edge(source, target, relationship=record['relationship'])
        
        # Create visualization
        plt.figure(figsize=(15, 10))
        pos = nx.spring_layout(G)
        
        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_size=2000, node_color='lightblue')
        nx.draw_networkx_edges(G, pos, edge_color='gray', arrows=True)
        
        # Add labels
        nx.draw_networkx_labels(G, pos)
        edge_labels = nx.get_edge_attributes(G, 'relationship')
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
        
        plt.title("Market Knowledge Graph")
        plt.axis('off')
        plt.savefig('knowledge_graph.png')
        logger.info("\nGraph visualization saved as 'knowledge_graph.png'")

def main():
    verifier = RelationshipVerifier()
    try:
        print("\n🔍 Verifying Neo4j Relationships")
        print("==============================")
        
        verifier.verify_all_relationships()
        verifier.verify_sentiment_paths()
        verifier.visualize_knowledge_graph()
        
        print("\n✅ Verification complete!")
        
    finally:
        verifier.close()

if __name__ == "__main__":
    main()
