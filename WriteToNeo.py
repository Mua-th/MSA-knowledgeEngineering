from neo4j import GraphDatabase
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Neo4jConnection:
    def __init__(self, uri, user, password, database="neo4j"):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database

    def close(self):
        if self._driver:
            self._driver.close()

    def query(self, query, parameters=None):
        logger.info(f"Executing query: {query} with parameters: {parameters}")
        with self._driver.session(database=self.database) as session:
            result = session.run(query, parameters)
            return [record for record in result]

# Connect to Neo4j
uri = "bolt://localhost:7687"
user = "neo4j"
password = "password"
database = "neo4j"

conn = Neo4jConnection(uri, user, password, database)

# Example text document
document_id = "doc1"
text = "This is a samp."

# Create Document node
try:
    conn.query("CREATE (d:Document {id: $id, text: $text})", parameters={"id": document_id, "text": text})
    logger.info(f"Document node created with id: {document_id}")
except Exception as e:
    logger.error(f"Failed to create Document node: {e}")

# Close the connection
conn.close()