import logging
from typing import List, Dict
from datetime import datetime

logger = logging.getLogger(__name__)

def prepare_batch_data(messages: List, analyzer, rule_engine) -> List[Dict]:
    """Prepare data for Neo4j batch creation"""
    batch_data = []
    
    for message in messages:
        try:
            text = message.value.decode('utf-8')
            doc_id = message.key.decode('utf-8') if message.key else str(message.offset)
            
            # Analyze text with validation
            analysis = analyzer.analyze_market_context(text)
            if not analysis or not analyzer._validate_analysis(analysis):
                logger.warning(f"Invalid analysis for document {doc_id}")
                continue
            
            # Get recommendations
            recommendations = rule_engine.process_analysis(analysis)
            
            # Prepare entity relationships
            entity_rels = []
            for entity in analysis['entities']:
                entity_rels.append({
                    'text': entity['text'],
                    'type': entity['type'],
                    'confidence': entity['confidence']
                })
            
            # Prepare sector relationships with related entities
            sector_rels = []
            for sector, data in analysis['sectors'].items():
                related_entity = next(
                    (e['text'] for e in analysis['entities'] 
                     if any(m in text[s:e] for s,e in e['mentions'])),
                    None
                )
                sector_rels.append({
                    'name': sector,
                    'confidence': data['confidence'],
                    'related_entity': related_entity
                })
            
            # Create batch item
            batch_item = {
                'id': analysis['document_id'],
                'properties': {
                    'text': text,
                    'created_at': analysis['timestamp'],
                    'sentiment': analysis['sentiment']['score'],
                    'sentiment_confidence': analysis['sentiment']['magnitude'],
                    'market_confidence': analysis['market_confidence']
                },
                'entities': entity_rels,
                'sectors': sector_rels,
                'trends': analysis['trends'],
                'keywords': analysis['keywords']
            }
            
            batch_data.append(batch_item)
            
        except Exception as e:
            logger.error(f"Error preparing batch item: {e}", exc_info=True)
            continue
            
    return batch_data
