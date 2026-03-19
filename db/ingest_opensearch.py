"""
Ingest dummy data into local OpenSearch for testing text_match_terms.
"""

from opensearchpy import OpenSearch


def ingest_opensearch():
    """Insert dummy documents and comments into local OpenSearch"""
    
    client = OpenSearch(
        hosts=[{"host": "localhost", "port": 9200}],
        use_ssl=False,
        verify_certs=False,
    )
    
    index_name = "regulations"
    
    # Delete and recreate index
    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
    
    client.indices.create(
        index=index_name,
        body={
            "mappings": {
                "properties": {
                    "docket_id": {"type": "keyword"},
                    "type": {"type": "keyword"},
                    "text": {"type": "text"},
                }
            }
        }
    )
    
    # Dummy data - 2 dockets with documents and comments
    dummy_data = [
        # DEA-2024-0059 - 3 docs, 2 comments with "meaningful use"
        {"docket_id": "DEA-2024-0059", "type": "document", 
         "text": "This document discusses meaningful use criteria"},
        {"docket_id": "DEA-2024-0059", "type": "document", 
         "text": "Additional meaningful use requirements"},
        {"docket_id": "DEA-2024-0059", "type": "document", 
         "text": "Final meaningful use guidelines"},
        {"docket_id": "DEA-2024-0059", "type": "comment", 
         "text": "I support the meaningful use standards"},
        {"docket_id": "DEA-2024-0059", "type": "comment", 
         "text": "The meaningful use criteria seem reasonable"},
        
        # CMS-2025-0240 - 2 docs with "medicare" and "updates", 4 comments with "medicare"
        {"docket_id": "CMS-2025-0240", "type": "document", 
         "text": "Medicare program updates for 2025 including payment changes"},
        {"docket_id": "CMS-2025-0240", "type": "document", 
         "text": "Medicare Advantage plan modifications and updates"},
        {"docket_id": "CMS-2025-0240", "type": "comment", 
         "text": "These medicare changes will help seniors"},
        {"docket_id": "CMS-2025-0240", "type": "comment", 
         "text": "I have concerns about medicare funding"},
        {"docket_id": "CMS-2025-0240", "type": "comment", 
         "text": "Medicare should cover more services"},
        {"docket_id": "CMS-2025-0240", "type": "comment", 
         "text": "Support the medicare updates"},
    ]
    
    for i, doc in enumerate(dummy_data):
        client.index(index=index_name, id=i, body=doc)
    
    client.indices.refresh(index=index_name)
    
    print(f"✓ Ingested {len(dummy_data)} records")
    print("  DEA-2024-0059: 3 docs, 2 comments (term: 'meaningful use')")
    print("  CMS-2025-0240: 2 docs, 4 comments (terms: 'medicare', 'updates')")


if __name__ == "__main__":
    ingest_opensearch()