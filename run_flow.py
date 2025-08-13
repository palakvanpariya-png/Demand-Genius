# run_flow.py

from pymongo import MongoClient
from app.services.query_parser import parse_query
from app.services.retrieval_service import structured_search, build_structured_match

# 1. Connect to Mongo
client = MongoClient("mongodb://localhost:27017") 
db = client["your_database_name"]  # Change to your DB name
collection = db["sitemaps"]        # Change to your collection name

# 2. Example queries
queries = [
    "Show me TOFU content for Financial Services",
    "List all MOFU pages created after January 1st, 2025",
    "What funnel stages do we have the least content for?"
]

# 3. Process each query
for q in queries:
    print(f"\n🔍 Query: {q}")
    parsed = parse_query(q)
    print(f"Parsed: {parsed}")

    if parsed["classification"] == "structured" and parsed["filters"]:
        results = structured_search(parsed["filters"], collection)
        print(f"✅ Found {len(results)} results:")
        for r in results:
            print(f"- {r.get('title')} ({r.get('url')})")
    else:
        print("⚠️ No structured filters found — will handle via semantic search later.")
