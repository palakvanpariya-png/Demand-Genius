# test_pipeline.py
from try_query_parser import parse_query_with_enhanced_tools
from pipeline_builder import build_structured_pipeline
from pipeline_executor import execute_pipeline_count
from bson import ObjectId
from pprint import pprint

tenant_id = "6875f3afc8337606d54a7f37"
user_query = "shpw all tofu content"

# 1️⃣ Parse the query using enhanced query parser
parsed_query = parse_query_with_enhanced_tools(user_query)
print("\n=== Parsed Query ===")
pprint(parsed_query)

# 2️⃣ Build the MongoDB aggregation pipeline
pipeline = build_structured_pipeline(parsed_query, tenant_id)
print("\n=== Debug Pipeline ===")
for stage in pipeline:
    pprint(stage)

# 3️⃣ Execute pipeline to get only count
count = execute_pipeline_count(pipeline, tenant_id)
print(f"\nTotal results: {count}")