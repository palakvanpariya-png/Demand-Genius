# app/services/pipeline_executor.py
from mongo_client import get_mongo_client
import pprint

def execute_pipeline_count(pipeline, tenant_id, collection_name="sitemaps"):
    db = get_mongo_client()
    collection = db[collection_name]

    # 🔍 Debug: print pipeline step by step
    print("\n========== DEBUG PIPELINE ==========")
    for stage in pipeline:
        pprint.pprint(stage)
    print("====================================\n")

    # Add count stage
    pipeline_with_count = pipeline + [{"$count": "count"}]

    result = list(collection.aggregate(pipeline_with_count))
    return result[0]["count"] if result else 0