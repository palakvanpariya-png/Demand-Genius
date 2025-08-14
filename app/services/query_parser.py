# app/core/query_parser.py

from pymongo import MongoClient
from bson import ObjectId
from collections import defaultdict
from typing import Dict, List

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"

# Cache to avoid DB calls every query
TENANT_CATEGORY_CACHE: Dict[str, Dict[str, List[str]]] = {}

def load_tenant_categories(tenant_id: ObjectId) -> Dict[str, List[str]]:
    """
    Load categories and attributes for a tenant from MongoDB.
    Returns: {category_name: [attribute1, attribute2, ...]}
    """
    tenant_key = str(tenant_id)
    if tenant_key in TENANT_CATEGORY_CACHE:
        return TENANT_CATEGORY_CACHE[tenant_key]

    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Load categories
    categories = {str(cat["_id"]): cat["name"] for cat in db["category"].find({"tenant": tenant_id})}

    # Load category attributes
    category_attrs = defaultdict(list)
    for attr in db["category_attribute"].find({"tenant": tenant_id}):
        cat_name = categories.get(str(attr["category"]))
        if cat_name:
            category_attrs[cat_name].append(attr["name"])

    client.close()
    TENANT_CATEGORY_CACHE[tenant_key] = category_attrs
    return category_attrs


# Quick test
if __name__ == "__main__":
    tenant_id = ObjectId("6875f3afc8337606d54a7f37")
    categories = load_tenant_categories(tenant_id)
    print("Tenant categories:", categories)
