# scripts/extract_categories.py

from collections import defaultdict
from pymongo import MongoClient
from bson import ObjectId

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"
SITEMAPS_COLLECTION = "sitemaps"
CATEGORY_COLLECTION = "categories"
CATEGORY_ATTR_COLLECTION = "category_attributes"
TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")  # convert string to ObjectId


def extract_categorical_fields():
    """
    Extract all categorical fields for a tenant by mapping categoryAttribute IDs
    in sitemaps to readable {category_name: attribute_name}.
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    # Load categories for tenant
    categories = {str(cat["_id"]): cat["name"] for cat in db[CATEGORY_COLLECTION].find({"tenant": TENANT_ID})}

    # Load category attributes for tenant
    category_attrs = {}
    for attr in db[CATEGORY_ATTR_COLLECTION].find({"tenant": TENANT_ID}):
        category_id = str(attr["category"])
        category_name = categories.get(category_id)
        if category_name:
            category_attrs[str(attr["_id"])] = {
                "category_name": category_name,
                "attribute_name": attr["name"]
            }

    # Loop over sitemaps
    value_sets = defaultdict(set)
    for doc in db[SITEMAPS_COLLECTION].find({"tenant": TENANT_ID}):
        attr_ids = doc.get("categoryAttribute", [])
        for attr_id in attr_ids:
            attr_info = category_attrs.get(str(attr_id))
            if attr_info:
                value_sets[attr_info["category_name"]].add(attr_info["attribute_name"])

    client.close()

    # Convert sets to lists
    categorical_fields = {k: list(v) for k, v in value_sets.items()}

    return categorical_fields


if __name__ == "__main__":
    categories = extract_categorical_fields()
    print("📂 Tenant Categories:")
    for field, values in categories.items():
        print(f"- {field} ({len(values)} unique): {values}")
