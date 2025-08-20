# scripts/extract_categories.py

from collections import defaultdict
from pymongo import MongoClient
from bson import ObjectId

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"
SITEMAPS_COLLECTION = "sitemaps"
CATEGORY_COLLECTION = "categories"
CATEGORY_ATTR_COLLECTION = "category_attributes"
CONTENT_TYPES_COLLECTION = "content_types"
CUSTOM_TAGS_COLLECTION = "custom_tags"
TOPICS_COLLECTION = "topics"

TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")


def extract_categorical_fields():
    """
    Extract all categorical fields for a tenant:
    - categoryAttribute mappings (categories + attributes)
    - geoFocus as Language
    - content_types
    - custom_tags
    - topics
    """
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    value_sets = defaultdict(set)

    # ----------------------------
    # Categories + Attributes
    # ----------------------------
    categories = {str(cat["_id"]): cat["name"] for cat in db[CATEGORY_COLLECTION].find({"tenant": TENANT_ID})}

    category_attrs = {}
    for attr in db[CATEGORY_ATTR_COLLECTION].find({"tenant": TENANT_ID}):
        category_id = str(attr["category"])
        category_name = categories.get(category_id)
        if category_name:
            category_attrs[str(attr["_id"])] = {
                "category_name": category_name,
                "attribute_name": attr["name"]
            }

    for doc in db[SITEMAPS_COLLECTION].find({"tenant": TENANT_ID}):
        attr_ids = doc.get("categoryAttribute", [])
        for attr_id in attr_ids:
            attr_info = category_attrs.get(str(attr_id))
            if attr_info:
                value_sets[attr_info["category_name"]].add(attr_info["attribute_name"])

        geo_focus = doc.get("geoFocus")
        if geo_focus:
            value_sets["Language"].add(geo_focus)

    # ----------------------------
    # Content Types
    # ----------------------------
    for ct in db[CONTENT_TYPES_COLLECTION].find({"tenant": TENANT_ID}):
        value_sets["Content Type"].add(ct["name"])

    # ----------------------------
    # Custom Tags
    # ----------------------------
    for tag in db[CUSTOM_TAGS_COLLECTION].find({"tenant": TENANT_ID}):
        value_sets["Custom Tags"].add(tag["name"])

    # ----------------------------
    # Topics
    # ----------------------------
    for topic in db[TOPICS_COLLECTION].find({"tenant": TENANT_ID}):
        value_sets["Topics"].add(topic["name"])

    client.close()

    # Convert sets to lists
    categorical_fields = {k: sorted(list(v)) for k, v in value_sets.items()}

    return categorical_fields


if __name__ == "__main__":
    categories = extract_categorical_fields()
    print("📂 Tenant Categories:")
    for field, values in categories.items():
        print(f"- {field} ({len(values)} unique): {values}")
