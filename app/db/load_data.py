import json
import os
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime

# === MongoDB connection ===
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# === Utility to parse $oid and $date ===
def parse_mongo_json(obj):
    if isinstance(obj, dict):
        if "$oid" in obj:
            return ObjectId(obj["$oid"])
        elif "$date" in obj:
            return datetime.fromisoformat(obj["$date"].replace("Z", "+00:00"))
        else:
            return {k: parse_mongo_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [parse_mongo_json(i) for i in obj]
    else:
        return obj

# === Function to load JSON files ===
def load_json_file(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return [parse_mongo_json(doc) for doc in data]
    else:
        return [parse_mongo_json(data)]

# === Main load function ===
def load_data():
    base_dir = "/home/ubuntu/Demand Genius/Demand-Genius/data"  # Folder where JSON files are stored

    file_mapping = {
        "categories.json": "categories",
        "category_attributes.json": "category_attributes",
        "sitemaps.json": "sitemaps"
    }

    for filename, collection_name in file_mapping.items():
        file_path = os.path.join(base_dir, filename)
        if not os.path.exists(file_path):
            print(f"⚠️ File not found: {file_path}")
            continue

        # Drop collection if it exists
        if collection_name in db.list_collection_names():
            db[collection_name].drop()
            print(f"🗑️ Dropped existing '{collection_name}' collection.")

        # Load and insert new data
        documents = load_json_file(file_path)
        if documents:
            db[collection_name].insert_many(documents)
            print(f"✅ Inserted {len(documents)} docs into '{collection_name}' collection.")

if __name__ == "__main__":
    load_data()
