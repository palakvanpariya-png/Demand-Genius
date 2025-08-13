# app/db/generate_embeddings.py
import os
from pymongo import MongoClient
from openai import OpenAI
import numpy as np

# === Environment Variables ===
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"
OPENAI_API_KEY = "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"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
openai_client = OpenAI(api_key=OPENAI_API_KEY)

# === Helper: Generate Embedding ===
def get_embedding(text):
    if not text.strip():
        return None
    resp = openai_client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    )
    return resp.data[0].embedding

# === Process One Collection ===
# === Process Sitemaps with Safety ===
def process_sitemaps():
    collection = db["sitemaps"]
    cursor = collection.find({"embedding": {"$exists": False}})  # only new docs

    for doc in cursor:
        try:
            text_parts = [
                str(doc.get(field, "")) 
                for field in ["name", "description", "summary", "readerBenefit", "explanation"] 
                if doc.get(field)
            ]
            combined_text = " ".join(text_parts)[:30000]  # truncate if needed

            if not combined_text.strip():
                print(f"⚠️ Skipped empty doc: {doc['_id']}")
                continue

            embedding = get_embedding(combined_text)
            if embedding:
                collection.update_one({"_id": doc["_id"]}, {"$set": {"embedding": embedding}})
                print(f"✅ Updated sitemaps - ID {doc['_id']}")
        except Exception as e:
            print(f"❌ Error processing {doc['_id']}: {e}")

# === Main ===
if __name__ == "__main__":
    # process_collection("categories", ["name", "slug"])
    # process_collection("category_attributes", ["name", "description"])
    process_sitemaps()
    print("🎯 Embedding generation complete.")
