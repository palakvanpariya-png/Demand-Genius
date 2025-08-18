# scripts/generate_synonyms.py

import os
import json
from pymongo import MongoClient
from bson import ObjectId
from openai import OpenAI
from dotenv import load_dotenv

# import your existing extractor
from category_extracter import extract_categorical_fields  

load_dotenv()

MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"
TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")

SYNONYMS_COLLECTION = "tenant_synonyms"

# ✅ Init OpenAI client
client_ai = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def get_synonyms_with_openai(category_name: str, value: str) -> list[str]:
    prompt = f"""
You are helping normalize tenant category values.

Category: "{category_name}"
Value: "{value}"

Task:
- Suggest 5–10 natural synonyms, aliases, or alternative phrasings that users might use.
- If it's an acronym (like TOFU = Top of Funnel, MOFU = Middle of Funnel, BOFU = Bottom of Funnel), expand it and give realistic alternatives.
- If you can't think of good synonyms, return [].

Rules:
- Respond ONLY in raw JSON.
- Either:
  ["Alt1", "Alt2", "Alt3"]
  OR
  {{"synonyms": ["Alt1", "Alt2"]}}
"""

    response = client_ai.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    raw = response.choices[0].message.content.strip()
    print(f"🔍 RAW response for {category_name} → {value}: {raw}")

    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):  # case: ["a", "b", "c"]
            return parsed
        elif isinstance(parsed, dict) and "synonyms" in parsed:  # case: {"synonyms": [...]}
            return parsed["synonyms"]
        else:
            print(f"⚠️ Unexpected JSON structure for {value}: {parsed}")
            return []
    except Exception as e:
        print(f"⚠️ JSON parse error for {value}: {e}")
        # fallback: extract quoted strings
        import re
        matches = re.findall(r'"(.*?)"', raw)
        return matches if matches else []




def generate_and_store_synonyms():
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[DB_NAME]

    # ✅ Use your existing extractor
    tenant_categories = extract_categorical_fields()

    """
    tenant_categories looks like:
    {
        "Funnel Stage": ["TOFU", "MOFU", "BOFU"],
        "Industry": ["Financial Services", "Biotech", ...],
        "Page Type": [...],
        ...
    }
    """

    for category_name, values in tenant_categories.items():
        for value in values:
            # check if synonyms already exist
            existing = db[SYNONYMS_COLLECTION].find_one({
                "tenant": TENANT_ID,
                "category": category_name,
                "value": value
            })
            if existing:
                print(f"✅ Skipping {category_name} → {value} (already has synonyms)")
                continue

            # ask OpenAI
            synonyms = get_synonyms_with_openai(category_name, value)

            record = {
                "tenant": TENANT_ID,
                "category": category_name,
                "value": value,
                "synonyms": synonyms
            }
            db[SYNONYMS_COLLECTION].insert_one(record)

            print(f"✨ Stored synonyms for {category_name} → {value}: {synonyms}")

    mongo_client.close()


if __name__ == "__main__":
    generate_and_store_synonyms()
