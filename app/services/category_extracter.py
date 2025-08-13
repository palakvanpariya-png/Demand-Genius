# app/core/query_parser.py

import re
import unicodedata
from typing import Dict, Any, List
from collections import defaultdict
from rapidfuzz import process, fuzz
from pymongo import MongoClient
from bson import ObjectId

# Optional spaCy for lemmatization
try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
except Exception as e:
    print(f"⚠️ spaCy lemmatization disabled: {e}")
    nlp = None

# Mongo config
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "my_database"

# Caching tenant categories to avoid DB hits every query
TENANT_CATEGORY_CACHE: Dict[str, Dict[str, List[str]]] = {}


def clean_text(text: str) -> str:
    """Normalize text: lowercase, remove accents & punctuation, collapse spaces."""
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def lemmatize_words(text: str) -> List[str]:
    """Return lemmatized words if spaCy available, else split."""
    if not nlp:
        return text.split()
    doc = nlp(text)
    return [token.lemma_ for token in doc if token.is_alpha]


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


def fuzzy_extract(query: str, options: List[str], threshold: int = 85) -> List[str]:
    """Return all options that fuzzy-match query above threshold."""
    results = process.extract(query, options, scorer=fuzz.WRatio, score_cutoff=threshold)
    return [match for match, score, _ in results]


def classify_query(user_query: str, filters_found: Dict[str, Any] = None) -> str:
    """Hybrid structured/advisory classification."""
    query_clean = clean_text(user_query)

    STRUCTURED_TRIGGERS = ["show", "list", "find", "get", "give me", "display"]
    ADVISORY_TRIGGERS = ["how many", "what is the best", "least", "most", "should", "why"]

    # Advisory first (overrides)
    if any(trigger in query_clean for trigger in ADVISORY_TRIGGERS):
        return "advisory"

    # Structured requires at least one filter + structured verb
    has_filter = bool(filters_found and any(filters_found.values()))
    has_structured_verb = any(trigger in query_clean for trigger in STRUCTURED_TRIGGERS)

    return "structured" if has_filter and has_structured_verb else "advisory"


def parse_query(user_query: str, tenant_id: ObjectId, use_llm: bool = False) -> Dict[str, Any]:
    """
    Parse a user query into structured filters and classification.
    """
    query_clean = clean_text(user_query)
    lemmas = lemmatize_words(query_clean)
    filters: Dict[str, List[str]] = {}

    # Load tenant-specific categories
    CATEGORY_MAP = load_tenant_categories(tenant_id)

    for category, options in CATEGORY_MAP.items():
        normalized_options = [clean_text(opt) for opt in options]
        matches = set()

        # Exact multi-word match
        for idx, opt in enumerate(normalized_options):
            if re.search(rf"\b{re.escape(opt)}\b", query_clean):
                matches.add(options[idx])

        # Lemma match
        for lemma in lemmas:
            for idx, opt in enumerate(normalized_options):
                if lemma == opt.lower():
                    matches.add(options[idx])

        # Fuzzy match
        for lemma in lemmas:
            fuzzy_results = fuzzy_extract(lemma, normalized_options)
            for match in fuzzy_results:
                original_idx = normalized_options.index(match)
                matches.add(options[original_idx])

        if matches:
            filters[category] = list(matches)

    # Optional: LLM fallback if no filters found
    if use_llm and not filters:
        from app.services.llm import extract_filters_from_llm
        llm_filters = extract_filters_from_llm(user_query)
        filters.update(llm_filters)

    classification = classify_query(user_query, filters_found=filters)

    return {
        "classification": classification,
        "filters": filters
    }
    