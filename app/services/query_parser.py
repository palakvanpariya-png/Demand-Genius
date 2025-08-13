# app/core/query_parser.py

import re
import unicodedata
from typing import Dict, Any, List
from rapidfuzz import process

# Try loading spaCy for lemmatization, fallback if not available
try:
    import spacy
    nlp = spacy.load("en_core_web_sm")
except Exception as e:
    print(f"⚠️ spaCy lemmatization disabled: {e}")
    nlp = None

# Category synonym maps
CATEGORY_SYNONYMS = {
    "funnel_stage": {
        "tof": "TOFU", "top of funnel": "TOFU",
        "mofu": "MOFU", "middle of funnel": "MOFU",
        "bofu": "BOFU", "bottom of funnel": "BOFU"
    },
    "industry": {
        "financial services": "Financial Services",
        "fintech": "Financial Services",
        "healthcare": "Healthcare",
        "saas": "SaaS"
    },
    "content_type": {
        "blog": "Blog",
        "case study": "Case Study",
        "whitepaper": "Whitepaper",
        "report": "Report"
    }
}


def clean_text(text: str) -> str:
    """Normalize case, remove accents & extra spaces."""
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text.lower()


def lemmatize_words(text: str) -> List[str]:
    """Lemmatize words if spaCy is available, else just split."""
    if not nlp:
        return text.split()
    doc = nlp(text)
    return [token.lemma_ for token in doc if token.is_alpha]


def fuzzy_match(word: str, mapping: Dict[str, str], threshold: int = 85) -> str:
    """Return best fuzzy match above threshold, safe unpack."""
    # Skip fuzzy matching for very short words (avoids noise)
    if len(word) < 3:
        return None
    result = process.extractOne(word, mapping.keys(), score_cutoff=threshold)
    if not result:
        return None
    match, score, _ = result
    return mapping[match] if match else None


def classify_query(user_query: str, filters_found: Dict[str, Any] = None) -> str:
    """
    Determine if query is structured or advisory.
    Now uses intent verbs and requires both:
      1. At least one recognized filter
      2. A 'structured' action verb present
    """

    query_clean = clean_text(user_query)

    # Define intent trigger phrases
    STRUCTURED_TRIGGERS = ["show", "list", "find", "get", "give me", "display"]
    ADVISORY_TRIGGERS = ["how many", "what is the best", "least", "most", "should", "why"]

    # 1. Detect advisory phrasing first (overrides everything)
    if any(trigger in query_clean for trigger in ADVISORY_TRIGGERS):
        return "advisory"

    # 2. Require at least one category filter match
    has_filter = bool(filters_found and any(filters_found.values()))

    # 3. Check if structured verb trigger is present
    has_structured_verb = any(trigger in query_clean for trigger in STRUCTURED_TRIGGERS)

    # 4. Only call structured if both verb + filter present
    if has_filter and has_structured_verb:
        return "structured"

    # 5. Otherwise default to advisory
    return "advisory"



def parse_query(user_query: str, use_llm: bool = False) -> Dict[str, Any]:
    query_clean = clean_text(user_query)
    lemmas = lemmatize_words(query_clean)
    filters: Dict[str, List[str]] = {}

    for category, mapping in CATEGORY_SYNONYMS.items():
        matches = set()

        # Rule-based exact match
        for key, val in mapping.items():
            if re.search(rf"\b{re.escape(key)}\b", query_clean, re.IGNORECASE):
                matches.add(val)

        # Lemma-based match
        for lemma in lemmas:
            if lemma in mapping:
                matches.add(mapping[lemma])

        # Fuzzy match
        for lemma in lemmas:
            fuzzy_val = fuzzy_match(lemma, mapping)
            if fuzzy_val:
                matches.add(fuzzy_val)

        if matches:
            filters[category] = list(matches)

    # Optional: LLM fallback
    if use_llm and not filters:
        from app.services.llm import extract_filters_from_llm
        llm_filters = extract_filters_from_llm(user_query)
        filters.update(llm_filters)

    # Classification after filter extraction
    classification = classify_query(user_query, filters_found=filters)

    return {
        "classification": classification,
        "filters": filters
    }

