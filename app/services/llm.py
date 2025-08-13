# app/services/llm.py

import os
from typing import Dict, List
import json
from openai import OpenAI

# Set this env var in your deployment
# export OPENAI_API_KEY="sk-xxxx"
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")  # smaller/cheaper model for extraction

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


def extract_filters_from_llm(user_query: str) -> Dict[str, List[str]]:
    """
    Uses an LLM to extract structured filters (funnel_stage, industry, content_type)
    from a user query when rule-based parser fails.
    """

    system_prompt = """
    You are a query parser that extracts structured filters from a user query.
    Your job is to return ONLY a JSON object with these optional keys:
    - funnel_stage: ["TOFU", "MOFU", "BOFU"]
    - industry: list of normalized industry names (title case)
    - content_type: list of normalized content types (title case)

    If a category is not mentioned, omit it from the JSON.

    Rules:
    - Be strict: only include if confidently inferred from the query.
    - Normalize synonyms to the standard forms above.
    - Return only valid JSON, no extra text.
    """

    user_prompt = f"Query: {user_query}\nExtract filters now."

    try:
        response = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": system_prompt.strip()},
                {"role": "user", "content": user_prompt.strip()},
            ],
            temperature=0,
            max_tokens=150
        )

        content = response.choices[0].message["content"].strip()

        # Parse JSON safely
        filters = json.loads(content)
        # Ensure all values are lists
        return {k: v if isinstance(v, list) else [v] for k, v in filters.items()}

    except Exception as e:
        # In production, log the error properly
        print(f"LLM extraction failed: {e}")
        return {}
