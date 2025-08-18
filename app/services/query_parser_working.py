import os
import json
from openai import OpenAI
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
from rapidfuzz import fuzz
from category_extracter import extract_categorical_fields

load_dotenv()

# Load API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Example tenant categories (this should come from Mongo in production)
# tenant_categories = {
#     "Tags": ["Marketing", "Blog Post", "Demand Generation"],
#     "Page Type": ["Webinar", "Blog", "Case Study"],
#     "Primary Audience": ["Revenue Teams", "Marketing Personas", "Women of Color"],
#     "Language": ["English", "German", "French"],
#     "Funnel Stage": ["TOFU", "MOFU", "BOFU"]
# }
tenant_categories = extract_categorical_fields()# Optional synonyms mapping for fuzzy matching
synonyms_map = {
    "marketing personas": ("Primary Audience", "Marketing Personas"),
    "revenue teams": ("Primary Audience", "Revenue Teams"),
    "non english": ("Language", "German"),  # Example
}

# ----------------------------
# Build OpenAI Tools Schema (Tenant-Aware)
# ----------------------------
def build_tools_schema(categories):
    """
    Build a JSON schema for OpenAI tools that strictly enforces
    tenant-specific categories and their allowed values.
    """
    filters_properties = {
        cat: {
            "type": "array",
            "items": {
                "type": "string",
                "enum": categories[cat]  # <-- enforce allowed values
            }
        }
        for cat in categories.keys()
    }

    schema = [
        {
            "type": "function",
            "function": {
                "name": "parse_query",
                "description": "Classify query and extract tenant-specific filters/constraints from user query",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "classification": {
                            "type": "string",
                            "enum": ["structured", "advisory", "analytical", "exploratory"]
                        },
                        "filters": {
                            "type": "object",
                            "properties": filters_properties,
                            "additionalProperties": False
                        },
                        "constraints": {
                            "type": "object",
                            "properties": {
                                "temporal": {
                                    "type": "object",
                                    "properties": {
                                        "type": {"type": "string", "enum": ["before", "after", "range", "relative"]},
                                        "start_date": {"type": "string"},
                                        "end_date": {"type": "string"},
                                        "relative_period": {"type": "string"}
                                    }
                                },
                                "gated": {"type": "boolean"},
                                "limit": {"type": "integer"},
                                "sort_by": {"type": "string"}
                            }
                        },
                        "quoted_entities": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "user_intent": {
                            "type": "string",
                            "description": "High-level user intent behind the query",
                            "enum": [
                                "explore", 
                                "retrieve", 
                                "compare", 
                                "summarize", 
                                "count", 
                                "analyze",
                                "trend_analysis",
                                "performance_review"
                            ]
                        },
                        "operation_type": {
                            "type": "string",
                            "description": "Specific operation requested",
                            "enum": [
                                "list", 
                                "aggregate", 
                                "analyze", 
                                "rank", 
                                "drilldown",
                                "compare",
                                "summarize",
                                "count"
                            ]
                        },
                        "aggregation_requested": {
                            "type": "boolean",
                            "description": "Whether query requests data aggregation"
                        },
                        "comparison_entities": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Entities to compare if comparison is requested"
                        }
                    },
                    "required": [
                        "classification", 
                        "filters", 
                        "constraints", 
                        "quoted_entities",
                        "user_intent",
                        "operation_type",
                        "aggregation_requested"
                    ]
                }
            }
        }
    ]
    return schema


# ----------------------------
# Utility
# ----------------------------
def normalize_filters(filters: dict) -> dict:
    """Ensure every filter value is always a list (never None)."""
    if not filters:
        return {}
    return {cat: (vals or []) for cat, vals in filters.items()}

# ----------------------------
# Fuzzy & Synonym Mapping
# ----------------------------
def post_process_results(parsed_data, categories):
    # normalize filters early
    filters = normalize_filters(parsed_data.get("filters", {}))
    quoted_entities = parsed_data.get("quoted_entities", [])

    # 1. Map quoted entities directly to categories if exact match
    for entity in quoted_entities:
        for cat, values in categories.items():
            if entity in values and entity not in filters.get(cat, []):
                filters.setdefault(cat, []).append(entity)

    # 2. Apply synonyms mapping
    for phrase, (cat, mapped_value) in synonyms_map.items():
        if phrase.lower() in parsed_data.get("query_text", "").lower():
            filters.setdefault(cat, []).append(mapped_value)

    # 3. Apply fuzzy matching for unquoted natural language terms
    filters = apply_synonyms_map(
        query_text=parsed_data.get("query_text", ""),
        filters=filters,
        categories=categories,
        synonyms_map=synonyms_map
    )

    parsed_data["filters"] = normalize_filters(filters)
    return parsed_data


def apply_synonyms_map(query_text: str, filters: dict, categories: dict, synonyms_map: dict, threshold: int = 85) -> dict:
    query_lower = query_text.lower()
    filters = normalize_filters(filters)

    for phrase, (cat, mapped_values) in synonyms_map.items():
        phrase_lower = phrase.lower()
        score = fuzz.partial_ratio(phrase_lower, query_lower)

        if score >= threshold:
            for val in mapped_values:
                if val in categories.get(cat, []):  # only allow valid category values
                    filters.setdefault(cat, []).append(val)

    # Deduplicate values for each category safely
    for cat in filters:
        filters[cat] = list(set(filters[cat]))

    return filters



# ----------------------------
# Main Query Parser
# ----------------------------
def parse_query_with_tools(query_text):
    tools_schema = build_tools_schema(tenant_categories)

    system_message = (
        "You are a precise query parser. Match any mentioned terms to the exact tenant categories "
        "and values provided. If a value is mentioned in quotes, map it to the category it belongs to. "
        "If a natural language phrase matches a category value (even partially), map it as well. "
        "Only use the provided category values."
    )

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": query_text}
        ],
        tools=tools_schema,
        tool_choice={"type": "function", "function": {"name": "parse_query"}}
    )

    # Extract tool output
    tool_output = completion.choices[0].message.tool_calls[0].function.arguments
    parsed_data = json.loads(tool_output)
    parsed_data["query_text"] = query_text  # for post-processing context

    # Post-process for synonyms & fuzzy matches
    return post_process_results(parsed_data, tenant_categories)


# ----------------------------
# Example Run
# ----------------------------
if __name__ == "__main__":
    queries = [
    'Show me all assets tagged "Marketing" and "Blog Post" in German targeting marketing personas',
    'List all MOFU pages created after January 1st, 2025',
    'What funnel stages do we have the least content for?',
    'What are the most common tags used across our directory?',
    'Show me TOFU content tagged ‘AI Tools’',
    'Show me Blog content that would help a business choose the best B2B data provider']
    for query in queries:
        result = parse_query_with_tools(query)
        print("---------------------------")
        print(json.dumps(result, indent=2))
        print("---------------------------")