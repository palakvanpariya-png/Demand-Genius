import os
import json
from openai import OpenAI
from rapidfuzz import process, fuzz
from dotenv import load_dotenv

load_dotenv()

# Load API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Example tenant categories (this should come from Mongo in production)
tenant_categories = {
    "Tags": ["Marketing", "Blog Post", "Demand Generation"],
    "Page Type": ["Webinar", "Blog", "Case Study"],
    "Primary Audience": ["Revenue Teams", "Marketing Personas", "Women of Color"],
    "Language": ["English", "German", "French"],
    "Funnel Stage": ["TOFU", "MOFU", "BOFU"]
}

# Optional synonyms mapping for fuzzy matching
synonyms_map = {
    "marketing personas": ("Primary Audience", "Marketing Personas"),
    "revenue teams": ("Primary Audience", "Revenue Teams"),
    "non english": ("Language", "German"),  # Example
}

# ----------------------------
# Build OpenAI Tools Schema
# ----------------------------
def build_tools_schema(categories):
    filters_properties = {
        cat: {"type": "array", "items": {"type": "string"}} 
        for cat in categories.keys()
    }

    schema = [
        {
            "type": "function",
            "function": {
                "name": "parse_query",
                "description": "Classify query and extract filters/constraints from user query",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "classification": {
                            "type": "string",
                            "enum": ["structured", "advisory"]
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
                                        "type": {"type": "string", "enum": ["before", "after"]},
                                        "value": {"type": "string", "format": "date"}
                                    }
                                },
                                "gated": {"type": "boolean"}
                            }
                        },
                        "quoted_entities": {
                            "type": "array",
                            "items": {"type": "string"}
                        }
                    },
                    "required": ["classification", "filters", "constraints", "quoted_entities"]
                }
            }
        }
    ]
    return schema

# ----------------------------
# Fuzzy & Synonym Mapping
# ----------------------------
def post_process_results(parsed_data, categories):
    filters = parsed_data.get("filters", {})
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
    for cat, values in categories.items():
        for word in parsed_data.get("query_text", "").split():
            match, score, _ = process.extractOne(word, values, scorer=fuzz.partial_ratio)
            if score >= 90:  # threshold
                filters.setdefault(cat, []).append(match)

    parsed_data["filters"] = filters
    return parsed_data


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
    # query = 'Show me all assets tagged "Marketing" and "Blog Post" in German targeting marketing personas'
    # query = 'List all MOFU pages created after January 1st, 2025'
    # query = 'What funnel stages do we have the least content for?'
    # query = ' What are the most common tags used across our directory? '
    query = 'Show me Blog content that would help a business choose the best B2B data provider'
    result = parse_query_with_tools(query)
    print(json.dumps(result, indent=2))