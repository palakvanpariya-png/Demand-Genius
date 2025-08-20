import os
import json
from openai import OpenAI
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
from category_extracter import extract_categorical_fields

load_dotenv()

# Load API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Get tenant categories from your existing function
tenant_categories = extract_categorical_fields()

# ----------------------------
# Enhanced Database Schema Mapping
# ----------------------------
# we have to optimize this for multiple tenant 
def get_database_field_mapping():
    """Map category names to actual MongoDB fields and collections"""
    return {
        "Funnel Stage": {
            "collection": "category_attributes",
            "field_path": "categoryAttribute",
            "lookup_field": "name",
            "requires_join": True
        },
        "Primary Audience": {
            "collection": "category_attributes", 
            "field_path": "categoryAttribute",
            "lookup_field": "name",
            "requires_join": True
        },
        "Secondary Audience": {
            "collection": "category_attributes",
            "field_path": "categoryAttribute", 
            "lookup_field": "name",
            "requires_join": True
        },
        "Page Type": {
            "collection": "sitemaps",
            "field_path": "contentType",
            "lookup_field": "name",
            "requires_join": True
        },
        "Industry": {
            "collection": "category_attributes",
            "field_path": "categoryAttribute",
            "lookup_field": "name", 
            "requires_join": True
        },
        "Language": {
            "collection": "sitemaps",
            "field_path": "geoFocus",
            "lookup_field": None,
            "requires_join": False
        }
    }

# takes category values and does the matching 
def intelligent_fuzzy_matching(query_text, categories, threshold=80):
    """More intelligent fuzzy matching against category values"""
    matches = {}
    query_lower = query_text.lower()
    
    for category, values in categories.items():
        category_matches = []
        
        for value in values:
            # Direct substring match
            if value.lower() in query_lower:
                category_matches.append(value)
                continue
                
            # Fuzzy matching
            score = fuzz.partial_ratio(value.lower(), query_lower)
            if score >= threshold:
                category_matches.append(value)
                continue
                
            # Word-level matching for multi-word values
            value_words = value.lower().split()
            query_words = query_lower.split()
            
            word_matches = 0
            for v_word in value_words:
                for q_word in query_words:
                    if fuzz.ratio(v_word, q_word) >= 85:
                        word_matches += 1
                        break
            
            # If most words match, include it
            if word_matches >= len(value_words) * 0.6:
                category_matches.append(value)
        
        if category_matches:
            matches[category] = list(set(category_matches))
    
    return matches

# ----------------------------
# Enhanced Tools Schema with Strategic Classification
# ----------------------------
def build_schema(categories):
    """Enhanced schema with better classification and database mapping info"""
    
    filters_properties = {
        cat: {
            "type": "array",
            "items": {
                "type": "string",
                "enum": categories[cat]
            }
        }
        for cat in categories.keys()
    }

    schema = [
        {
            "type": "function",
            "function": {
                "name": "parse_query",
                "description": "Classify query and extract filters with database mapping context",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "classification": {
                            "type": "string",
                            "enum": ["structured", "analytical", "strategic", "exploratory", "comparative"]
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
                                "sort_by": {"type": "string"},
                                "sort_order": {"type": "string", "enum": ["asc", "desc"]}
                            }
                        },
                        "quoted_entities": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "user_intent": {
                            "type": "string",
                            "enum": [
                                "explore", "retrieve", "compare", "summarize", 
                                "count", "analyze", "trend_analysis", "performance_review",
                                "gap_analysis", "distribution_analysis", "strategic_review"
                            ]
                        },
                        "operation_type": {
                            "type": "string", 
                            "enum": [
                                "list", "aggregate", "analyze", "rank", "drilldown",
                                "compare", "summarize", "count", "identify_gaps", "balance_check"
                            ]
                        },
                        "aggregation_requested": {"type": "boolean"},
                        "comparison_entities": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "response_expectation": {
                            "type": "string",
                            "enum": ["conversational", "data_list", "insights", "yes_no", "statistics"],
                            "description": "Expected type of response format"
                        },
                        "business_context": {
                            "type": "string",
                            "enum": ["content_strategy", "audience_analysis", "performance_review", "gap_identification", "general_inquiry"],
                            "description": "Business context of the query"
                        }
                    },
                    "required": [
                        "classification", "filters", "constraints", "quoted_entities",
                        "user_intent", "operation_type", "aggregation_requested", 
                        "response_expectation", "business_context"
                    ]
                }
            }
        }
    ]
    return schema

# ----------------------------
# Enhanced Post Processing
# ----------------------------
def enhanced_post_processing(parsed_data, categories):
    """
    Enhanced post-processing with:
    - Fuzzy matching for filters
    - Quoted entity mapping
    - Database mapping preparation
    - Automatic aggregation field detection
    """
    # -------------------------------
    # 1. Normalize filters
    # -------------------------------
    filters = normalize_filters(parsed_data.get("filters", {}))
    quoted_entities = parsed_data.get("quoted_entities", [])
    query_text = parsed_data.get("query_text", "")

    # -------------------------------
    # 2. Direct quoted entity mapping
    # -------------------------------
    for entity in quoted_entities:
        for cat, values in categories.items():
            if entity in values and entity not in filters.get(cat, []):
                filters.setdefault(cat, []).append(entity)

    # -------------------------------
    # 3. Intelligent fuzzy matching
    # -------------------------------
    fuzzy_matches = intelligent_fuzzy_matching(query_text, categories)
    for category, matched_values in fuzzy_matches.items():
        for value in matched_values:
            if value not in filters.get(category, []):
                filters.setdefault(category, []).append(value)

    # -------------------------------
    # 4. Build database mapping info
    # -------------------------------
    field_mapping = get_database_field_mapping()
    database_info = {
        "required_joins": [],
        "direct_fields": {},
        "aggregation_fields": []
    }

    for category, filter_values in filters.items():
        if filter_values and category in field_mapping:
            mapping_info = field_mapping[category]
            if mapping_info["requires_join"]:
                database_info["required_joins"].append({
                    "collection": mapping_info["collection"],
                    "field": mapping_info["field_path"],
                    "lookup_field": mapping_info["lookup_field"],
                    "values": filter_values,
                    "category": category
                })
            else:
                database_info["direct_fields"][mapping_info["field_path"]] = filter_values

    # -------------------------------
    # 5. Automatic aggregation field detection
    # -------------------------------
    agg_keywords = ["most content", "least content", "distribution", "count by", "rank by"]
    is_agg_query = parsed_data.get("aggregation_requested") or parsed_data.get("operation_type") in ["aggregate", "count", "rank"]

    if is_agg_query:
        query_lower = query_text.lower()
        for category in categories.keys():
            # Check if the category name is mentioned in the query (fuzzy match)
            if fuzz.partial_ratio(category.lower(), query_lower) >= 80:
                if category not in database_info["aggregation_fields"]:
                    database_info["aggregation_fields"].append(category)
                
                # Ensure $lookup join exists for aggregation
                mapping_info = field_mapping.get(category)
                if mapping_info and mapping_info.get("requires_join"):
                    existing_joins = [j["category"] for j in database_info["required_joins"]]
                    if category not in existing_joins:
                        database_info["required_joins"].append({
                            "collection": mapping_info["collection"],
                            "field": mapping_info["field_path"],
                            "lookup_field": mapping_info["lookup_field"],
                            "values": [],  # No filter needed for aggregation
                            "category": category
                        })

    # -------------------------------
    # 6. Update parsed data
    # -------------------------------
    parsed_data.update({
        "filters": normalize_filters(filters),
        "database_mapping": database_info
    })

    return parsed_data


# ----------------------------
# Utility Functions
# ----------------------------
def normalize_filters(filters: dict) -> dict:
    """Ensure every filter value is always a list (never None)."""
    if not filters:
        return {}
    return {cat: (vals or []) for cat, vals in filters.items()}

# ----------------------------
# Main Enhanced Parser
# ----------------------------
def parse_query_with_enhanced_tools(query_text):
    """Enhanced query parser with better database integration"""
    
    tools_schema = build_schema(tenant_categories)
    
    system_message = """You are an advanced query parser for a content analytics system. 

Your job is to:
1. Classify queries by type (structured, analytical, strategic, exploratory, comparative)
2. Extract filters that map to database categories
3. Identify the business context and expected response type
4. Determine if aggregation or analysis is needed

Key guidelines:
- "Show me X" = structured (list items)
- "What/Which X has the most/least Y" = analytical (aggregate data)  
- "Are we overly focused on X" = strategic (business insights)
- Map quoted terms exactly to category values
- Identify natural language terms that match categories
- Consider the user's business intent

Only use the provided category values. Be precise in classification."""

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": query_text}
        ],
        tools=tools_schema,
        tool_choice={"type": "function", "function": {"name": "parse_query"}}
    )

    # Extract and process results
    tool_output = completion.choices[0].message.tool_calls[0].function.arguments
    parsed_data = json.loads(tool_output)
    parsed_data["query_text"] = query_text
    
    # Enhanced post-processing
    return enhanced_post_processing(parsed_data, tenant_categories) 

# ----------------------------
# Example Usage
# ----------------------------
if __name__ == "__main__":
    test_queries = [
        # 'Show me all assets tagged "Marketing" and "Blog Post" in German targeting marketing personas',
        # 'List all MOFU pages created after January 1st, 2025', 
        # 'What funnel stages do we have the least content for?',
        # 'Are we overly focused on TOFU content?',
        'Which personas have little or no BOFU content?',
        'Show me TOFU content',
        'which content would benefit investers'
        # 'Suggest improvements to our content categorization',
        # 'What content topics seem overused or repetitive? '
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        result = parse_query_with_enhanced_tools(query)
        print(json.dumps(result, indent=2))