import os
import json
from openai import OpenAI
from dotenv import load_dotenv
from category_extracter import extract_categorical_fields  # You already have this
# Make sure category_extractor returns dict like: { "Funnel Stage": ["TOFU", "MOFU"], ... }

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

def parse_query(query_text, tenant_id):
    """
    Parses user query into structured filters, constraints, classification, and semantic intent.
    Uses OpenAI function calling with tenant-specific category lists.
    """
    # Fetch tenant-specific categories dynamically
    tenant_categories = extract_categorical_fields()

    # Build schema dynamically from tenant categories
    filter_properties = {}
    for category, values in tenant_categories.items():
        filter_properties[category] = {
            "type": "array",
            "items": {"type": "string", "enum": values},
            "description": f"List of {category} values mentioned in the query."
        }

    tools = [
        {
            "type": "function",
            "function": {
                "name": "extract_query_info",
                "description": (
                    "Extract structured filters and constraints from the user query. "
                    "Only use category values explicitly mentioned in the query or synonyms from provided lists. "
                    "Do not guess or add values not directly mentioned."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "filters": {
                            "type": "object",
                            "properties": filter_properties,
                            "additionalProperties": False
                        },
                        "constraints": {
                            "type": "object",
                            "description": "Any time-based or numeric constraints."
                        },
                        "quoted_entities": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "semantic_intent": {
                            "type": "string",
                            "description": "High-level intent of the query such as vendor_comparison, decision_support, trend_analysis."
                        }
                    },
                    "required": ["filters", "constraints", "quoted_entities", "semantic_intent"]
                }
            }   
        }
    ]

    prompt = """
        You are a query parsing assistant for a multi-tenant content search system.

You MUST output a strict JSON object with these keys:
- filters: { <Category Name>: [values], ... } (Only include categories explicitly mentioned in the query or exact/tenant-specific synonym matches. No guessing.)
- quoted_entities: [list of exact phrases inside double quotes from the query. If none, return []]
- semantic_intent: One of: ["informational", "decision_support", "vendor_comparison", "content_gap_analysis", "statistical", "other"]
- classification: "structured" if the query can be answered by applying filters to the dataset, otherwise "advisory".
- query_text: original query string.

Rules:
1. **Filters**
   - Use only categories provided in the `AVAILABLE_CATEGORIES` object (tenant-specific).
   - Populate values only if they are explicitly in the query OR match a tenant synonym from `CATEGORY_SYNONYMS`.
   - If no explicit mention, leave the list empty.
   - Dates and numeric constraints must be kept in the query for downstream processing — do NOT discard them.

2. **Quoted Entities**
   - Only capture text inside double quotes in the query.
   - Do not extract paraphrased or guessed entities.

3. **Semantic Intent**
   - informational → user asks for facts or details without decision-making.
   - decision_support → user seeks help choosing or evaluating.
   - vendor_comparison → explicitly comparing vendors/products/services.
   - content_gap_analysis → asking what’s missing in content.
   - statistical → asking for counts, most/least common, trends.
   - other → anything else.

4. **Classification**
   - structured → filters + dataset lookup are enough.
   - advisory → requires reasoning, summarization, or derived insight.

5. Do NOT hallucinate filters or entities — be conservative.

6. Output format must be valid JSON, no extra commentary.

---

Example Input:
AVAILABLE_CATEGORIES = {
  "Funnel Stage": ["TOFU", "MOFU", "BOFU"],
  "Industry": ["Financial Services", "Healthcare", "Retail"],
  "Page Type": ["Blog", "Resource Hub", "Case Study"],
  "Primary Audience": ["Businesses", "Revenue Teams"],
  "Secondary Audience": ["Marketing", "Sales"]
}
CATEGORY_SYNONYMS = {
  "revenue teams": {"Primary Audience": "Revenue Teams"},
  "blog content": {"Page Type": "Blog"}
}

Query:
"Show me Blog content for revenue teams in Healthcare created after January 1st, 2024"

Example Output:
{
  "filters": {
    "Page Type": ["Blog"],
    "Primary Audience": ["Revenue Teams"],
    "Industry": ["Healthcare"]
  },
  "quoted_entities": [],
  "semantic_intent": "informational",
  "classification": "structured",
  "query_text": "Show me Blog content for revenue teams in Healthcare created after January 1st, 2024"
}

        """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "system", "content": "You are a query parsing assistant."},
                  {"role": "user", "content": prompt}],
        tools=tools,
        tool_choice={"type": "function", "function": {"name": "extract_query_info"}}
    )

    # Extract structured output from the tool
    tool_call = response.choices[0].message.tool_calls[0]
    parsed = json.loads(tool_call.function.arguments)

    # --- Post-processing ---
    # Restrict guessed values (remove empties / nulls)
    for category, values in parsed["filters"].items():
        if not values or all(v.strip() == "" for v in values):
            parsed["filters"][category] = []

    # Classification rule (B)
    if any(parsed["filters"].values()):
        parsed["classification"] = "structured"
    else:
        parsed["classification"] = "advisory"

    parsed["query_text"] = query_text

    return parsed

if __name__ == "__main__":
    # Example test
    tenant_id = "demo_tenant"
    test_queries = [
        'Show me Blog content that would help a business choose the best B2B data provider',
        'List all MOFU pages created after January 1st, 2025',
        'What funnel stages do we have the least content for?',
        ' What are the most common tags used across our directory? '
    ]
    for q in test_queries:
        result = parse_query(q, tenant_id)
        print(json.dumps(result, indent=2))
        print("-" * 50)



    # query = 'Show me all assets tagged "Marketing" and "Blog Post" in German targeting marketing personas'
    # query = 'List all MOFU pages created after January 1st, 2025'
    # query =  'What funnel stages do we have the least content for?'
    # query = ' What are the most common tags used across our directory? '