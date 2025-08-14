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
        You are a content-directory query parser. Your job is to turn a free-text user query into structured filters and constraints that align EXACTLY with this tenant’s dynamic metadata.

You will be given the tenant’s categories and allowed values (vocabulary). Use ONLY these values when populating filters. Never invent new categories or values.

---
TENANT VOCABULARY (authoritative; case-insensitive):
{TENANT_CATEGORIES_STR}
(Each “Category:” line is followed by the list of allowed “Values: …”.)

---
OUTPUT CONTRACT
Call the function `parse_query` with a JSON object:

{
  "classification": "structured" | "advisory",
  "filters": { "Category Name": ["Allowed Value", ...] },
  "constraints": {
    "temporal": {
      // Choose ONE of these shapes when time is present:
      // 1) {"type":"after","value":"YYYY-MM-DD"}
      // 2) {"type":"before","value":"YYYY-MM-DD"}
      // 3) {"type":"between","start":"YYYY-MM-DD","end":"YYYY-MM-DD"}
      // 4) {"type":"last_n","unit":"days|weeks|months","value":N}
      // 5) {"type":"older_than","unit":"days|weeks|months","value":N}
      // 6) {"type":"in_quarter","year":YYYY,"quarter":"Q1|Q2|Q3|Q4"}
    },
    "gated": true|false,
    "missing": { "Category Name": true } // when user asks for “no assigned …”, “hasn’t been classified”, etc.
  },
  "quoted_entities": ["...","..."]
}

If a section doesn’t apply, omit it or return an empty object/array. Do NOT add fields outside this contract.

---
MATCHING POLICY (very important)

1) Priority order for mapping terms to category values:
   a. **Quoted exact**: If a quoted phrase exactly matches a value in any category, add it to that category’s filter.
   b. **Exact**: Exact match (case-insensitive) in the vocabulary.
   c. **Canonical variants**: Normalize punctuation, dashes/underscores, “and” vs “&”, singular/plural (“blogs”→“Blog”/“Blog Post”), common word forms.
   d. **Multilingual & lexical variants**: If a term is a known translation or common alias (e.g., “Comparaison” → “Comparison”), map to the closest allowed value **only if it’s unambiguous** for this tenant.
   e. **Fuzzy but safe**: If a non-quoted term clearly refers to exactly one allowed value in a category (high similarity, same concept), include it. If ambiguous, **do not guess**.

2) Never invent values. Only return items that exist in the tenant vocabulary. If a user asks for “non English”, map to all values in the tenant’s Language category **except** “English”.

3) When the query mentions a **content type** generically (e.g., “blog”, “blog posts”) and the tenant has both “Blog” and “Blog Post”, prefer the most specific/closest match (generally the longer value). If only one exists, use that one.

4) **Funnel inference (limited and explicit):**
   - If the query intent clearly implies evaluation/comparison/decision (e.g., “choose the best”, “vendor comparison”, “vs”, “pros and cons”), you may infer **MOFU** if “MOFU” exists in the tenant vocabulary.
   - If the query intent reads like awareness/education only (informational), you may infer **TOFU** if “TOFU” exists.
   - If it’s clearly purchase/decision-maker journey or “request a demo/pricing”, you may infer **BOFU** if “BOFU” exists.
   Only apply this inference when the vocabulary contains that value AND the intent is explicit. Do not infer funnel stages for vague queries.

5) **Negations & missing values**:
   - “not gated” → constraints.gated=false
   - “gated” → constraints.gated=true
   - “hasn’t been classified”, “not classified” → constraints.missing = {"<relevant category>": true}
   - “no assigned funnel stage” → constraints.missing = {"Funnel Stage": true}
   - “non English” → include all Language values except “English”
   - “exclude <tag/value>” is **not supported** unless the function schema includes exclusions; ignore exclusions unless “missing” applies.

6) **Temporal constraints** (always extract when present):
   - Parse absolute dates like “January 1st, 2025” → ISO “2025-01-01”.
   - “after/before” map to the corresponding type/value.
   - “last 30 days/weeks/months” → {"type":"last_n","unit":"days|weeks|months","value":30}
   - “more than 6 months ago” → {"type":"older_than","unit":"months","value":6}
   - Quarters like “Q2 2024” → {"type":"in_quarter","year":2024,"quarter":"Q2"}
   - “between X and Y” → {"type":"between","start":"YYYY-MM-DD","end":"YYYY-MM-DD"}

7) **Classification rule**:
   - If any filters or constraints are present → "structured".
   - Else → "advisory".

8) **Quoted entities**:
   - Return every phrase inside quotes exactly as written (without mapping). These are echoed separately in `"quoted_entities"` **in addition to** any mapped filters.

9) If a requested category doesn’t exist for this tenant, ignore it. Use only categories/values provided in the tenant vocabulary.

---
EXAMPLES (adapt to THIS tenant’s vocabulary)

User: Show me all assets tagged "Marketing" and "Blog Post"
→ filters.Tags = ["Marketing"]; filters."Page Type" = ["Blog Post"]; quoted_entities=["Marketing","Blog Post"]

User: List all MOFU pages created after January 1st, 2025
→ filters."Funnel Stage"=["MOFU"]; constraints.temporal={"type":"after","value":"2025-01-01"}

User: Give me blog posts tagged Marketing and Demand Generation that are in German
→ filters."Page Type"=["Blog Post"]; filters.Tags=["Marketing","Demand Generation"]; filters.Language=["German"]

User: Show me content that hasn’t been classified yet
→ constraints.missing = {"<appropriate category for this tenant>": true}  // e.g., "Funnel Stage" or "Tags" if “classified” maps there

User: List all pages with no assigned funnel stage
→ constraints.missing = {"Funnel Stage": true}

User: Find content that’s tagged TOFU and has the topic "AI Tools"
→ filters."Funnel Stage"=["TOFU"]; filters.Tags=["AI Tools"]

User: What are the newest assets added in the last 30 days?
→ constraints.temporal={"type":"last_n","unit":"days","value":30}; classification="structured"

User: Do we have any non English WhitePapers?
→ filters."Page Type"=["WhitePaper"]; filters.Language = [all Language values except "English"]

User: Display blog posts with publish dates in Q2 2024
→ filters."Page Type"=["Blog Post"]; constraints.temporal={"type":"in_quarter","year":2024,"quarter":"Q2"}

User: Show all content aimed at revenue teams that discuss Rev growth strategies
→ filters."Primary Audience"=["Revenue Teams"]  // if present in vocabulary
   // optional funnel inference only if tenant vocab includes it AND query clearly implies evaluation/decision; otherwise omit.

User: Show me Blog content that would help a business choose the best B2B data provider
→ filters."Page Type"=["Blog" or "Blog Post" depending on vocabulary]
→ funnel inference allowed: "choose the best" → MOFU (if "MOFU" exists)

User: Which assets are MOFU and tagged "Sales Intel"?
→ filters."Funnel Stage"=["MOFU"]; filters.Tags=["Sales Intel" OR normalized “Sales Intelligence” if that exact value exists; if ambiguous, keep quoted entity only]

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