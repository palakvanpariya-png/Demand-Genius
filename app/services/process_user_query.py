from typing import Dict, Any
from advisory_handler import AdvisoryHandler  # the class we just rewrote

def process_user_query(
    query_text: str,
    tenant_id: str,
    db_connection,
    tenant_categories: Dict[str, list],
    query_parser
) -> Dict[str, Any]:
    """
    Process a user query through parsing and advisory generation for a multi-tenant system.

    Args:
        query_text: Raw user query string.
        tenant_id: Tenant ID to scope the search.
        db_connection: MongoDB database connection.
        tenant_categories: Dictionary of tenant-specific categories & valid values.
        query_parser: Your parser function, e.g., parse_query_with_tools.

    Returns:
        Dictionary containing:
            - advisory: Human-readable advisory response with follow-up actions.
            - parsed_query: Normalized filters, constraints, and classification.
            - original_query: Original user input.
    """
    # Step 1: Parse query
    parsed_data = query_parser(query_text)

    # Step 2: Generate advisory response using multi-tenant handler
    advisory_handler = AdvisoryHandler(
        db_connection=db_connection,
        tenant_id=tenant_id,
        tenant_categories=tenant_categories
    )
    advisory_response = advisory_handler.generate_advisory(parsed_data)

    # Step 3: Return combined response for UI / API
    return {
        "original_query": query_text,
        "parsed_query": parsed_data,
        "advisory": advisory_response
    }

# ----------------------------
# Example usage
# ----------------------------
if __name__ == "__main__":
    import pymongo
    from query_parser_working import parse_query_with_tools  # replace with actual parser
    from category_extracter import extract_categorical_fields
    from bson import ObjectId

    # Connect to Mongo
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]

    # Example tenant info
    tenant_id = ObjectId("6875f3afc8337606d54a7f37")
    tenant_categories = extract_categorical_fields()

    test_queries = [
        "Show me TOFU content tagged 'AI Tools'",
        "What funnel stages do we have the least content for?",
        "Are we overly focused on TOFU content?",
        "Show me all assets tagged \"Marketing\" and \"Blog Post\" in German targeting marketing personas"
    ]

    for query in test_queries:
        result = process_user_query(query, tenant_id, db, tenant_categories, parse_query_with_tools)
        print(f"\nQuery: {query}")
        print(f"Advisory: {result['advisory']['advisory_text']}")
        print(f"Follow-up Actions: {result['advisory']['follow_up_actions']}")
