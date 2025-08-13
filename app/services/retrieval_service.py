from typing import Dict, Any, List
from pymongo.collection import Collection

def build_structured_match(filters: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Build a MongoDB $match object from parsed query filters.

    Args:
        filters: dict where each key is a category (e.g., funnel_stage)
                 and value is a list of matched filter values.

    Returns:
        dict: MongoDB $match stage for an aggregation pipeline.
    """
    match_conditions = {}

    for category, values in filters.items():
        if not values:
            continue
        if len(values) == 1:
            match_conditions[category] = values[0]
        else:
            match_conditions[category] = {"$in": values}

    return {"$match": match_conditions}

def structured_search(filters: Dict[str, List[str]], collection: Collection, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Perform a structured search in MongoDB based on parsed filters.

    Args:
        filters: dict of category -> list of values.
        collection: pymongo Collection instance.
        limit: max number of docs to return.

    Returns:
        List of matching documents.
    """
    match_stage = build_structured_match(filters)
    pipeline = [
        match_stage,
        {"$project": {
            "_id": 0,
            "title": 1,
            "url": 1,
            "funnel_stage": 1,
            "industry": 1,
            "content_type": 1
        }},
        {"$limit": limit}
    ]

    return list(collection.aggregate(pipeline))


