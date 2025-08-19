from bson import ObjectId
from pprint import pprint


def build_structured_pipeline(parsed_query: dict, tenant_id: str):
    """
    Convert query_parser output into a MongoDB aggregation pipeline.
    Handles both standard filters and aggregation queries.
    """
    pipeline = []

    # Step 1: Always filter by tenant
    pipeline.append({"$match": {"tenant": ObjectId(tenant_id)}})

    # Step 2: Apply required joins (category_attribute, categories, etc.)
    for join in parsed_query.get("database_mapping", {}).get("required_joins", []):
        if join["collection"] == "category_attributes":
            # Build lookup $match
            lookup_match = {
                "$expr": {"$in": ["$_id", "$$local_ids"]},
                "tenant": ObjectId(tenant_id)
            }

            # Only apply value filtering if NOT an aggregation
            if join.get("values") and not parsed_query.get("aggregation_requested"):
                lookup_match[join["lookup_field"]] = {"$in": join["values"]}

            pipeline.append({
                "$lookup": {
                    "from": "category_attributes",
                    "let": {"local_ids": "$categoryAttribute"},
                    "pipeline": [{"$match": lookup_match}],
                    "as": "categoryAttributeDetails"
                }
            })

            pipeline.append({
                "$unwind": {
                    "path": "$categoryAttributeDetails",
                    "preserveNullAndEmptyArrays": False
                }
            })

    # Step 3: Apply direct field filters (if any)
    direct_fields = parsed_query.get("database_mapping", {}).get("direct_fields", {})
    if direct_fields:
        pipeline.append({
            "$match": {
                field: {"$in": values}
                for field, values in direct_fields.items() if values
            }
        })

    # Step 4: Apply aggregation stages if requested
    if parsed_query.get("aggregation_requested"):
        for agg_field in parsed_query.get("database_mapping", {}).get("aggregation_fields", []):
            # Use joined field if exists, else raw field
            group_field = f"$categoryAttributeDetails.{agg_field}" if any(
                join["category"] == agg_field for join in parsed_query.get("database_mapping", {}).get("required_joins", [])
            ) else f"${agg_field}"

            pipeline.append({
                "$group": {
                    "_id": group_field,
                    "count": {"$sum": 1}
                }
            })
            pipeline.append({"$sort": {"count": -1}})
            pipeline.append({"$limit": 1})

    return pipeline

