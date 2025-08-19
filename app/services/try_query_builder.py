from typing import Any, Dict, List, Union, Optional
from bson import ObjectId
from typing import Dict, Optional


# -------------------------
# Mapping of filter types
# -------------------------
FILTER_MAPPING = {
    "Funnel Stage": "category",
    "Industry": "category",
    "Primary Audience": "category",
    "Secondary Audience": "category",
    "Page Type": "direct",
    "Language": "direct",
    "Title": "direct",
    "URL": "direct",
}


    
# for joining the categories with sitemaps      
def build_category_lookups( 
    required_joins: List[Dict[str, Any]],
    tenant_id: Union[str, ObjectId],
    category_index: Any,
    *,
    as_suffix: str = "Details"
) -> List[Dict[str, Any]]:
    # Normalize tenant_id
    if not isinstance(tenant_id, ObjectId):
        try:
            tenant_oid = ObjectId(str(tenant_id))
        except Exception as e:
            raise ValueError(f"Invalid tenant_id; expected ObjectId or hex string. Got: {tenant_id}") from e
    else:
        tenant_oid = tenant_id

    stages: List[Dict[str, Any]] = []

    for join in (required_joins or []):
        collection: str = join.get("collection", "").strip()
        local_field: str = join.get("field", "").strip()
        lookup_field: str = join.get("lookup_field", "name").strip()
        values: List[str] = join.get("values", []) or []
        category_name: Optional[str] = join.get("category")
        as_field: str = join.get("as") or (f"{local_field}{as_suffix}" if local_field else f"{collection}{as_suffix}")

        if not collection or not local_field:
            # Skip malformed join
            continue

        # Resolve category_id via category_extracter (if provided)
        category_match: Dict[str, Any] = {}
        if category_name:
            try:
                category_id = category_index.get_category_id(tenant_oid, category_name)
                if category_id:
                    category_match["category"] = category_id if isinstance(category_id, ObjectId) else ObjectId(str(category_id))
            except Exception:
                # If category cannot be resolved, we do not constrain by category;
                # but we still enforce tenant scoping.
                pass

        # Optional name/value filtering
        value_match: Dict[str, Any] = {}
        if values:
            value_match[lookup_field] = {"$in": values}

        # Build $lookup with pipeline, enforcing:
        #   - _id ∈ $$local_ids
        #   - tenant == tenant_oid
        #   - (optional) category == category_id
        #   - (optional) lookup_field ∈ values
        lookup_stage = {
            "$lookup": {
                "from": collection,
                "let": {"local_ids": f"${local_field}"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {"$in": ["$_id", "$$local_ids"]},
                            "tenant": tenant_oid,
                            **category_match,
                            **value_match,
                        }
                    }
                ],
                "as": as_field,
            }
        }

        # Unwind to simplify downstream matching/grouping (keeps only docs that matched)
        unwind_stage = {
            "$unwind": {
                "path": f"${as_field}",
                "preserveNullAndEmptyArrays": False
            }
        }

        stages.append(lookup_stage)
        stages.append(unwind_stage)

    return stages


def build_tenant_match(tenant_id: str) -> dict:
    return {
        "$match": {
            "tenant": ObjectId(tenant_id)
        }
    }

def apply_direct_field_filters(filters: dict) -> dict:
    match_conditions = {}

    for field, values in filters.items():
        if not values:  # skip empty
            continue

        # Simple: match any of the values
        match_conditions[field] = {"$in": values}

    if match_conditions:
        return {"$match": match_conditions}
    else:
        return {}

def apply_aggregation(pipeline: List[Dict], parser_output: dict) -> List[Dict]:
    """
    Extend the existing pipeline to handle aggregation, counts, and top-N queries
    based on parser output.
    """
    operation_type = parser_output.get("operation_type")
    aggregation_requested = parser_output.get("aggregation_requested", False)
    agg_fields = parser_output.get("database_mapping", {}).get("aggregation_fields", [])

    # If no aggregation requested, return original pipeline
    if not aggregation_requested and operation_type not in ["aggregate", "count", "rank"]:
        return pipeline

    # For "count" type queries: just count total filtered docs
    if operation_type in ["count"]:
        pipeline.append({"$count": "total_results"})
        return pipeline

    # For aggregation/rank queries
    if operation_type in ["aggregate", "rank"]:
        if not agg_fields:
            # fallback: count all docs
            pipeline.append({"$count": "total_results"})
            return pipeline

        # Build _id for $group stage
        if len(agg_fields) == 1:
            group_id = f"$categoryAttributeDetails.{agg_fields[0]}"  # assuming category join
        else:
            group_id = {field: f"$categoryAttributeDetails.{field}" for field in agg_fields}

        pipeline.append({
            "$group": {
                "_id": group_id,
                "count": {"$sum": 1}
            }
        })

        # Sort descending for "most" type queries
        pipeline.append({"$sort": {"count": -1}})

        # Optionally, limit 1 for "top" queries
        if operation_type == "rank":
            pipeline.append({"$limit": 1})

    return pipeline




def build_structured_pipeline(parser_output: dict, tenant_id: str, extractor) -> list:
    """
    Build the full MongoDB aggregation pipeline including:
    - Tenant match
    - Category lookups
    - Direct field filters
    - Aggregation / count / rank stages based on parser output
    """
    pipeline = []

    # 1. Always start with tenant filter
    pipeline.append(build_tenant_match(tenant_id))

    # 2. Separate category vs direct filters
    filters = parser_output.get("filters", {})
    category_filters = {}
    direct_filters = {}

    for field, values in (filters or {}).items():
        if not values:
            continue

        filter_type = FILTER_MAPPING.get(field)
        if filter_type == "category":
            category_filters[field] = values
        elif filter_type == "direct":
            direct_filters[field] = values

    # 3. Category lookups (joins)
    required_joins = parser_output.get("database_mapping", {}).get("required_joins", [])
    if required_joins:
        category_stages = build_category_lookups(required_joins, tenant_id, extractor)
        pipeline.extend(category_stages)

    # 4. Direct field filters
    direct_match_stage = apply_direct_field_filters(direct_filters)
    if direct_match_stage:
        pipeline.append(direct_match_stage)

    # 5. Apply aggregation / count / rank if requested
    pipeline = apply_aggregation(pipeline, parser_output)

    return pipeline

