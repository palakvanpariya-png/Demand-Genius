import logging
from typing import Dict, Any, List
from bson import ObjectId

logger = logging.getLogger(__name__)

class AdvisoryHandler:
    """
    Multi-tenant aware advisory handler.
    Generates human-readable advisory responses based on parsed query data.
    """

    def __init__(self, db_connection, tenant_id: str, tenant_categories: Dict[str, Dict[str, ObjectId]]):
        """
        Args:
            db_connection: MongoDB database connection
            tenant_id: Tenant context for scoping queries
            tenant_categories: Dict mapping category_name -> {value_name -> category_attribute_id}
                Example:
                {
                    "Funnel Stage": {"TOFU": ObjectId(...), "MOFU": ObjectId(...), ...},
                    "Page Type": {"Blog": ObjectId(...), "Webinar": ObjectId(...), ...}
                }
        """
        self.db = db_connection
        self.tenant_id = tenant_id
        self.collection = db_connection['sitemap']
        self.tenant_categories = tenant_categories

    # -----------------------------
    # Public function
    # -----------------------------
    def generate_advisory(self, parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        classification = parsed_data.get("classification", "")
        filters = parsed_data.get("filters", {})
        constraints = parsed_data.get("constraints", {})

        # Map filters to category_attribute ObjectIds
        mapped_filters = self._map_filters_to_category_ids(filters)

        # Build Mongo query
        query = self._build_mongo_query(mapped_filters, constraints)

        # Always scope by tenant
        query["tenant"] = ObjectId(self.tenant_id)

        # Count matching documents
        count = self.collection.count_documents(query)

        # Build advisory text
        filter_summary = self._summarize_filters(filters)
        if count == 0:
            advisory_text = f"No content found matching your query: {filter_summary}."
            follow_up = ["broaden_search", "try_different_filters"]
        elif count == 1:
            advisory_text = f"Found 1 piece of content matching: {filter_summary}."
            follow_up = ["view_details", "find_similar"]
        else:
            advisory_text = f"Found {count} pieces of content matching: {filter_summary}."
            follow_up = ["view_list", "refine_filters", "sort_results"]

        return {
            "advisory_text": advisory_text,
            "follow_up_actions": follow_up,
            "metadata": {
                "count": count,
                "filters_applied": filter_summary,
                "has_results": count > 0
            }
        }

    # -----------------------------
    # Helper functions
    # -----------------------------
    def _map_filters_to_category_ids(self, filters: dict) -> dict:
        """
        Maps human-readable filter values to their category attribute ObjectIds.
        Safe for multi-tenant usage and handles accidental list/dict mismatches.
        
        Args:
            filters: Dict of {category_name: [values]}
        
        Returns:
            Dict of {category_name: [ObjectId, ...]}
        """
        mapped_filters = {}

        for category_name, values in filters.items():
            # Get category mapping for this tenant
            category_map = self.tenant_categories.get(category_name, {})

            # Ensure category_map is a dict
            if isinstance(category_map, list):
                # Convert list to dict with values as keys and ids as values
                # Fallback: use index as dummy ObjectId placeholder if needed
                logger.warning(f"category_map for '{category_name}' is a list. Converting to dict.")
                category_map = {str(v): v for v in category_map}

            elif not isinstance(category_map, dict):
                logger.warning(f"category_map for '{category_name}' is neither dict nor list. Skipping.")
                continue

            mapped_values = []
            for v in values:
                if v in category_map:
                    mapped_values.append(category_map[v])
                else:
                    logger.warning(f"Value '{v}' not found in category '{category_name}' mapping.")

            if mapped_values:
                mapped_filters[category_name] = mapped_values

        return mapped_filters


    def _build_mongo_query(self, mapped_filters: Dict[str, List[ObjectId]], constraints: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build MongoDB query from mapped filters and constraints.
        """
        query = {}

        # Apply category filters
        for category_name, ids in mapped_filters.items():
            if ids:
                query.setdefault("categoryAttribute", {})
                query["categoryAttribute"]["$in"] = ids

        # Apply other constraints if needed (temporal, gated)
        temporal = constraints.get("temporal")
        if temporal:
            if temporal.get("type") == "after":
                query["createdAt"] = {"$gte": temporal.get("start_date")}
            elif temporal.get("type") == "before":
                query["createdAt"] = {"$lte": temporal.get("end_date")}
            elif temporal.get("type") == "range":
                query["createdAt"] = {"$gte": temporal.get("start_date"), "$lte": temporal.get("end_date")}

        gated = constraints.get("gated")
        if gated is not None:
            query["isMarketingContent"] = gated  # Example: adjust based on your field

        return query

    def _summarize_filters(self, filters: Dict[str, List[str]]) -> str:
        """
        Generate human-readable summary of filters
        """
        active = []
        for field, vals in filters.items():
            if vals:
                active.append(f"{field}: {', '.join(vals)}")
        return "; ".join(active) if active else "all content"
