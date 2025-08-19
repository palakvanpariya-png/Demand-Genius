from bson import ObjectId
from pymongo.collection import Collection
from typing import Optional

class CategoryExtractor:
    def __init__(self, categories_collection: Collection, tenant_id: str):
        """
        Extractor to resolve category names into ObjectIds.
        """
        self.tenant_id = ObjectId(tenant_id)
        self.categories_collection = categories_collection
        self.category_map: Dict[str, ObjectId] = {}
        self._load_categories()

    def _load_categories(self):
        """
        Loads all categories for the tenant into a dict:
        { "Funnel Stage": ObjectId(...), "Industry": ObjectId(...), ... }
        """
        categories = self.categories_collection.find({"tenant": self.tenant_id})
        for c in categories:
            self.category_map[c["name"]] = c["_id"]

    def get_category_id(self, category_name: str) -> Optional[ObjectId]:
        """
        Returns the ObjectId for a given category name.
        """
        return self.category_map.get(category_name)