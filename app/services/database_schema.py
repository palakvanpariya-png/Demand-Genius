from pymongo import MongoClient
from bson import ObjectId
from typing import Dict, List, Optional, Any, Tuple
import logging
from dataclasses import dataclass
from functools import lru_cache

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CollectionInfo:
    """Information about a reference collection"""
    name: str
    id_field: str = "_id"
    name_field: str = "name"
    tenant_field: str = "tenant"

@dataclass
class FieldMapping:
    """Dynamic field mapping information"""
    category_name: str
    source_collection: str
    field_path: str
    requires_join: bool
    reference_collection: Optional[str] = None
    join_config: Optional[Dict[str, Any]] = None

@dataclass
class TenantSchema:
    """Complete tenant schema information"""
    tenant_id: str
    categories: Dict[str, List[str]]  # category_name -> list of attribute values
    field_mappings: Dict[str, FieldMapping]  # category_name -> FieldMapping
    collections_info: Dict[str, CollectionInfo]  # collection_name -> CollectionInfo

class DynamicTenantSchemaExtractor:
    """
    Dynamically extracts tenant schema without hardcoded assumptions
    """
    
    def __init__(self, db_client, database_name: str):
        self.db = db_client[database_name]
        self._schema_cache = {}
        
        # Define the collections we work with - this is the only "configuration"
        self.known_collections = {
            "categories": CollectionInfo("categories"),
            "category_attributes": CollectionInfo("category_attributes", tenant_field="tenant"),
            "content_types": CollectionInfo("content_types"),
            "topics": CollectionInfo("topics"),
            "custom_tags": CollectionInfo("custom_tags"),
            "sitemaps": CollectionInfo("sitemaps", name_field="name")
        }
    
    def extract_tenant_schema(self, tenant_id: str) -> TenantSchema:
        """
        Dynamically extract complete schema for a tenant
        """
        try:
            tenant_object_id = ObjectId(tenant_id) if isinstance(tenant_id, str) else tenant_id
            logger.info(f"Extracting schema for tenant: {tenant_id}")
            
            # Step 1: Extract all categories and their attributes
            categories = self._extract_tenant_categories(tenant_object_id)
            
            # Step 2: Discover sitemap field relationships dynamically
            field_mappings = self._discover_field_mappings(tenant_object_id, categories)
            
            # Step 3: Build complete schema
            schema = TenantSchema(
                tenant_id=str(tenant_object_id),
                categories=categories,
                field_mappings=field_mappings,
                collections_info=self.known_collections
            )
            
            # Cache and return
            self._schema_cache[tenant_id] = schema
            logger.info(f"Successfully extracted schema for tenant {tenant_id}")
            return schema
            
        except Exception as e:
            logger.error(f"Error extracting schema for tenant {tenant_id}: {str(e)}")
            raise
    
    def _extract_tenant_categories(self, tenant_id: ObjectId) -> Dict[str, List[str]]:
        """
        Extract all categories and their attribute values for a tenant
        """
        categories = {}
        
        try:
            # Get all categories for this tenant
            categories_cursor = self.db.categories.find({"tenant": tenant_id})
            
            for category_doc in categories_cursor:
                category_name = category_doc.get("name", "").strip()
                if not category_name:
                    continue
                
                # Get attributes for this category
                attributes = self._get_category_attributes(category_doc["_id"])
                if attributes:  # Only include categories that have attributes
                    categories[category_name] = attributes
            
            return categories
            
        except Exception as e:
            logger.error(f"Error extracting categories: {str(e)}")
            return {}
    
    def _get_category_attributes(self, category_id: ObjectId) -> List[str]:
        """Get attribute values for a category"""
        try:
            attributes_cursor = self.db.category_attributes.find({"category": category_id})
            return [attr.get("name", "").strip() for attr in attributes_cursor 
                   if attr.get("name", "").strip()]
        except Exception as e:
            logger.error(f"Error getting attributes for category {category_id}: {str(e)}")
            return []
    
    def _discover_field_mappings(self, tenant_id: ObjectId, categories: Dict[str, List[str]]) -> Dict[str, FieldMapping]:
        """
        Dynamically discover how categories map to sitemap fields
        """
        field_mappings = {}
        
        try:
            # Sample sitemap document to understand structure
            sample_sitemap = self.db.sitemaps.find_one({"tenant": tenant_id})
            if not sample_sitemap:
                logger.warning(f"No sitemap found for tenant {tenant_id}")
                return {}
            
            # For each category, determine how it maps to sitemap fields
            for category_name in categories.keys():
                mapping = self._determine_category_mapping(category_name, sample_sitemap, tenant_id)
                if mapping:
                    field_mappings[category_name] = mapping
            
            # Add direct sitemap field mappings (non-category fields)
            direct_mappings = self._discover_direct_field_mappings(sample_sitemap, tenant_id)
            field_mappings.update(direct_mappings)
            
            return field_mappings
            
        except Exception as e:
            logger.error(f"Error discovering field mappings: {str(e)}")
            return {}
    
    def _determine_category_mapping(self, category_name: str, sample_sitemap: Dict, tenant_id: ObjectId) -> Optional[FieldMapping]:
        """
        Determine how a specific category maps to sitemap fields
        """
        try:
            # Most categories will be in categoryAttribute array
            # Let's verify this by checking if any category_attributes for this category
            # are referenced in the sitemap's categoryAttribute array
            
            category_doc = self.db.categories.find_one({
                "name": category_name,
                "tenant": tenant_id
            })
            
            if not category_doc:
                return None
            
            # Check if this category's attributes are referenced in sitemaps
            category_attributes = list(self.db.category_attributes.find({
                "category": category_doc["_id"]
            }))
            
            if not category_attributes:
                return None
            
            # Check if sitemap's categoryAttribute array contains any of these attributes
            sitemap_category_attrs = sample_sitemap.get("categoryAttribute", [])
            category_attr_ids = [attr["_id"] for attr in category_attributes]
            
            # If there's overlap, this category uses the categoryAttribute array
            if any(attr_id in sitemap_category_attrs for attr_id in category_attr_ids):
                return FieldMapping(
                    category_name=category_name,
                    source_collection="sitemaps",
                    field_path="categoryAttribute",
                    requires_join=True,
                    reference_collection="category_attributes",
                    join_config={
                        "from": "category_attributes",
                        "local_field": "categoryAttribute",
                        "foreign_field": "_id",
                        "filter_field": "category",
                        "filter_value": category_doc["_id"]
                    }
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Error determining mapping for category {category_name}: {str(e)}")
            return None
    
    def _discover_direct_field_mappings(self, sample_sitemap: Dict, tenant_id: ObjectId) -> Dict[str, FieldMapping]:
        """
        Discover direct field mappings (non-category fields in sitemaps)
        """
        direct_mappings = {}
        
        # Map of sitemap fields to their reference collections
        field_collection_map = {
            "contentType": "content_types",
            "topic": "topics",
            "geoFocus": None,  # Direct string field
            "tag": "custom_tags"  # Assuming tags reference custom_tags
        }
        
        for field_name, ref_collection in field_collection_map.items():
            if field_name not in sample_sitemap:
                continue
            
            try:
                if ref_collection is None:
                    # Direct field (like geoFocus/Language)
                    # Get unique values from sitemaps for this field
                    unique_values = self._get_unique_field_values(field_name, tenant_id)
                    if unique_values:
                        # Create a virtual category for this field
                        virtual_category_name = self._field_to_category_name(field_name)
                        direct_mappings[virtual_category_name] = FieldMapping(
                            category_name=virtual_category_name,
                            source_collection="sitemaps",
                            field_path=field_name,
                            requires_join=False
                        )
                else:
                    # Reference field - get values from referenced collection
                    ref_values = self._get_reference_field_values(ref_collection, tenant_id)
                    if ref_values:
                        virtual_category_name = self._field_to_category_name(field_name)
                        direct_mappings[virtual_category_name] = FieldMapping(
                            category_name=virtual_category_name,
                            source_collection="sitemaps",
                            field_path=field_name,
                            requires_join=True,
                            reference_collection=ref_collection,
                            join_config={
                                "from": ref_collection,
                                "local_field": field_name,
                                "foreign_field": "_id"
                            }
                        )
            except Exception as e:
                logger.error(f"Error processing field {field_name}: {str(e)}")
                continue
        
        return direct_mappings
    
    def _get_unique_field_values(self, field_name: str, tenant_id: ObjectId, limit: int = 100) -> List[str]:
        """Get unique values for a direct field"""
        try:
            pipeline = [
                {"$match": {"tenant": tenant_id, field_name: {"$exists": True, "$ne": None}}},
                {"$group": {"_id": f"${field_name}"}},
                {"$limit": limit}
            ]
            
            results = list(self.db.sitemaps.aggregate(pipeline))
            return [str(result["_id"]) for result in results if result["_id"]]
            
        except Exception as e:
            logger.error(f"Error getting unique values for {field_name}: {str(e)}")
            return []
    
    def _get_reference_field_values(self, collection_name: str, tenant_id: ObjectId) -> List[str]:
        """Get name values from a reference collection"""
        try:
            docs = list(self.db[collection_name].find(
                {"tenant": tenant_id}, 
                {"name": 1}
            ))
            return [doc.get("name", "").strip() for doc in docs if doc.get("name", "").strip()]
            
        except Exception as e:
            logger.error(f"Error getting values from {collection_name}: {str(e)}")
            return []
    
    def _field_to_category_name(self, field_name: str) -> str:
        """Convert field name to user-friendly category name"""
        field_name_map = {
            "contentType": "Content Type",
            "topic": "Topic", 
            "geoFocus": "Language",
            "tag": "Custom Tags"
        }
        return field_name_map.get(field_name, field_name.title())
    
    @lru_cache(maxsize=100)
    def get_cached_schema(self, tenant_id: str) -> Optional[TenantSchema]:
        """Get cached schema"""
        return self._schema_cache.get(tenant_id)
    
    def get_tenant_categories_for_ai(self, tenant_id: str) -> Dict[str, List[str]]:
        """
        Get tenant categories in AI-compatible format
        Includes both real categories and virtual categories from direct fields
        """
        try:
            schema = self.get_cached_schema(tenant_id)
            if not schema:
                schema = self.extract_tenant_schema(tenant_id)
            
            # Start with real categories
            ai_categories = dict(schema.categories)
            
            # Add virtual categories from direct field mappings
            tenant_obj_id = ObjectId(tenant_id)
            for mapping in schema.field_mappings.values():
                if mapping.category_name not in ai_categories:
                    # This is a virtual category, get its values
                    if not mapping.requires_join:
                        # Direct field values
                        values = self._get_unique_field_values(mapping.field_path, tenant_obj_id)
                    else:
                        # Reference field values
                        values = self._get_reference_field_values(mapping.reference_collection, tenant_obj_id)
                    
                    if values:
                        ai_categories[mapping.category_name] = values
            
            return ai_categories
            
        except Exception as e:
            logger.error(f"Error getting AI categories for tenant {tenant_id}: {str(e)}")
            return {}
    
    def get_database_field_mapping(self, tenant_id: str) -> Dict[str, Dict[str, Any]]:
        """
        Get database field mapping compatible with original code format
        """
        try:
            schema = self.get_cached_schema(tenant_id)
            if not schema:
                schema = self.extract_tenant_schema(tenant_id)
            
            # Convert FieldMapping objects to original format
            field_mapping = {}
            for category_name, mapping in schema.field_mappings.items():
                field_mapping[category_name] = {
                    "collection": mapping.source_collection,
                    "field_path": mapping.field_path,
                    "lookup_field": "name" if mapping.requires_join else None,
                    "requires_join": mapping.requires_join
                }
                
                if mapping.join_config:
                    field_mapping[category_name]["join_config"] = mapping.join_config
            
            return field_mapping
            
        except Exception as e:
            logger.error(f"Error getting field mapping for tenant {tenant_id}: {str(e)}")
            return {}
    
    def clear_cache(self, tenant_id: Optional[str] = None):
        """Clear schema cache"""
        if tenant_id:
            self._schema_cache.pop(tenant_id, None)
        else:
            self._schema_cache.clear()


# Example usage
def example_usage():
    """Example usage with your actual data structure"""
    
    # Initialize
    client = MongoClient('mongodb://localhost:27017/')
    extractor = DynamicTenantSchemaExtractor(client, 'my_database')
    
    tenant_id = "6875f3afc8337606d54a7f37"
    
    try:
        # Extract schema
        schema = extractor.extract_tenant_schema(tenant_id)
        print(schema)
        
        # print(f"Discovered categories for tenant {tenant_id}:")
        # for category_name, attributes in schema.categories.items():
        #     print(f"  {category_name}: {len(attributes)} attributes")
        #     print(f"    Sample: {attributes[:3]}")
        
        # print(f"\nField mappings:")
        # for category, mapping in schema.field_mappings.items():
        #     print(f"  {category}: {mapping.source_collection}.{mapping.field_path}")
        
        # # Get AI format
        # ai_categories = extractor.get_tenant_categories_for_ai(tenant_id)
        # print(f"\nTotal categories for AI: {len(ai_categories)}")
        
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    example_usage()