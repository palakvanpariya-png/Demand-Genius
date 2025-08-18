import os
import json
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from openai import OpenAI
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
from category_extracter import extract_categorical_fields
from dataclasses import dataclass

load_dotenv()

# Load API Key
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY)

# Enhanced tenant categories
tenant_categories = extract_categorical_fields()

# Enhanced synonyms mapping with confidence scores
synonyms_map = {
    "marketing personas": ("Primary Audience", "Marketing Personas", 0.9),
    "revenue teams": ("Primary Audience", "Revenue Teams", 0.9),
    "sales teams": ("Primary Audience", "Revenue Teams", 0.8),
    "non english": ("Language", "German", 0.7),
    "demand gen": ("Tags", "Demand Generation", 0.8),
    "case studies": ("Page Type", "Case Study", 0.9),
    "top of funnel": ("Funnel Stage", "TOFU", 0.9),
    "middle of funnel": ("Funnel Stage", "MOFU", 0.9),
    "bottom of funnel": ("Funnel Stage", "BOFU", 0.9),
}

@dataclass
class QueryResult:
    classification: str
    filters: Dict[str, List[str]]
    constraints: Dict[str, Any]
    quoted_entities: List[str]
    user_intent: str
    operation_type: str
    confidence_score: float
    suggestions: List[str]
    temporal_info: Dict[str, Any]
    aggregation_fields: List[str]
    ranking_criteria: Optional[str]

# ----------------------------
# Enhanced Temporal Processing
# ----------------------------
class TemporalParser:
    DATE_PATTERNS = {
        r'(\d{1,2})/(\d{1,2})/(\d{4})': 'MM/DD/YYYY',
        r'(\d{4})-(\d{1,2})-(\d{1,2})': 'YYYY-MM-DD',
        r'(\d{1,2})-(\d{1,2})-(\d{4})': 'DD-MM-YYYY',
        r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{1,2}),?\s+(\d{4})': 'Month DD, YYYY',
        r'(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})': 'DD Month YYYY'
    }
    
    RELATIVE_PATTERNS = {
        r'last\s+(\d+)\s+(day|week|month|year)s?': 'relative_past',
        r'past\s+(\d+)\s+(day|week|month|year)s?': 'relative_past',
        r'next\s+(\d+)\s+(day|week|month|year)s?': 'relative_future',
        r'(today|yesterday|tomorrow)': 'specific_relative',
        r'this\s+(week|month|year)': 'current_period',
        r'last\s+(week|month|year)': 'previous_period'
    }
    
    @staticmethod
    def parse_temporal_constraints(query_text: str) -> Dict[str, Any]:
        temporal_info = {
            "type": None,
            "start_date": None,
            "end_date": None,
            "relative": None,
            "range": False
        }
        
        query_lower = query_text.lower()
        
        # Check for date ranges
        range_patterns = [
            r'between\s+(.+?)\s+and\s+(.+?)(?:\s|$)',
            r'from\s+(.+?)\s+to\s+(.+?)(?:\s|$)',
            r'(.+?)\s+to\s+(.+?)(?:\s|$)'
        ]
        
        for pattern in range_patterns:
            match = re.search(pattern, query_lower)
            if match:
                start_str, end_str = match.groups()
                start_date = TemporalParser._parse_date_string(start_str)
                end_date = TemporalParser._parse_date_string(end_str)
                
                if start_date and end_date:
                    temporal_info.update({
                        "type": "range",
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                        "range": True
                    })
                    return temporal_info
        
        # Check for before/after patterns
        if 'before' in query_lower:
            match = re.search(r'before\s+(.+?)(?:\s|$)', query_lower)
            if match:
                date = TemporalParser._parse_date_string(match.group(1))
                if date:
                    temporal_info.update({
                        "type": "before",
                        "end_date": date.isoformat()
                    })
        
        if 'after' in query_lower or 'since' in query_lower:
            pattern = r'(?:after|since)\s+(.+?)(?:\s|$)'
            match = re.search(pattern, query_lower)
            if match:
                date = TemporalParser._parse_date_string(match.group(1))
                if date:
                    temporal_info.update({
                        "type": "after",
                        "start_date": date.isoformat()
                    })
        
        # Check for relative dates
        for pattern, rel_type in TemporalParser.RELATIVE_PATTERNS.items():
            match = re.search(pattern, query_lower)
            if match:
                temporal_info["relative"] = {
                    "type": rel_type,
                    "match": match.group(0),
                    "parsed": TemporalParser._parse_relative_date(match, rel_type)
                }
                break
        
        return temporal_info
    
    @staticmethod
    def _parse_date_string(date_str: str) -> Optional[datetime]:
        date_str = date_str.strip()
        
        # Try different date formats
        formats = [
            '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', 
            '%B %d, %Y', '%d %B %Y', '%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Try month names
        months = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        
        for month_name, month_num in months.items():
            if month_name in date_str.lower():
                # Extract year and day
                year_match = re.search(r'\b(\d{4})\b', date_str)
                day_match = re.search(r'\b(\d{1,2})\b', date_str)
                
                if year_match:
                    year = int(year_match.group(1))
                    day = int(day_match.group(1)) if day_match else 1
                    return datetime(year, month_num, day)
        
        return None
    
    @staticmethod
    def _parse_relative_date(match, rel_type: str) -> Dict[str, Any]:
        now = datetime.now()
        
        if rel_type == 'specific_relative':
            relative_word = match.group(1)
            if relative_word == 'today':
                return {"start_date": now.isoformat(), "days": 0}
            elif relative_word == 'yesterday':
                yesterday = now - timedelta(days=1)
                return {"start_date": yesterday.isoformat(), "days": -1}
            elif relative_word == 'tomorrow':
                tomorrow = now + timedelta(days=1)
                return {"start_date": tomorrow.isoformat(), "days": 1}
        
        elif rel_type in ['relative_past', 'relative_future']:
            if len(match.groups()) >= 2:
                number = int(match.group(1))
                unit = match.group(2)
                
                multiplier = -1 if rel_type == 'relative_past' else 1
                
                if unit in ['day', 'days']:
                    delta = timedelta(days=number * multiplier)
                elif unit in ['week', 'weeks']:
                    delta = timedelta(weeks=number * multiplier)
                elif unit in ['month', 'months']:
                    delta = timedelta(days=30 * number * multiplier)  # Approximate
                elif unit in ['year', 'years']:
                    delta = timedelta(days=365 * number * multiplier)  # Approximate
                
                target_date = now + delta
                return {
                    "start_date": target_date.isoformat(),
                    "number": number,
                    "unit": unit,
                    "direction": "past" if multiplier == -1 else "future"
                }
        
        return {}

# ----------------------------
# Query Optimization
# ----------------------------
class QueryOptimizer:
    @staticmethod
    def suggest_improvements(query_text: str, parsed_result: Dict) -> List[str]:
        suggestions = []
        
        # Safely get filters and ensure they're normalized
        filters = parsed_result.get('filters', {})
        if filters is None:
            filters = {}
        
        # Normalize filters to handle None values
        normalized_filters = {}
        for cat, vals in filters.items():
            if vals is None:
                normalized_filters[cat] = []
            elif isinstance(vals, list):
                normalized_filters[cat] = vals
            else:
                normalized_filters[cat] = [vals] if vals else []
        
        # Check for ambiguous queries
        constraints = parsed_result.get('constraints', {})
        if not normalized_filters and not constraints:
            suggestions.append("Consider adding specific filters like 'Blog posts' or 'Marketing content'")
        
        # Check for overly broad queries
        filter_count = sum(len(v) for v in normalized_filters.values())
        if filter_count > 5:
            suggestions.append("Your query has many filters. Consider narrowing down to the most important criteria")
        
        # Suggest temporal constraints if missing
        if 'recent' in query_text.lower() and not constraints.get('temporal'):
            suggestions.append("Consider specifying a time range like 'last 30 days' or 'after January 2024'")
        
        # Suggest specific operations for analysis queries
        analysis_keywords = ['analyze', 'trends', 'patterns', 'insights']
        if any(keyword in query_text.lower() for keyword in analysis_keywords):
            if parsed_result.get('operation_type') == 'list':
                suggestions.append("For analysis, try 'aggregate by category' or 'rank by performance'")
        
        # Suggest aggregation for counting queries
        count_keywords = ['how many', 'count', 'number of']
        if any(keyword in query_text.lower() for keyword in count_keywords):
            if not parsed_result.get('aggregation_fields'):
                suggestions.append("Consider grouping results by categories like 'Tags' or 'Page Type'")
        
        return suggestions
    
    @staticmethod
    def calculate_confidence(parsed_result: Dict, query_text: str) -> float:
        confidence = 0.5  # Base confidence
        
        # Safely get filters and normalize them
        filters = parsed_result.get('filters', {})
        if filters:
            normalized_filters = {}
            for cat, vals in filters.items():
                if vals is None:
                    normalized_filters[cat] = []
                elif isinstance(vals, list):
                    normalized_filters[cat] = vals
                else:
                    normalized_filters[cat] = [vals] if vals else []
            
            filter_count = sum(len(v) for v in normalized_filters.values())
            confidence += min(0.3, filter_count * 0.05)
        
        # Increase confidence for quoted entities
        quoted_entities = parsed_result.get('quoted_entities', [])
        if quoted_entities:
            quoted_count = len(quoted_entities)
            confidence += min(0.2, quoted_count * 0.1)
        
        # Decrease confidence for very short queries
        if len(query_text.split()) < 3:
            confidence -= 0.2
        
        # Increase confidence for specific operations
        if parsed_result.get('operation_type') != 'list':
            confidence += 0.1
        
        return max(0.0, min(1.0, confidence))

# ----------------------------
# Enhanced Result Ranking
# ----------------------------
class ResultRanker:
    RANKING_CRITERIA = {
        'relevance': 'Most relevant to query terms',
        'recency': 'Most recently created/updated',
        'popularity': 'Most viewed or engaged with',
        'completeness': 'Most complete metadata',
        'authority': 'From most authoritative sources'
    }
    
    @staticmethod
    def determine_ranking_criteria(query_text: str, user_intent: str) -> Optional[str]:
        query_lower = query_text.lower()
        
        # Temporal indicators suggest recency ranking
        temporal_keywords = ['recent', 'latest', 'new', 'updated', 'fresh']
        if any(keyword in query_lower for keyword in temporal_keywords):
            return 'recency'
        
        # Performance indicators suggest popularity ranking
        performance_keywords = ['popular', 'best', 'top', 'most viewed', 'trending']
        if any(keyword in query_lower for keyword in performance_keywords):
            return 'popularity'
        
        # Quality indicators suggest authority ranking
        quality_keywords = ['authoritative', 'official', 'verified', 'trusted']
        if any(keyword in query_lower for keyword in quality_keywords):
            return 'authority'
        
        # Default based on intent
        if user_intent in ['explore', 'analyze']:
            return 'relevance'
        elif user_intent == 'retrieve':
            return 'recency'
        
        return 'relevance'

# ----------------------------
# Enhanced Aggregation Support
# ----------------------------
class AggregationDetector:
    @staticmethod
    def detect_aggregation_needs(query_text: str, operation_type: str) -> List[str]:
        aggregation_fields = []
        query_lower = query_text.lower()
        
        # Group by patterns
        group_patterns = [
            r'group\s+by\s+(\w+)',
            r'by\s+(category|tag|type|audience|language|stage)',
            r'per\s+(category|tag|type|audience|language|stage)'
        ]
        
        for pattern in group_patterns:
            matches = re.findall(pattern, query_lower)
            for match in matches:
                # Map common terms to actual categories
                field_mapping = {
                    'category': 'Tags',
                    'tag': 'Tags',
                    'type': 'Page Type',
                    'audience': 'Primary Audience',
                    'language': 'Language',
                    'stage': 'Funnel Stage'
                }
                aggregation_fields.append(field_mapping.get(match, match))
        
        # Implicit aggregation for analysis operations
        if operation_type in ['aggregate', 'analyze']:
            # Suggest common aggregation fields
            if 'content' in query_lower:
                aggregation_fields.extend(['Tags', 'Page Type'])
            if 'audience' in query_lower:
                aggregation_fields.append('Primary Audience')
            if 'funnel' in query_lower:
                aggregation_fields.append('Funnel Stage')
        
        return list(set(aggregation_fields))  # Remove duplicates

# ----------------------------
# Enhanced Tools Schema
# ----------------------------
def build_enhanced_tools_schema(categories):
    filters_properties = {
        cat: {"type": "array", "items": {"type": "string"}} 
        for cat in categories.keys()
    }

    schema = [
        {
            "type": "function",
            "function": {
                "name": "parse_query",
                "description": "Classify query and extract comprehensive filters/constraints from user query",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "classification": {
                            "type": "string",
                            "enum": ["structured", "advisory", "analytical", "exploratory"]
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
                                "sort_by": {"type": "string"}
                            }
                        },
                        "quoted_entities": {
                            "type": "array",
                            "items": {"type": "string"}
                        },
                        "user_intent": {
                            "type": "string",
                            "description": "High-level user intent behind the query",
                            "enum": [
                                "explore", 
                                "retrieve", 
                                "compare", 
                                "summarize", 
                                "count", 
                                "analyze",
                                "trend_analysis",
                                "performance_review"
                            ]
                        },
                        "operation_type": {
                            "type": "string",
                            "description": "Specific operation requested",
                            "enum": [
                                "list", 
                                "aggregate", 
                                "analyze", 
                                "rank", 
                                "drilldown",
                                "compare",
                                "summarize",
                                "count"
                            ]
                        },
                        "aggregation_requested": {
                            "type": "boolean",
                            "description": "Whether query requests data aggregation"
                        },
                        "comparison_entities": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Entities to compare if comparison is requested"
                        }
                    },
                    "required": [
                        "classification", 
                        "filters", 
                        "constraints", 
                        "quoted_entities",
                        "user_intent",
                        "operation_type",
                        "aggregation_requested"
                    ]
                }
            }
        }
    ]
    return schema

# ----------------------------
# Enhanced Post Processing
# ----------------------------
def enhanced_post_process_results(parsed_data, categories, query_text):
    # Normalize filters early to handle None values
    raw_filters = parsed_data.get("filters", {})
    filters = normalize_filters(raw_filters)
    quoted_entities = parsed_data.get("quoted_entities", []) or []

    # 1. Enhanced temporal processing
    temporal_info = TemporalParser.parse_temporal_constraints(query_text)
    if temporal_info["type"]:
        if "constraints" not in parsed_data:
            parsed_data["constraints"] = {}
        parsed_data["constraints"]["temporal"] = temporal_info

    # 2. Map quoted entities with confidence
    for entity in quoted_entities:
        for cat, values in categories.items():
            if values:  # Ensure values is not None
                best_match, score, _ = process.extractOne(entity, values, scorer=fuzz.ratio)
                if score > 85:  # High confidence threshold
                    if best_match not in filters.get(cat, []):
                        filters.setdefault(cat, []).append(best_match)

    # 3. Enhanced synonym mapping with confidence
    filters = apply_enhanced_synonyms_map(
        query_text=query_text,
        filters=filters,
        categories=categories,
        synonyms_map=synonyms_map
    )

    # 4. Detect aggregation needs
    aggregation_fields = AggregationDetector.detect_aggregation_needs(
        query_text, parsed_data.get('operation_type', 'list')
    )

    # 5. Determine ranking criteria
    ranking_criteria = ResultRanker.determine_ranking_criteria(
        query_text, parsed_data.get('user_intent', 'retrieve')
    )

    # 6. Generate suggestions (with error handling)
    try:
        suggestions = QueryOptimizer.suggest_improvements(query_text, parsed_data)
    except Exception as e:
        print(f"Warning: Could not generate suggestions: {e}")
        suggestions = []

    # 7. Calculate confidence (with error handling)
    try:
        confidence_score = QueryOptimizer.calculate_confidence(parsed_data, query_text)
    except Exception as e:
        print(f"Warning: Could not calculate confidence: {e}")
        confidence_score = 0.5

    # Create enhanced result
    result = QueryResult(
        classification=parsed_data.get('classification', 'structured'),
        filters=normalize_filters(filters),
        constraints=parsed_data.get('constraints', {}),
        quoted_entities=quoted_entities,
        user_intent=parsed_data.get('user_intent', 'retrieve'),
        operation_type=parsed_data.get('operation_type', 'list'),
        confidence_score=confidence_score,
        suggestions=suggestions,
        temporal_info=temporal_info,
        aggregation_fields=aggregation_fields,
        ranking_criteria=ranking_criteria
    )

    return result

def apply_enhanced_synonyms_map(query_text: str, filters: dict, categories: dict, 
                               synonyms_map: dict, threshold: int = 80) -> dict:
    query_lower = query_text.lower()
    filters = normalize_filters(filters)

    for phrase, (cat, mapped_value, confidence) in synonyms_map.items():
        phrase_lower = phrase.lower()
        score = fuzz.partial_ratio(phrase_lower, query_lower)

        # Use both threshold and confidence for matching
        if score >= threshold and confidence >= 0.7:
            if mapped_value in categories.get(cat, []):
                if mapped_value not in filters.get(cat, []):
                    filters.setdefault(cat, []).append(mapped_value)

    # Deduplicate values for each category
    for cat in filters:
        filters[cat] = list(set(filters[cat]))

    return filters

def normalize_filters(filters: dict) -> dict:
    """Ensure every filter value is always a list (never None)."""
    if not filters:
        return {}
    normalized = {}
    for cat, vals in filters.items():
        if vals is None:
            normalized[cat] = []
        elif isinstance(vals, list):
            normalized[cat] = vals
        else:
            # Handle case where vals is a single string
            normalized[cat] = [vals] if vals else []
    return normalized

# ----------------------------
# Enhanced Main Query Parser
# ----------------------------
def parse_query_with_enhanced_tools(query_text):
    tools_schema = build_enhanced_tools_schema(tenant_categories)

    system_message = (
        "You are an advanced query parser that understands natural language queries about content assets. "
        "Extract filters, temporal constraints, user intent, and operation types precisely. "
        "Pay special attention to:\n"
        "1. Temporal expressions (dates, ranges, relative times)\n"
        "2. Aggregation requests (grouping, counting, summarizing)\n"
        "3. Comparison requests (comparing entities or categories)\n"
        "4. Ranking/sorting preferences\n"
        "5. Analytical vs retrieval intent\n"
        "Match terms to the exact tenant categories provided. Use quoted entities for exact matches."
    )

    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": query_text}
        ],
        tools=tools_schema,
        tool_choice={"type": "function", "function": {"name": "parse_query"}}
    )

    # Extract tool output
    tool_output = completion.choices[0].message.tool_calls[0].function.arguments
    parsed_data = json.loads(tool_output)

    # Enhanced post-processing
    return enhanced_post_process_results(parsed_data, tenant_categories, query_text)

# ----------------------------
# Enhanced Example Run
# ----------------------------
if __name__ == "__main__":
    test_queries = [
        'Show me all assets tagged "Marketing" and "Blog Post" in German targeting marketing personas',
        'List all MOFU pages created after January 1st, 2025',
        'What funnel stages do we have the least content for?',
        'Compare Blog posts vs Case Studies created in the last 3 months',
        'Show me trending content from last week grouped by audience',
        'Analyze performance of TOFU content between March and June 2024',
        'Count all content by page type and language',
        'Show me Blog content that would help a business choose the best B2B data provider'
    ]
    
    for query in test_queries:
        print(f"\n{'='*60}")
        print(f"Query: {query}")
        print('='*60)
        
        result = parse_query_with_enhanced_tools(query)
        
        # Pretty print the results
        print(f"Classification: {result.classification}")
        print(f"User Intent: {result.user_intent}")
        print(f"Operation: {result.operation_type}")
        print(f"Confidence: {result.confidence_score:.2f}")
        
        if result.filters:
            print(f"Filters: {json.dumps(result.filters, indent=2)}")
        
        if result.constraints:
            print(f"Constraints: {json.dumps(result.constraints, indent=2)}")
        
        if result.temporal_info['type']:
            print(f"Temporal Info: {json.dumps(result.temporal_info, indent=2)}")
        
        if result.aggregation_fields:
            print(f"Aggregation Fields: {result.aggregation_fields}")
        
        if result.ranking_criteria:
            print(f"Ranking Criteria: {result.ranking_criteria}")
        
        if result.suggestions:
            print("Suggestions:")
            for suggestion in result.suggestions:
                print(f"  - {suggestion}")