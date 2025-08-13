from app.services.query_parser import parse_query
from app.services.retrieval_service import build_structured_match


q = "Show me BOFU content for Financial Services"
parsed = parse_query(q)
print(parsed)
match_stage = build_structured_match(parsed["filters"])

print(match_stage)


# q3 = "Show me BOFU content for Financial Services"
# print(parse_query(q3))
# # {'classification': 'structured', 'filters': {'funnel_stage': 'BOFU', 'industry': 'Financial Services'}}
