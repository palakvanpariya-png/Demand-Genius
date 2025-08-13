from bson import ObjectId
from app.services.query_parser import parse_query

TENANT_ID = ObjectId("6875f3afc8337606d54a7f37")

q1 = "Show me all content for Individual Investors in Healthcare"
q2 = "What funnel stages do we have the least content for?"

print(parse_query(q1, tenant_id=TENANT_ID))
# {'classification': 'structured', 'filters': {'Primary Audience': ['Individual Investors'], 'Industry': ['Healthcare']}}

print(parse_query(q2, tenant_id=TENANT_ID))
# {'classification': 'advisory', 'filters': {}}
