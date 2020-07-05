import googlemaps

from dataclass import PoiData

API_KEY = r"AIzaSyAih3qyStqQpzZkB9R0Oo7D9GJWMUW2iWQ"

maps = googlemaps.Client(
    key=API_KEY,
    queries_per_second=3,
    retry_over_query_limit=False,
    timeout=5
)

PAGE_TOKEN = "CqQCFwEAAJ3jgImftOAblwOS8PVXNyjgiKTRT8gl9F1b6kmjDHphBeQd1MCLrIFKuOvMD5Uf1mInCIrhBp2aQd2MqhzqUUGR5GBs1u-kFE1RSoI299Bg5-lJMEckD7qUeZ4fcEj78qDOpse_vz34s0C1bVI8TEsZMgC73yjq54Xw2nfIHtftegsDTV1If_ewQoaUnDNEugNnzJ6C3bqftzR_7OEXDrsGKDzzlnpzag78hD7bqKhhNWZ1Nldd9hp1RlRf1FiqNP7ULGphvUhJxhPiHhb2kgkJmvl12n77FyXRWXIiI_BK9syzE0ZGc7uIrENaGSyaD0Q4IqDTIVvhbzewaT-R53o1GrDdgk6NKOff1LQoHF3hzEphZ6P0460kFsrTDXdxwhIQw1t86KgsBHEOpkMywq9rwBoUPcqD6sOwOF-p9n9EM-1SJL0JRKs"

# response: dict = maps.places_nearby(
#     location=(53.909804, 27.580184),
#     radius=650,
#     open_now=False,
#     rank_by="distance",
#     language='ru',
#     type='cafe'
# )

# valid request, only page token is present
response1: dict = maps.places_nearby(page_token=PAGE_TOKEN)
print(f"First request returned {len(response1['results'])} POIs")

# invalid one
response2: dict = maps.places_nearby(
    location=(53.909804, 27.580184),
    radius=650,
    open_now=False,
    language='ru',
    type='cafe',
    page_token=PAGE_TOKEN
)
print(f"Second request JSON keys: {', '.join(response2.keys())}")

# poi = PoiData.from_response(response)

print("DONE")
