import googlemaps
from dataclass import PoiData

API_KEY = r"AIzaSyAih3qyStqQpzZkB9R0Oo7D9GJWMUW2iWQ"

maps = googlemaps.Client(
    key=API_KEY,
    queries_per_second=3,
    retry_over_query_limit=False,
    timeout=5
)

response: dict = maps.places_nearby(
    location=(53.909804, 27.580184),
    radius=650,
    open_now=False,
    rank_by="distance",
    language='ru',
    type='cafe'
)

poi = PoiData.from_response(response)

print("DONE")
