# For better understanding, refer to expressions.py and see CREATE TABLE ... expression
from typing import List

from exceptions import *


NONE_TYPE = type(None)


class PoiData(object):

    place_id: str
    id: str
    lon: float
    lat: float
    name: str
    rating: float
    business_status: str
    scope: str
    user_ratings_total: int
    vicinity: str
    types: str
    price: int

    is_valid: bool = False

    # timestamp is redundant, will be set default by the DB

    def __init__(self,
                 place_id, id, lon, lat,
                 name, rating, business_status, scope,
                 user_ratings_total, vicinity, types, price,
                 json: dict):

        self.place_id = place_id
        self.id = id
        self.lon = lon
        self.lat = lat
        self.name = name
        self.rating = rating
        self.business_status = business_status
        self.scope = scope
        self.user_ratings_total = user_ratings_total
        self.vicinity = vicinity
        self.types = types
        self.price = price
        self.json = json

        self.validate()

        self.is_valid = True    # only set after validated

    def validate(self) -> None:

        """ Raises InvalidPoiDataError if validation fails"""

        try:
            assert self.__item_valid(item=self.place_id, itemtype=str, not_null=True), \
                f'invalid value for place_id "{self.place_id}" (type {type(self.place_id)})'

            assert self.__item_valid(item=self.id, itemtype=str, not_null=True), \
                f'invalid value for id "{self.id}" (type {type(self.id)})'

            assert self.__item_valid(item=self.name, itemtype=str, not_null=True), \
                f'invalid value for name "{self.name}" (type {type(self.name)})'

            assert self.__item_valid(item=self.scope, itemtype=str, not_null=True), \
                f'invalid value for scope "{self.scope}" (type {type(self.scope)})'

            assert self.__item_valid(item=self.vicinity, itemtype=str, not_null=True), \
                f'invalid value for vicinity "{self.vicinity}" (type {type(self.vicinity)})'

            assert self.__item_valid(item=self.types, itemtype=str, not_null=True), \
                f'invalid value for types "{self.types}" (type {type(self.types)})'

            assert self.__item_valid(item=self.business_status, itemtype=str, not_null=True), \
                f'invalid value for business_status "{self.business_status}" (type {type(self.business_status)})'

            # numeric

            assert self.__item_valid(item=self.lon, itemtype=float, not_null=True), \
                f'invalid value for lon "{self.lon}" (type {type(self.lon)})'

            assert self.__item_valid(item=self.lat, itemtype=float, not_null=True), \
                f'invalid value for lat "{self.lat}" (type {type(self.lat)})'

            assert self.__item_valid(item=self.rating, itemtype=(float, int, NONE_TYPE), not_null=True), \
                f'invalid value for rating "{self.rating}" (type {type(self.rating)})'

            assert self.__item_valid(item=self.user_ratings_total, itemtype=(int, NONE_TYPE), not_null=False), \
                f'invalid value for user_ratings_total "{self.user_ratings_total}" (type {type(self.user_ratings_total)})'

            assert self.__item_valid(item=self.price, itemtype=(int, NONE_TYPE), not_null=False), \
                f'invalid value for price "{self.price}" (type {type(self.price)})'

            # price_level can be None (NULL)

        except AssertionError as e:
            raise InvalidPoiDataError(str(e))  # raise error with the same message

    def as_row(self):

        """ Returns all data items as table row values (tuple-like) """
        return [
            self.place_id, self.id, self.lon, self.lat,
            self.name, self.rating, self.scope, self.user_ratings_total,
            self.vicinity, self.types, self.price
        ]

    @staticmethod
    def __item_valid(item, itemtype: [type, List[type]], not_null: bool) -> bool:

        if not_null:
            return True if item and isinstance(item, itemtype) else False
        else:
            return True if isinstance(item, itemtype) else False

    @classmethod
    def __parse_single_poi(cls, poi: dict):

        try:
            place_id = poi["place_id"]
            _id = poi["id"]

            geom = poi["geometry"]
            lonlat = geom["location"]
            lon = lonlat["lng"]
            lat = lonlat["lat"]

            name = poi["name"]
            rating = poi.get("rating")      # can be NULL (None)
            business_status = poi["business_status"]
            scope = poi["scope"]
            user_ratings_total = poi.get("user_ratings_total")      # can be NULL (None)
            vicinity = poi["vicinity"]
            types = poi["types"]
            price = poi.get("price_level")      # can be NULL (None)

        except KeyError as e:
            raise ResponseParsingError(f"failed to parse data for a POI - dict item {e} could not be found")

        return PoiData(
            place_id=place_id,
            id=_id,
            lon=lon,
            lat=lat,
            name=name,
            rating=rating,
            business_status=business_status,
            scope=scope,
            user_ratings_total=user_ratings_total if user_ratings_total else 0,
            vicinity=vicinity,
            types=", ".join(types),
            price=price,
            json=poi
        )

    @classmethod
    def from_response(cls, resp: dict):

        """
        Parses valid response JSON (dict) as a list of atomic POI data items.
        Must be used instead of default constructor.
        """

        # first, validate
        status = resp["status"]

        if status == "OK":
            pass    # everything is OK
        elif status == "ZERO_RESULTS":
            raise ZeroResultsException
        elif status == "OVER_QUERY_LIMIT":
            raise WastedQuotaException
        elif status == 'REQUEST_DENIED':
            raise RequestDeniedException
        elif status == 'INVALID_REQUEST':
            raise InvalidRequestException
        else:
            raise Exception(f"unexpected response status {status}")

        try:
            results = resp["results"]
            assert len(results) > 0, "no results found in response"

        except KeyError as e:
            raise ResponseParsingError(f'could not get item "{e}" from response JSON')

        except AssertionError as e:
            raise ResponseParsingError(str(e))

        return [
            cls.__parse_single_poi(poi=x) for x in results
        ]
