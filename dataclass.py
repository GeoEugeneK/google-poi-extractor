# For better understanding, refer to expressions.py and see CREATE TABLE ... expression


class InvalidPoiDataError(Exception):
    pass


class PoiData(object):

    place_id: str
    id: str
    lon: float
    lat: float
    name: str
    rating: float
    scope: str
    user_ratings_total: int
    vicinity: str
    types: str
    price: int

    is_valid: bool = False

    # timestamp is redundant, will be set default by the DB

    def __init__(self, place_id, id, lon, lat, name, rating, scope, user_ratings_total, vicinity, types, price):

        self.place_id = place_id
        self.id = id
        self.lon = lon
        self.lat = lat
        self.name = name
        self.rating = rating
        self.scope = scope
        self.user_ratings_total = user_ratings_total
        self.vicinity = vicinity
        self.types = types
        self.price = price

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

        except AssertionError as e:
            raise InvalidPoiDataError(str(e))  # raise error with the same message

    def get_row(self):

        """ Returns all data items as table row values (tuple-like) """
        return [
            self.place_id, self.id, self.lon, self.lat,
            self.name, self.rating, self.scope, self.user_ratings_total,
            self.vicinity, self.types, self.price
        ]

    @staticmethod
    def __item_valid(item, itemtype: type, not_null: bool) -> bool:

        if not_null:
            return True if item and isinstance(item, itemtype) else False
        else:
            return True if isinstance(item, itemtype) else False

    @staticmethod
    def from_response(resp):
        pass  # TODO
