class SearchTaskDefinition(object):

    lon: float          # EPSG:4326
    lat: float          # EPSG:4326
    radius: float
    place_type: str

    def __init__(self, lon: float, lat: float, radius: float, place_type: str):
        self.lon = lon
        self.lat = lat
        self.radius = radius
        self.place_type = place_type
