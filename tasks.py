import secrets


class TaskDefinition(object):

    """ Super simple container for a task. """

    lon: float          # EPSG:4326
    lat: float          # EPSG:4326
    radius: float
    place_type: str

    tries: int
    task_id: str        # initialize internally if not provided

    def __init__(self, lon: float, lat: float, radius: float, place_type: str, task_id: str = None):
        self.lon = lon
        self.lat = lat
        self.radius = radius
        self.place_type = place_type

        self.tries = 0
        self.task_id = task_id if task_id else secrets.token_hex(32)    # unique identifier
