
DATABASE = "poi.sqlite3"        # SQLite version >= 3.1 database
KEYS_FILE = "keys.txt"
TRACKER_JSON = "tracker.json"
SEARCH_TYPES_FILE = "search_types.txt"
VALID_TYPES_FILE = "valid_types.txt"

# tables
POI_TABLE = "poi"      # valid sqlite table name, will be placed in public schema by default
JOBS_TABLE = "jobs"
SUCCESS_TABLE = "success"

RESUME = False      # if True, will pick up where it stopped in the last session
TASKS_FILE = f""    # additional parameter needed to resume correctly. Ignored when "RESUME" is set to False

RAW_DATA_FOLDER = "./data/"     # raw response JSONs
RESPONSE_JSON_EXTENSION = ".json"

# must be of Polygon geometry type, only the first feature will be picked
AOI_LAYER_URI = "D:/gis_works2/buildingsOSM.gpkg|layername=border_wgs84"        # simply put, city boundaries

MAX_TRIES_WITH_TASK = 3
MAX_REQUESTS_PER_MIN = 20
INITIAL_RADIUS = 650
MIN_ALLOWED_RADIUS = 6     # meters    |   avoid infinite search point recursion!

METRIC_CRS_EPSG = 32635     # utm 34N
DEFAULT_ENCODING = "utf-8"
LANGUAGE = 'ru'

DEBUG = False    # will suppress some messages when disabled
TB_FILE = "./tracebacks/tb.txt"
