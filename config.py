
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
AOI_LAYER_URI = ""       # must be of Polygon geometry type, only the first feature will be picked
INITIAL_RADIUS = 650

METRIC_CRS_EPSG = 32634     # utm 34N
DEFAULT_ENCODING = "utf-8"
LANGUAGE = 'ru'
