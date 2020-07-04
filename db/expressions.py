from config import POI_TABLE, JOBS_TABLE, SUCCESS_TABLE


#   fields:
#   x  y  id  place_id  name  rating  scope  user_ratings_total  vicinity  types  price_level  formatted_address

CREATE_POI_TABLE = f"""
CREATE TABLE {POI_TABLE} (
                    place_id TEXT PRIMARY KEY,
                    id TEXT,
                    lon FLOAT,
                    lat FLOAT,
                    name TEXT,
                    rating FLOAT,
                    scope TEXT,
                    user_ratings_total INTEGER,
                    vicinity TEXT,
                    types TEXT,
                    price INTEGER,
                    /*  Extractor metadata  */
                    date_obtained TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    needs_recurse BOOLEAN
                    
                    /*   NEW    */
                    business_status TEXT, 
                    
                    
                    
                );
"""


CREATE_TASKS_TABLE = f"""
DROP TABLE IF EXISTS {JOBS_TABLE};      /*  Drop data from previous session  */
CREATE TABLE {JOBS_TABLE} (
                    id TEXT PRIMARY KEY,        -- random token
                    lon FLOAT,
                    lat FLOAT,
                    radius FLOAT,
                    place_type TEXT
                );
"""

CREATE_SUCCESS_TABLE = f"""
DROP TABLE IF EXISTS {SUCCESS_TABLE};   /*  Drop data from previous session  */
CREATE TABLE {SUCCESS_TABLE} (
                        id TEXT PRIMARY KEY,    -- random token
                        lon FLOAT,
                        lat FLOAT,
                        radius FLOAT,
                        place_type TEXT
                    );
"""

GET_UNFINISHED_FROM_PREVIOUS_SESSION = f"""
SELECT id, lon, lat, radius, place_type 
FROM {JOBS_TABLE} 
WHERE id NOT IN (
    SELECT id
    FROM {SUCCESS_TABLE}
);
"""

GET_POI_IDS_FROM_PREVIOUS_SESSIONS = f"""
SELECT DISTINCT place_id
FROM {POI_TABLE};
"""

DROP_JOBS = f"""
DROP TABLE IF EXISTS {JOBS_TABLE};
"""

DROP_SUCCESS = f"""
DROP TABLE IF EXISTS {SUCCESS_TABLE};
"""
