#   fields:
#   x  y  id  place_id  name  rating  scope  user_ratings_total  vicinity  types  price_level  formatted_address

CREATE_POI_TABLE = """
CREATE TABLE IF NOT EXISTS projects (
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
                                    );
"""