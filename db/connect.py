import os
import sqlite3


def make_db_connection(db_file: str):

    """ Creates a database connection to a SQLite database. Raises error when fails to connect. """

    print(f"INFO: using sqlite3 lib v.{sqlite3.version}")   # info

    conn = None
    db_basename = os.path.basename(db_file)
    parent_dir = os.path.dirname(os.path.abspath(db_file))

    print(f'INFO: database workspace is "{parent_dir}"')
    if os.path.isfile(db_file):
        print(f'INFO: connecting to existing "{db_basename}" database...')
    else:
        print(f'INFO: file "{db_basename}" does not exists, new database will be created')

    try:
        conn = sqlite3.connect(db_file)
        print(f"INFO: database connection established successfully!")
        return conn

    except sqlite3.Error as e:
        print(f"CRITICAL: failed to establish database connecting to {db_file}")
        if conn:
            conn.close()
        raise e