import sqlite3
import os

print(f"INFO: using sqlite3 lib v.{sqlite3.version}")


def make_db_connection(db_file):

    """ Creates a database connection to a SQLite database. Raises error when fails to connect. """

    # validate file
    basename = os.path.basename(db_file)
    parent_dir = os.path.dirname(os.path.abspath(db_file))
    assert os.path.exists(parent_dir), f"invalid parent directory {parent_dir} for an SQLite database."

    conn = None

    print(f'INFO: database workspace is "{parent_dir}"')
    if os.path.isfile(db_file):
        print(f'INFO: connecting to existing "{basename}" database...')
    else:
        print(f'INFO: file "{basename}" does not exists, new database will be created')

    try:
        conn = sqlite3.connect(db_file)
        print(f"INFO: database connection established successfully!")
    except sqlite3.Error as e:
        print(f"CRITICAL: failed to establish database connecting to {db_file}")
        if conn:
            conn.close()
        raise e
    finally:
        return conn


# making sure it works
if __name__ == '__main__':
    conn = make_db_connection(r"D:\gis_works3\McKinsey\data\check.sqlite3")