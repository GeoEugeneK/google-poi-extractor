import sqlite3
import os
from typing import List

from dataclass import PoiData
from exceptions import InvalidPoiDataError
from db.expressions import CREATE_POI_TABLE


print(f"INFO: using sqlite3 lib v.{sqlite3.version}")


class DatabaseWriter(object):

    conn: sqlite3.Connection
    cursor: sqlite3.Cursor

    __batch: List[PoiData]
    __write_each: int = 100

    def __init__(self, db_file: str):

        self.conn = None
        self.db_file = db_file

        # validate file
        self.__db_basename = os.path.basename(self.db_file)
        self.__parent_dir = os.path.dirname(os.path.abspath(self.db_file))
        assert os.path.exists(
            self.__parent_dir), f"invalid parent directory {self.__parent_dir} for an SQLite database."

        self.make_db_connection()
        self.cursor = self.conn.cursor()

    def make_db_connection(self):

        """ Creates a database connection to a SQLite database. Raises error when fails to connect. """

        conn = None

        print(f'INFO: database workspace is "{self.__parent_dir}"')
        if os.path.isfile(self.db_file):
            print(f'INFO: connecting to existing "{self.__db_basename}" database...')
        else:
            print(f'INFO: file "{self.__db_basename}" does not exists, new database will be created')

        try:
            conn = sqlite3.connect(self.db_file)
            print(f"INFO: database connection established successfully!")
        except sqlite3.Error as e:
            print(f"CRITICAL: failed to establish database connecting to {self.db_file}")
            if conn:
                conn.close()
            raise e
        finally:
            self.conn = conn

    def create_table(self):

        """ Creates table that  """

        self.cursor.execute(sql=CREATE_POI_TABLE)
        self.conn.commit()

    def __write_batch(self):

        if self.__batch:
            # TODO
            pass
        else:
            print(f"ERROR: batch is empty at the moment, cannot write any data!")

    def include_data(self, data: PoiData):

        if not data.is_valid:
            raise InvalidPoiDataError(f"writer received invalid PoiData instance!")

        self.__batch.append(data)

        if len(self.__batch) >= self.__write_each:
            self.__write_batch()

