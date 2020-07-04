import multiprocessing as mp
import os
import sqlite3
import time
from typing import List, Set

from queue import Empty
from dataclass import PoiData
from db.expressions import CREATE_POI_TABLE, DROP_JOBS, DROP_SUCCESS
from exceptions import InvalidPoiDataError, FinishException
from tasks import TaskDefinition

print(f"INFO: using sqlite3 lib v.{sqlite3.version}")


class StatsClass(object):

    total_pois: int
    unique_pois: int
    tasks: int

    def __init__(self):

        self.total_pois = 0
        self.unique_pois = 0
        self.tasks = 0


class DatabaseWriter(mp.Process):

    conn: sqlite3.Connection
    cursor: sqlite3.Cursor

    __poi_batch: List[PoiData]
    __jobs_batch: List[TaskDefinition]
    __success_batch: List[TaskDefinition]
    __success_ids: Set[str]      # will hold until the end of session
    __place_ids: Set[str]       # will hold until the end of session - helps avoid repeating POIs

    __write_each: int = 100     # only applies to POIs (PoiData class instances)

    finished: bool = None
    __printlock: mp.Lock

    def __init__(self, db_file: str, results_queue: mp.Queue, printlock: mp.Lock):

        self.conn = None
        self.db_file = db_file

        # validate file
        self.__db_basename = os.path.basename(self.db_file)
        self.__parent_dir = os.path.dirname(os.path.abspath(self.db_file))
        assert os.path.exists(
            self.__parent_dir), f"invalid parent directory {self.__parent_dir} for an SQLite database."

        self.make_db_connection()
        self.cursor = self.conn.cursor()

        self.queue = results_queue
        self.__printlock = printlock

        self.__poi_batch = []
        self.__jobs_batch = []
        self.__success_batch = []
        self.__success_ids = set()
        self.__place_ids = set()

        self.finished = False
        self.stats = StatsClass()

        super().__init__(daemon=True)

    def print(self, *args, **kwargs):
        with self.__printlock:
            print(*args, **kwargs)

    def print_info(self):

        percent_unique = self.stats.unique_pois - self.stats.total_pois * 100
        self.print(
            f"Total {self.stats.total_pois} places collected, "
            f"{self.stats.unique_pois} ({percent_unique:.1f}%) of them unique.\n"
            f"{self.stats.tasks} tasks completed in this session"
        )

    def make_db_connection(self):

        """ Creates a database connection to a SQLite database. Raises error when fails to connect. """

        conn = None

        self.print(f'INFO: database workspace is "{self.__parent_dir}"')
        if os.path.isfile(self.db_file):
            self.print(f'INFO: connecting to existing "{self.__db_basename}" database...')
        else:
            self.print(f'INFO: file "{self.__db_basename}" does not exists, new database will be created')

        try:
            conn = sqlite3.connect(self.db_file)
            self.print(f"INFO: database connection established successfully!")
        except sqlite3.Error as e:
            self.print(f"CRITICAL: failed to establish database connecting to {self.db_file}")
            if conn:
                conn.close()
            raise e
        finally:
            self.conn = conn

    def create_table(self):

        """ Creates table that  """

        self.cursor.execute(sql=CREATE_POI_TABLE)
        self.conn.commit()

    def __write_poi_batch(self):

        if self.__poi_batch:
            # TODO
            self.__poi_batch = []
        else:
            self.print(f"ERROR: POI batch is empty at the moment, cannot write any data!")

    def __write_jobs_batch(self):

        if self.__jobs_batch:

            rows = [
                (task.task_id, task.lon, task.lat, task.radius, task.place_type) for task in self.__jobs_batch
            ]

            # TODO
            self.__jobs_batch = []
        else:
            self.print(f"ERROR: jobs batch is empty at the moment, cannot write any data!")

    def __write_success_batch(self):

        if self.__success_batch:

            rows = [
                (task.task_id, task.lon, task.lat, task.radius, task.place_type) for task in self.__success_batch
            ]

            # TODO
            self.__success_batch = []
        else:
            self.print(f"ERROR: success batch is empty at the moment, cannot write any data!")


    def __include_single_poi_data(self, data: PoiData):

        if not data.is_valid:
            raise InvalidPoiDataError(f"writer received invalid PoiData instance!")

        self.__place_ids.add(data.place_id)
        self.__poi_batch.append(data)


    # TODO remove
    # def include_pois(self, data: List[PoiData]):
    #
    #     assert isinstance(data, list) and len(data) > 0, "received invalid PoiData array!"
    #
    #     for i in data:
    #         if i.place_id not in self.__place_ids:
    #             # limit to unique
    #             self.__include_single_poi_data(data=i)
    #
    #     if len(self.__poi_batch) >= self.__write_each:
    #         # first write POI data and only then acknowledge jobs as successful
    #         self.__write_poi_batch()
    #         self.__write_success_batch()

    def set_success_ids(self, task_ids: List[str]):

        for i in task_ids:
            self.__success_ids.add(i)

    def set_place_ids(self, place_ids: List[str]):

        for i in place_ids:
            self.__place_ids.add(i)

    def include_success_data(self, data: TaskDefinition):

        if data.task_id not in self.__success_ids:
            self.__success_ids.add(data.task_id)
            self.__success_batch.append(data)

    def cleanup_database(self):

        self.cursor.execute(DROP_JOBS)
        self.cursor.execute(DROP_SUCCESS)

        self.conn.commit()

    def get_from_queue_and_do_job(self):

        try:
            poi: PoiData = self.queue.get(timeout=1)
        except Empty as e:
            time.sleep(2)   # wait for new stuff in queue
            raise e

        if not isinstance(poi, PoiData):
            self.print(f"{self.name}: received poison pill")
            self.finished = True
            raise FinishException('can finish')

        if poi.place_id not in self.__place_ids:
            self.__include_single_poi_data(data=poi)


    def run(self) -> None:

        while not self.finished:

            try:
                self.get_from_queue_and_do_job()

            except Empty:
                continue

            except FinishException:
                self.finished = True
                break

        self.cleanup_database()
        self.conn.close()

        self.print(f"{self.name} finished")
        self.print_info()
