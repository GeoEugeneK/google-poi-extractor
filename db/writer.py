import multiprocessing as mp
import os
import sqlite3
import time
from queue import Empty
from typing import List, Set

import config
from dataclass import PoiData
from db.expressions import CREATE_POI_TABLE, DROP_JOBS, DROP_SUCCESS
from exceptions import InvalidPoiDataError, FinishException
from tasks import TaskDefinition


class StatsClass(object):

    total_pois: int
    unique_pois: int
    tasks: int
    inserts: int

    def __init__(self):

        self.total_pois = 0
        self.unique_pois = 0
        self.tasks = 0
        self.inserts = 0


class DatabaseWriter(mp.Process):

    conn: sqlite3.Connection
    cursor: sqlite3.Cursor

    __poi_batch: List[PoiData]
    __jobs_batch: List[TaskDefinition]
    __success_batch: List[TaskDefinition]
    __success_ids: Set[str]      # will hold until the end of session
    __place_ids: Set[str]       # will hold until the end of session - helps avoid repeating POIs

    __write_each: int = 100     # only applies to POIs (PoiData class instances)
    __commit_each: int = 12     # make commit each N inserts

    finished: bool = None
    __printlock: mp.Lock

    def __init__(self, db_file: str, poi_q: mp.Queue, complete_tasks_q: mp.Queue, printlock: mp.Lock):

        self.conn = None
        self.db_file = db_file

        # validate file
        self.__db_basename = os.path.basename(self.db_file)
        self.__parent_dir = os.path.dirname(os.path.abspath(self.db_file))
        assert os.path.exists(
            self.__parent_dir), f"invalid parent directory {self.__parent_dir} for an SQLite database."

        # must be initialized in a separate process for integrity
        # self.make_db_connection()
        # self.cursor = self.conn.cursor()

        self.poi_q = poi_q
        self.complete_tasks_q = complete_tasks_q
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

    def __insert_rows(self, table: str,  rows: List[tuple], column_names: List[str]):

        question_marks = ",".join("?" * len(rows))
        columns = ",".join(column_names)
        sql = f"""INSERT INTO {table} ({columns}) VALUES ({question_marks});"""
        self.cursor.execute(sql=sql)

        self.stats.inserts += 1

        if self.stats.inserts % self.__commit_each == 0:
            self.conn.commit()
            if config.DEBUG:
                self.print(f"{self.name}: database commit")

    def __write_poi_batch(self):

        if self.__poi_batch:

            rows = [
                (
                    poi, "todo"  # TODO rows
                )
                for poi in self.__poi_batch
            ]

            self.stats.unique_pois += len(rows)     # include number of poi inn stats

            column_names = []   # TODO
            self.__insert_rows(table=config.POI_TABLE, rows=rows, column_names=column_names)

            self.__poi_batch = []
            if config.DEBUG:
                self.print(f"{self.name}: inserted {len(rows)} rows into POI table")

        else:
            self.print(f"ERROR: POI batch is empty at the moment, cannot write any data!")

    def __write_jobs_batch(self):

        if self.__jobs_batch:

            rows = [
                (task.task_id, task.lon, task.lat, task.radius, task.place_type) for task in self.__jobs_batch
            ]

            self.stats.tasks += len(rows)  # include number of poi inn stats

            column_names = ["id", "lon", "lat", "radius", "place_type"]
            self.__insert_rows(table=config.JOBS_TABLE, rows=rows, column_names=column_names)

            self.__jobs_batch = []
            if config.DEBUG:
                self.print(f"{self.name}: inserted {len(rows)} rows into jobs table")

        else:
            self.print(f"ERROR: jobs batch is empty at the moment, cannot write any data!")

    def __write_success_batch(self):

        if self.__success_batch:

            rows = [
                (task.task_id, task.lon, task.lat, task.radius, task.place_type) for task in self.__success_batch
            ]

            column_names = ["id", "lon", "lat", "radius", "place_type"]
            self.__insert_rows(table=config.SUCCESS_TABLE, rows=rows, column_names=column_names)

            self.__success_batch = []
            if config.DEBUG:
                self.print(f"{self.name}: inserted {len(rows)} rows into successful tasks table")

        else:
            self.print(f"ERROR: success batch is empty at the moment, cannot write any data!")

    def __include_single_poi_data(self, data: PoiData):

        if not data.is_valid:
            raise InvalidPoiDataError(f"writer received invalid PoiData instance!")

        self.__place_ids.add(data.place_id)
        self.__poi_batch.append(data)

        if len(self.__poi_batch) >= self.__write_each:
            self.__write_poi_batch()

    def __include_success_data(self, task: TaskDefinition):

        self.__success_ids.add(task.task_id)
        self.__success_batch.append(task)

        if len(self.__success_batch) >= self.__write_each:
            self.__write_success_batch()

    def set_success_ids(self, task_ids: List[str]):

        for i in task_ids:
            self.__success_ids.add(i)

    def set_place_ids(self, place_ids: List[str]):

        for i in place_ids:
            self.__place_ids.add(i)


    def cleanup_database(self):

        self.cursor.execute(DROP_JOBS)
        self.cursor.execute(DROP_SUCCESS)

        self.conn.commit()

    def _get_poi_and_process(self):

        try:
            poi: PoiData = self.poi_q.get(timeout=1)
        except Empty as e:
            time.sleep(2)   # wait for new stuff in queue (THIS queue, it is much busier)
            raise e

        if not isinstance(poi, PoiData):
            self.print(f"{self.name}: received poison pill from POI channel")
            self.finished = True
            raise FinishException('can finish')

        self.stats.total_pois += 1      # count all including duplicates
        if poi.place_id not in self.__place_ids:
            self.__include_single_poi_data(data=poi)

    def _get_complete_task_and_process(self):

        try:
            task: TaskDefinition = self.complete_tasks_q.get(timeout=0.1)
        except Empty:
            return   # don't do anything, this queue is not that busy

        if config.DEBUG:
            self.print(f"{self.name}: found completed task")

        if not isinstance(task, TaskDefinition):
            self.print(f"{self.name}: received poison pill from complete tasks channel")
            self.finished = True
            raise FinishException('can finish')

        if task.task_id not in self.__success_ids:
            self.__include_success_data(task=task)
            if config.DEBUG:
                self.print(f"{self.name}: new task, including into the database")


    def run(self) -> None:

        """ Main loop in thread. """

        self.make_db_connection()
        self.cursor = self.conn.cursor()

        while not self.finished:

            # first, check for tasks
            if not self.complete_tasks_q.empty():
                try:
                    self._get_complete_task_and_process()

                except Empty:
                    pass

                except FinishException:
                    self.finished = True
                    break

            # then, check for POI - and wait of nothing found
            if not self.poi_q.empty():
                try:
                    self._get_poi_and_process()

                except Empty:
                    pass

                except FinishException:
                    self.finished = True
                    break
            else:
                time.sleep(2)   # wait for the queue to fill

        # make sure everything is recorded
        if self.__poi_batch:
            self.__write_poi_batch()

        if self.__jobs_batch:
            self.__write_jobs_batch()

        if self.__success_batch:
            self.__write_success_batch()

        # when done
        self.cleanup_database()
        self.conn.close()

        self.print(f"{self.name} finished")
        self.print_info()
