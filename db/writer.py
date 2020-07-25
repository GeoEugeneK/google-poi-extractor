import multiprocessing as mp
import os
import sqlite3
import time
from queue import Empty
from typing import List, Set

import config
from dataclass import PoiData
from db.expressions import CREATE_POI_TABLE, CREATE_SUCCESS_TABLE, CREATE_JOBS_TABLE, DROP_JOBS, DROP_SUCCESS
from exceptions import InvalidPoiDataError, FinishException
from tasks import TaskDefinition

SUCCESS_COLUMN_NAMES = ["id", "lon", "lat", "radius", "place_type"]
JOBS_COLUMN_NAMES = ["id", "lon", "lat", "radius", "place_type"]

POI_WRITEABLE_COLUMNS = ['place_id',
                         'id',
                         'lon',
                         'lat',
                         'name',
                         'rating',
                         'scope',
                         'user_ratings_total',
                         'vicinity',
                         'types',
                         'price',
                         'business_status']


class WriterStatsClass(object):

    total_pois: int
    unique_pois: int
    tasks: int
    inserts: int
    commits: int

    def __init__(self):

        self.total_pois = 0
        self.unique_pois = 0
        self.tasks = 0
        self.inserts = 0
        self.commits = 0


class DatabaseWriter(mp.Process):

    conn: sqlite3.Connection
    cursor: sqlite3.Cursor

    __poi_batch: List[PoiData]
    # __initial_jobs: List[TaskDefinition]
    __jobs_batch: List[TaskDefinition]
    __success_batch: List[TaskDefinition]
    __success_ids: Set[str]      # will hold until the end of session
    __place_ids: Set[str]       # will hold until the end of session - helps avoid repeating POIs

    finished: bool = None
    __printlock: mp.Lock

    __write_each: int = 1     # only applies to POIs (PoiData class instances). using 1 will insert each row separately
    __commit_each: int = 12     # make commit each N inserts


    def __init__(self, db_file: str, poi_q: mp.Queue, tasks_q: mp.Queue, complete_tasks_q: mp.Queue, printlock: mp.Lock):

        self.conn = None
        self.db_file = db_file

        # validate file
        self.__db_basename = os.path.basename(self.db_file)
        self.__parent_dir = os.path.dirname(os.path.abspath(self.db_file))
        assert os.path.exists(
            self.__parent_dir   ), f"invalid parent directory {self.__parent_dir} for an SQLite database."

        # must be initialized in a separate process for integrity
        # self.make_db_connection()
        # self.cursor = self.conn.cursor()

        self.poi_q = poi_q
        self.pending_tasks_q = tasks_q
        self.complete_tasks_q = complete_tasks_q
        self.__printlock = printlock

        self.__poi_batch = []
        self.__jobs_batch = []
        self.__success_batch = []
        self.__success_ids = set()
        self.__place_ids = set()

        self.finished = False
        self.stats = WriterStatsClass()

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

    def set_initial_jobs(self, jobs: List[TaskDefinition]):
        self.__jobs_batch.extend(jobs)

    def set_success_ids(self, task_ids: List[str]):

        """ When resuming, set success IDs to avoid writing duplicates. """

        for i in task_ids:
            self.__success_ids.add(i)

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

    def __insert_rows(self, table: str,  rows: List[tuple], column_names: List[str]):

        question_marks = ",".join("?" * len(column_names))
        columns = ",".join(column_names)
        sql = f"""INSERT INTO {table}({columns}) VALUES ({question_marks});"""

        # TODO remove debug
        if config.DEBUG:
            with open("./test/insert.sql", "w", encoding="utf-8-sig") as f:
                f.write(f"{sql}\n\n\nVALUES:\n\n{str(rows)}")
            print("DEBUG writer: wrote insert expression.")

        self.cursor.executemany(sql, rows)

        self.stats.inserts += 1     # keep track of inserts here, regardless of target table

        if config.DEBUG:
            self.conn.commit()  # commit every insert when debugging

        # number of inserts is non-zero at this point in any case
        elif self.stats.inserts % self.__commit_each == 0:
            self.conn.commit()
            self.stats.commits += 1
            self.print(f"{self.name}: database commit ({self.stats.commits} total)")

    def __write_poi_batch(self):

        if self.__poi_batch:

            rows = [
                (
                    x.place_id, x.id, x.lon, x.lat, x.name,
                    x.rating, x.scope, x.user_ratings_total,
                    x.vicinity, x.types, x.price, x.business_status
                )
                for x in self.__poi_batch
            ]

            self.stats.unique_pois += len(rows)     # include number of poi inn stats

            self.__insert_rows(table=config.POI_TABLE, rows=rows, column_names=POI_WRITEABLE_COLUMNS)

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

            self.stats.tasks += len(rows)  # include number of poi in stats

            # column_names = ["id", "lon", "lat", "radius", "place_type"]
            self.__insert_rows(table=config.JOBS_TABLE, rows=rows, column_names=JOBS_COLUMN_NAMES)

            self.__jobs_batch = []      # clean the batch
            if config.DEBUG:
                self.print(f"{self.name}: inserted {len(rows)} rows into jobs table")

        else:
            self.print(f"ERROR: jobs batch is empty at the moment, cannot write any data!")

    def __write_success_batch(self):

        if self.__success_batch:

            rows = [
                (task.task_id, task.lon, task.lat, task.radius, task.place_type) for task in self.__success_batch
            ]

            self.__insert_rows(table=config.SUCCESS_TABLE, rows=rows, column_names=SUCCESS_COLUMN_NAMES)

            self.__success_batch = []
            if config.DEBUG:
                self.print(f"{self.name}: inserted {len(rows)} rows into successful tasks table")

        else:
            self.print(f"ERROR: success batch is empty at the moment, cannot write any data!")

    def __include_single_poi_data(self, data: PoiData):

        """ Make sure that place ID is not duplicate, before calling the function. """

        if not data.is_valid:
            raise InvalidPoiDataError(f"writer received invalid PoiData instance!")

        self.__place_ids.add(data.place_id)     # avoid writing duplicates
        self.__poi_batch.append(data)

        if len(self.__poi_batch) >= self.__write_each:
            self.__write_poi_batch()

    def __include_successful_task(self, task: TaskDefinition):

        self.__success_ids.add(task.task_id)
        self.__success_batch.append(task)

        if len(self.__success_batch) >= self.__write_each:
            self.__write_success_batch()

    def __include_pending_task(self, task: TaskDefinition):

        self.__jobs_batch.append(task)

        if len(self.__jobs_batch) >= self.__write_each:
            self.__write_jobs_batch()

    def set_place_ids(self, place_ids: List[str]):

        for i in place_ids:
            self.__place_ids.add(i)


    def cleanup_database(self):

        self.cursor.execute(DROP_JOBS)
        self.cursor.execute(DROP_SUCCESS)

        self.conn.commit()

    def _get_poi_and_process(self):

        try:
            poi: PoiData = self.poi_q.get(timeout=0.1)
        except Empty as e:
            # time.sleep(0.75)   # wait for new stuff in queue (THIS queue, it is much busier)  || NOT NEEDED, wait in loop
            raise e     # will be caught in loop in run() method

        if not isinstance(poi, PoiData):
            self.print(f"{self.name}: received poison pill via POI channel")
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
            self.print(f"{self.name}: received poison pill via complete tasks channel")
            self.finished = True
            raise FinishException('can finish')

        if task.task_id not in self.__success_ids:
            self.__include_successful_task(task=task)
            if config.DEBUG:
                self.print(f"{self.name}: new complete task, including into the database")

    def _get_pending_task_and_process(self):

        try:
            task: TaskDefinition = self.pending_tasks_q.get(timeout=0.1)
        except Empty:
            return   # don't do anything, this queue is not that busy

        if config.DEBUG:
            self.print(f"{self.name}: found pending task")

        if not isinstance(task, TaskDefinition):
            self.print(f"{self.name}: received poison pill via pending tasks channel")
            self.finished = True
            raise FinishException('can finish')

        # all of these will be unique
        self.__include_pending_task(task=task)
        if config.DEBUG:
            self.print(f"{self.name}: pending task inlcuded into the database")


    def run(self) -> None:

        """ Main loop in thread. """

        # initialize connection in a separate thread
        self.make_db_connection()
        self.cursor = self.conn.cursor()
        self.__write_jobs_batch()
        self.conn.commit()      # commit initial jobs
        self.print(f"{self.name}: initial jobs written")

        last_task = time.time()

        while not self.finished:

            """     NOTE: the following order of calls guarantees 
                    that all POIs will be recorded before acknowledging task success.       """

            # check for scheduled tasks. if there are any, process them until there are none and move on
            while not self.pending_tasks_q.empty():
                try:
                    self._get_pending_task_and_process()
                    last_task = time.time()

                except Empty:
                    break

                except FinishException:
                    self.finished = True
                    break

            # then, check for POIs - and wait if nothing found.
            # do not write successful tasks before writing collected POIs!
            # this
            while not self.poi_q.empty():
                try:
                    self._get_poi_and_process()
                    last_task = time.time()

                except Empty:
                    break

                except FinishException:
                    self.finished = True
                    break
            else:
                time.sleep(1.0)   # wait for the queue to fill

            #  check for complete tasks. will only appear after any POIs have been collected
            while not self.complete_tasks_q.empty():
                try:
                    self._get_complete_task_and_process()
                    last_task = time.time()

                except Empty:
                    break

                except FinishException:
                    self.finished = True
                    break

            # exit if waiting for new task for too long
            waiting_uninterrupted = time.time() - last_task
            if waiting_uninterrupted >= config.MAX_WAITING_UNINTERRUPTED:
                break

        self.print(f"{self.name} ready to finish...")

        # make sure everything is recorded when quitting
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
