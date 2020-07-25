import json
import multiprocessing as mp
import os
import time
from queue import Empty
from typing import List

import googlemaps

import config
from dataclass import PoiData
from exceptions import *
from geometries.geomworks import Densifier
from tasks import TaskDefinition


MIN_REQUEST_INTERVAL = 60 / config.MAX_REQUESTS_PER_MIN  # seconds


class StatsClass(object):

    previous_requests: int
    requests: int
    pois: int
    tasks: int
    critical_errors: int

    zero_results: int
    request_errors: int
    recursions: int

    avg_request_time: float
    avg_task_time: float

    def __init__(self):

        self.previous_requests = 0
        self.requests = 0
        self.pois = 0
        self.tasks = 0
        self.critical_errors = 0
        self.zero_results = 0
        self.request_errors = 0
        self.recursions = 0

        self.avg_request_time = 0
        self.avg_task_time = 0


class GoogleWorker(mp.Process):

    """ One API key per collector worker. """

    key: str
    finished: bool = None
    tasks_q: mp.Queue
    poi_db_q: mp.Queue
    critical_errors_threshold: int = 10

    __writelock: mp.Lock
    __printlock: mp.Lock

    __info_each: int = 5

    def __init__(self,
                 api_key: str,
                 tasks_q: mp.Queue,
                 tasks_for_record_q: mp.Queue,
                 database_q: mp.Queue,
                 complete_tasks_q: mp.Queue,
                 rawfile_q: mp.Queue,
                 printlock: mp.Lock,
                 writelock: mp.Lock
                 ):

        self.key = api_key
        self.tasks_q: mp.Queue = tasks_q
        self.tasks_database_q = tasks_for_record_q
        self.poi_db_q: mp.Queue = database_q
        self.rawfile_q: mp.Queue = rawfile_q
        self.complete_tasks_q: mp.Queue = complete_tasks_q

        self.__writelock = writelock
        self.__printlock = printlock

        self.finished = False
        self.densifier: Densifier = None   # initialize in a separate thread

        self.stats = StatsClass()
        self.maps = googlemaps.Client(key=api_key,
                                      queries_per_second=3,
                                      retry_over_query_limit=False,
                                      timeout=5)
        self.__ignore_taks = set()

        super().__init__(daemon=True)

    def print(self, *args, **kwargs):
        with self.__printlock:
            print(*args, **kwargs)

    def print_info(self):

        avg_request_ms = int(self.stats.avg_request_time * 1000)
        avg_task_ms = int(self.stats.avg_task_time * 1000)
        errors_cnt = self.stats.critical_errors + self.stats.request_errors
        avg_pois = self.stats.pois / self.stats.tasks
        requests_per_minute = 60 / self.stats.avg_task_time

        with self.__printlock:
            print(f"{self.name}: {self.stats.tasks} tasks | {self.stats.requests} requests | "
                  f"{self.stats.pois} POIs | {avg_pois:.1f} POIs per task avg | "
                  f"{errors_cnt} errors | {requests_per_minute:.1f} req per min | "
                  f"{avg_request_ms} ms per request | {avg_task_ms} ms per task")

    def write_traceback(self, e: Exception):
        with self.__printlock:
            write_traceback(e=e, file=config.TB_FILE)

    def _prepare(self):

        self.densifier = Densifier()
        self._check_api_key()
        self._load_tracker()
        self._update_tracker()

        self.print(f'{self.name} ready. {self.stats.previous_requests} requests made in previous runs | Using API key {self.maps.key}')

    def _load_tracker(self):

        """ Loads requests count from previous sessions. """

        if os.path.isfile(config.TRACKER_JSON):
            with self.__writelock, open(config.TRACKER_JSON, encoding='utf-8-sig') as f:
                d = json.loads(f.read())
            try:
                self.stats.previous_requests = d[self.maps.key]
            except KeyError:
                self.stats.previous_requests = 0
        else:
            self.stats.previous_requests = 0

    def _update_tracker(self):
        with self.__writelock, open(config.TRACKER_JSON, 'r+', encoding='utf-8-sig') as f:
            trackerdict = json.loads(f.read())
            f.seek(0)  # return pointer to the beginning of file before writing'

            trackerdict[self.maps.key] = self.stats.previous_requests + self.stats.requests
            json.dump(trackerdict, f)

    def _check_api_key(self):

        """ Make a sample request that is known to be valid. """
        try:
            self.maps.places_nearby(
                location=(53.909804, 27.580184),
                radius=650,
                open_now=False,
                language=config.LANGUAGE,
                type='cafe',
                # rank_by='distance',        # IMPORTANT: cannot use rank_by and radius options together
                page_token=None,
            )
        except Exception as e:

            with self.__writelock:
                self.print(f'ERROR: bad API key "{self.maps.key}" (tracker={self.stats.previous_requests})\n')
                raise e

    def search_task(self, task: TaskDefinition, page_token: str = None, got_before: int = 0):

        # if config.DEBUG:
        #     self.print(f"{self.name}: got task")
        task.tries += 1

        got_before = got_before if got_before else 0

        location = (task.lat, task.lon)
        _started = time.time()
        self.stats.requests += 1

        # parse here
        try:
            if not page_token:
                resp = self.maps.places_nearby(location=location,
                                               radius=task.radius,
                                               open_now=False,
                                               language=config.LANGUAGE,
                                               type=task.place_type)
            else:
                resp = self.maps.places_nearby(page_token=page_token)

            # info timing
            _elapsed = time.time() - _started
            self.stats.avg_request_time = (_elapsed + self.stats.avg_request_time * self.stats.requests) / (
                        self.stats.requests + 1)

            pois: List[PoiData] = PoiData.from_response(resp=resp)

            # ensure gaps between requests
            if _elapsed < MIN_REQUEST_INTERVAL:
                time.sleep(MIN_REQUEST_INTERVAL - _elapsed)  # wait the rest of time
            time.sleep(0.15)   # as well, sleep for another 150 ms

        except ZeroResultsException:
            return []

        except (googlemaps.exceptions._OverQueryLimit, WastedQuotaException) as e:
            self.write_traceback(e)
            self.finished = True
            raise WastedQuotaException(
                f'{self.name} reached maximum allowed quota. For details, check your Google Developers Account\n'
                f'{self.stats.previous_requests} requests made overall\n'
                f'API key used: {self.maps.key}'
                'Terminating thread...'
            )

        except InvalidRequestException as e:
            self.write_traceback(e)
            self.stats.critical_errors += 1
            self.print(
                    f"ERROR: invalid request exception in {self.name}. API key: {self.maps.key}\n"
                    f"Params: location = {location}, radius={task.radius}, type={task.place_type}, "
                    f"page_token={page_token}, got_before = {got_before} "
            )
            return []

        except RequestDeniedException as e:
            self.write_traceback(e)
            self.stats.critical_errors += 1
            self.print(
                f"ERROR: request denied exception in {self.name}\n"
                f"Params: location = {location}, radius={task.radius}, type={task.place_type}, "
                f"page_token={page_token}, got_before = {got_before} "
                       )
            return []

        except googlemaps.exceptions.ApiError as e:
            self.write_traceback(e)
            self.stats.critical_errors += 1
            self.print(
                f"ERROR: API error occurred in {self.name}\n"
                f"Params: location = {location}, radius={task.radius}, type={task.place_type}, "
                f"page_token={page_token}, got_before = {got_before} "
            )
            raise e         # TODO fix
            return []

        next_page_token = resp.get("next_page_token")
        got_for_the_task = len(pois) + got_before

        # if config.DEBUG:
        #     self.print(f"{self.name}: {got_for_the_task} POIs retrieved (next page = {not not next_page_token})")

        self.stats.pois += len(pois)

        if next_page_token is None:

            # no need to make more requests for this task
            if got_before >= 60:
                # produce tasks for recursion if needed
                if config.DEBUG:
                    self.print(f"{self.name}: submitting task for recursion")
                self.submit_for_recursion(task=task)

            # if config.DEBUG:
            #     self.print(f"{self.name}: task complete")

            return pois
        else:
            # at some point it reaches a state where next_page_token is None and returns all the results
            return pois + self.search_task(
                task=task,
                page_token=next_page_token,
                got_before=got_for_the_task
            )

    def submit_for_recursion(self, task: TaskDefinition):

        """ Makes new tasks for a parent search task that needs recursion. """

        try:
            densified_tasks = self.densifier.densify(task=task)
            for t in densified_tasks:
                self.tasks_q.put(t)     # for processing
                self.tasks_database_q.put(t)    # for record in the database
        except SearchRecursionError:
            pass    # skip if no recursion is possible due to radius being too small (can be changed in config)

    def _get_from_queue_and_do_job(self):

        try:
            #   will wait for N sec and throw Empty exception if nothing found
            task: TaskDefinition = self.tasks_q.get(timeout=1)  # is an instance of TaskDefinition
        except Empty as e:
            time.sleep(1)   # wait for new tasks a bit
            raise e         # will repeat the main loop

        if not isinstance(task, TaskDefinition):
            self.print(f"{self.name}: received poison pill")
            self.finished = True
            raise FinishException('can finish')

        elif task.tries >= config.MAX_TRIES_WITH_TASK:
            return    # discard

        else:
            results = self.search_task(task=task)
            for i in results:
                assert isinstance(i, PoiData), "must be a PoiData instance!"
                self.poi_db_q.put(i)
                self.rawfile_q.put(i)
            self.complete_tasks_q.put(task)

    def run(self):

        self.print(f"{self.name} started")
        self._prepare()
        last_task = time.time()   # time waiting for a new task, will exit if waiting for too long

        while not self.finished:

            if self.stats.critical_errors > self.critical_errors_threshold:
                raise Exception(
                    f"Too many critical errors encountered ({self.stats.critical_errors}). Terminating..."
                )

            try:
                _started = time.time()
                self._get_from_queue_and_do_job()
                _elapsed = time.time() - _started
                self.stats.avg_task_time = (_elapsed + self.stats.tasks * self.stats.avg_task_time) / (self.stats.tasks + 1)
                self.stats.tasks += 1

                last_task = time.time()

                if self.stats.tasks % self.__info_each == 0:
                    self.print_info()

            except Empty:
                continue

            except FinishException:
                self.finished = True
                break

            if self.stats.requests % 4 == 0:    # update tracker every N
                self._update_tracker()

            # exit if waiting for new task for too long
            waiting_uninterrupted = time.time() - last_task
            if waiting_uninterrupted >= config.MAX_WAITING_UNINTERRUPTED:
                break

        # after the loop and before thread can be joined
        self._update_tracker()

        # join thread in main
