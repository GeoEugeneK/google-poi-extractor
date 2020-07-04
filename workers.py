import json
import os
import time

import multiprocessing as mp
from queue import Empty
from typing import List

import googlemaps

# from config import TRACKER_JSON, LANGUAGE
import config
from dataclass import PoiData
from geometries.geomworks import Densifier
from tasks import TaskDefinition

from exceptions import *


class StatsClass(object):

    previous_requests: int
    requests: int
    tasks: int
    critical_errors: int

    zero_results: int
    request_errors: int
    recursions: int

    avg_request_time: float
    avg_task_time: float

    def __init__(self):

        self.requests = 0
        self.critical_errors = 0
        self.zero_results = 0
        self.request_errors = 0
        self.recursions = 0

        self.avg_request_time = 0
        self.avg_task_time = 0


class GoogleWorker(mp.Process):

    """ One API key per collector worker. """

    key: str
    job_done: bool = None
    tasks_q: mp.Queue
    database_q: mp.Queue
    critical_errors_threshold: int = 10

    __writelock: mp.Lock
    __printlock: mp.Lock

    def __init__(self,
                 api_key: str,
                 tasks_q: mp.Queue,
                 database_q: mp.Queue,
                 complete_tasks_q: mp.Queue,
                 rawfile_q: mp.Queue,
                 printlock: mp.Lock,
                 writelock: mp.Lock
                 ):

        self.key = api_key
        self.tasks_q: mp.Queue = tasks_q
        self.database_q: mp.Queue = database_q
        self.rawfile_q: mp.Queue = rawfile_q
        self.complete_tasks_q: mp.Queue = complete_tasks_q

        self.__writelock = writelock
        self.__printlock = printlock

        self.job_done = False
        self.densifier = Densifier()

        self.stats = StatsClass()
        self.maps = googlemaps.Client(key=api_key,
                                      queries_per_second=3,
                                      retry_over_query_limit=False,
                                      timeout=5)

        super().__init__(daemon=True)

    def print(self, *args, **kwargs):
        with self.__printlock:
            print(*args, **kwargs)

    def print_info(self):

        avg_request_ms = int(self.stats.avg_request_time * 1000)
        avg_task_ms = int(self.stats.avg_task_time * 1000)
        errors_cnt = self.stats.critical_errors + self.stats.request_errors

        with self.__printlock:
            print(f"{self.name}: {self.stats.tasks} tasks | {self.stats.requests} requests | "
                  f"{errors_cnt} errors | {avg_request_ms} ms per request | {avg_task_ms} ms per task")

    def _prepare(self):

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
                self.print(f'ERROR: bad API key "{self.maps.key}" (tracker={self.stats.previous_requests})')
                raise e

    def search_task(self, task: TaskDefinition, page_token: str = None, got_before: int = 0):

        page_token = page_token if page_token else None
        got_before = got_before if got_before else None

        location = (task.lat, task.lon)
        _started = time.time()
        resp = self.maps.places_nearby(location=location,
                                       radius=task.radius,
                                       open_now=False,
                                       language=config.LANGUAGE,
                                       type=task.place_type,
                                       # rank_by='distance', # IMPORTANT: cannot use rank_by and radius options together
                                       page_token=page_token)

        # info timing
        _elapsed = time.time() - _started
        self.stats.avg_request_time = (_elapsed + self.stats.avg_request_time * self.stats.requests) / self.stats.requests + 1
        self.stats.requests += 1

        # parse here
        try:
            pois: List[PoiData] = PoiData.from_response(resp=resp)

        except ZeroResultsException:
            return []

        except WastedQuotaException:
            self.job_done = True
            raise WastedQuotaException(
                f'{self.name} reached maximum allowed quota. For details, check your Google Developers Account\n'
                f'{self.stats.previous_requests} requests made overall\n'
                f'API key used: {self.maps.key}'
                'Terminating thread...'
            )

        except InvalidRequestException:
            self.stats.critical_errors += 1
            self.print(
                    f"ERROR: invalid request exception in {self.name}. API key: {self.maps.key}\n"
                    f"Params: location = {location}, radius={task.radius}, type={task.place_type}, page_token={page_token}... "
            )
            return []

        except RequestDeniedException:
            self.stats.critical_errors += 1
            self.print(f"ERROR: request denied exception in {self.name}")
            return []

        next_page_token = resp.get("next_page_token")
        got_for_the_task = len(pois) + got_before

        if next_page_token is None:

            # no need to make more requests for this task
            if got_before >= 60:
                # produce tasks for recursion if needed
                self.submit_for_recursion(task=task)

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

        for t in self.densifier.densify(task=task):
            self.tasks_q.put(t)

    def _get_from_queue_and_do_job(self):

        try:
            #   will wait for N sec and throw Empty exception if nothing found
            task: TaskDefinition = self.tasks_q.get(timeout=5)  # is an instance of PoiSearchTask
        except Empty as e:
            time.sleep(1)   # wait a bit for new tasks
            raise e         # will repeat the main loop

        if not isinstance(task, TaskDefinition):
            self.print(f"{self.name}: received poison pill")
            self.job_done = True
            raise FinishException('can finish')

        else:
            results = self.search_task(task=task)
            for i in results:
                assert isinstance(i, PoiData), "must be a PoiData instance!"
                self.database_q.put(i)
                self.rawfile_q.put(i)
            self.complete_tasks_q.put(task)

    def run(self):

        self._prepare()

        self.print(f"{self.name} started")

        while not self.job_done:

            if self.stats.critical_errors > self.critical_errors_threshold:
                raise Exception(
                    f"Too many critical errors encountered ({self.stats.critical_errors}). Terminating..."
                )

            try:
                _started = time.time()
                self._get_from_queue_and_do_job()
                _elapsed = time.time() - _started
                self.stats.avg_task_time = (_elapsed + self.stats.tasks * self.stats.avg_task_time) / self.stats.tasks + 1
                self.stats.tasks += 1

            except Empty:
                continue

            except FinishException:
                self.job_done = True
                break

            if self.stats.requests % 10 == 0:
                self._update_tracker()

        # after the loop
        self._update_tracker()

