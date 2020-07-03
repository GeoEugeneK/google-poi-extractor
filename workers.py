import json
import os
from time import sleep

import threading
import googlemaps

# from config import TRACKER_JSON, LANGUAGE
import config


class GoogleWorker(threading.Thread):
    """ One key per collector worker. """

    def __init__(self,
                 api_key: str,
                 lock: threading.Lock,
                 threadname: str,
                 critical_error_threshold: int = 50,
                 ):
        super().__init__(daemon=True, name=threadname)

        self.lock = lock
        self.maps = googlemaps.Client(key=api_key,
                                      queries_per_second=3,
                                      retry_over_query_limit=False,
                                      timeout=5)

        self.job_done = False

        self.tracker = 0
        self.critical_errors = 0
        self.critical_error_threshold = critical_error_threshold
        self.minor_errors = 0  # do we really need it?

        self._prepare()

    def _prepare(self):

        self._check_api_key()
        self._load_tracker()
        self._update_tracker()

        with self.lock:
            print(f'{self.name} started. {self.tracker} requests made in previous runs | Using API key {self.maps.key}')

    def _load_tracker(self):

        """ Loads requests count from previous sessions. """

        with self.lock:
            if os.path.isfile(config.TRACKER_JSON):
                with open(config.TRACKER_JSON, encoding='utf-8-sig') as f:
                    d = json.loads(f.read())
                try:
                    self.tracker = d[self.maps.key]
                except KeyError:
                    self.tracker = 0
            else:
                self.tracker = 0

    def _update_tracker(self):
        with self.lock, open(config.TRACKER_JSON, 'r+', encoding='utf-8-sig') as f:
            trackerdict = json.loads(f.read())
            f.seek(0)  # return pointer to the beginning of file before writing'

            trackerdict[self.maps.key] = self.tracker
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

            with self.lock:
                print(f'ERROR: invalid API key "{self.maps.key}" (tracker={self.tracker})')
            raise e

    def _single_point_search(self, latlon: [tuple, list],
                             radius: int,
                             place_type: str = None,
                             page_token=None,
                             got_before=0,
                             working_result: dict = None):

        resp = self.maps.places_nearby(location=latlon,
                                       radius=radius,
                                       open_now=False,
                                       language=config.LANGUAGE,
                                       type=place_type,
                                       # rank_by='distance',         # IMPORTANT: cannot use rank_by and radius options together
                                       page_token=page_token)
        self.tracker += 1

        if working_result is None:
            working_result = {'xy': xy,
                              'pois_tsv': [],
                              'needs_recursion': False,
                              'type': place_type,
                              'radius': radius,
                              }

        # VALIDATION
        # check is everything ok
        requests_status = resp['status'].upper()
        if requests_status in ['OK', 'ZERO_RESULTS']:
            pass

        elif requests_status == 'OVER_QUERY_LIMIT':
            self._update_tracker()
            with self.lock:
                raise Exception(
                    f'Process {self.name} reached maximum allowed quota. For details, check your Google Developers Account\n'
                    f'{self.tracker} requests made overall\n'
                    f'API key used: {self.maps.key}'
                    'Terminating...'
                )

        elif requests_status in ['INVALID_REQUEST', 'REQUEST_DENIED']:
            self.critical_errors += 1
            self._update_tracker()
            with self.lock:
                raise Exception(
                    f'Bad request: {requests_status}\n'
                    f'API key: {self.maps.key}\n'
                    f'Params: xy={xy}, radius={radius}, type={place_type}, page_token={page_token}... '
                )

        # elif requests_status == 'UNKNOWN_ERROR':
        else:  # basically means the above line

            # don't bother much and put it back on queue
            self.tasks_queue.put()  # TODO - put what exactly?

        # parsing response
        for poi in resp['results']:
            geojsn_feat = extract_poi_data(poi=poi)
            working_result['pois_tsv'].append(geojsn_feat)

        try:
            next_page_token = resp['next_page_token']
        except KeyError:
            next_page_token = None

        got_before = resp['results'].__len__() + got_before
        if next_page_token is None:
            if got_before >= 60:
                working_result['needs_recursion'] = True
            return working_result
        else:
            # at some point it reaches a state where next_page_token is None and returns all the results
            return self._single_point_search(xy=xy,
                                             radius=radius,
                                             place_type=place_type,
                                             page_token=page_token,
                                             got_before=got_before,
                                             working_result=working_result)

    def _get_from_queue_and_do_job(self):
        with self.lock:
            if not self.tasks_queue.empty():
                search_task = self.tasks_queue.get()  # is an instance of PoiSearchTask
                if search_task.made_loops < SINGLE_SEARCH_RETRIES_LIMIT:
                    xy, place_type = search_task.xy, search_task.place_type
                else:
                    xy = None
            else:
                xy = None

        if xy is None:
            sleep(0.25)  # give it some rest

        # elif xy == (r'TERMINATE', r'TERMINATE'):
        #     self.job_done = True

        else:
            result = self._single_point_search(xy=xy, place_type=place_type)
            self.results_queue.put(result)

    def run(self):
        while not self.job_done:
            if self.critical_errors > self.critical_error_threshold:
                raise Exception(
                    f"Too many critical errors encountered ({self.critical_errors}). Terminating..."
                )
            self._get_from_queue_and_do_job()
            if self.tracker % 10 == 0:
                self._update_tracker()
        else:
            self._update_tracker()
            self.terminate()
