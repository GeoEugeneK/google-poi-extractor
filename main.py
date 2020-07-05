import multiprocessing as mp
import sqlite3
import sys
import time
from typing import List

import config
import resume
import timing
from api import get_api_keys
from db.connect import make_db_connection
from db.writer import DatabaseWriter
from db import expressions
from geometries.geomworks import make_grid, get_aoi_polygon
from json_writer import RawResponseWriter
from placetypes import get_search_types, get_valid_types
from tasks import TaskDefinition
from workers import GoogleWorker



def prepare_database(cursor: sqlite3.Cursor):

    """ Drop and make new tables """

    cursor.execute(expressions.DROP_SUCCESS)
    cursor.execute(expressions.DROP_JOBS)

    # create new
    cursor.execute(expressions.CREATE_POI_TABLE)
    cursor.execute(expressions.CREATE_SUCCESS_TABLE)
    cursor.execute(expressions.CREATE_JOBS_TABLE)


def make_initial_tasks() -> List[TaskDefinition]:

    """ Gets AOI polygon from specified layer, makes initial grid and creates initial tasks."""

    assert config.AOI_LAYER_URI and isinstance(config.AOI_LAYER_URI, str), \
        f'invalid layer URI {config.AOI_LAYER_URI}, see config to fix'

    # derive spacing
    spacing = config.INITIAL_RADIUS * 2 / (2 ** 0.5)

    aoi_polygon = get_aoi_polygon(config.AOI_LAYER_URI)
    initial_points = make_grid(aoi_polygon, spacing=spacing, metric_epsg=config.METRIC_CRS_EPSG)

    # figure out the types
    all_types = get_valid_types()
    search_types = get_search_types()

    if len(search_types) == 0:
        raise Exception(f"no search types found in {config.SEARCH_TYPES_FILE}")
    else:
        validated_search_types = []
        for i in search_types:
            if i in all_types:
                validated_search_types.append(i)
            else:
                print(f"WARN: place type \"{i}\" is invalid and will not be used. "
                      f"Please check valid types in {config.VALID_TYPES_FILE}")

        if len(validated_search_types) == 0:
            raise Exception("no valid types were selected for search!")
        else:
            print(f"INFO: {len(validated_search_types)} out of {len(search_types)} types will be used for search")
            del search_types

    initial_tasks = []

    for t in validated_search_types:
        type_tasks = [
            TaskDefinition(lon=pt.x(), lat=pt.y(), radius=config.INITIAL_RADIUS, place_type=t) for pt in initial_points
        ]
        initial_tasks.extend(type_tasks)

    print(f"INFO: total {len(initial_tasks)} initial search tasks were prepared "
          f"for {len(validated_search_types)} place types and search radius = {config.INITIAL_RADIUS:.1f} m")

    return initial_tasks


def restore_tasks_and_places(cursor: sqlite3.Cursor):

    tables = resume.get_existing_tables(cursor=cursor)

    if len(tables == 0):
        raise Exception(f"cannot restore tasks as no tables were found in the database!")

    elif config.JOBS_TABLE not in tables:
        raise Exception(f"cannot restore tasks as table \"{config.JOBS_TABLE}\" is missing")

    elif config.SUCCESS_TABLE not in tables:
        raise Exception(f"cannot restore tasks as table \"{config.SUCCESS_TABLE}\" is missing")

    elif config.POI_TABLE not in tables:
        raise Exception(f"cannot restore tasks as table \"{config.POI_TABLE}\" is missing")

    else:
        unfinished_tasks = resume.restore_unfinished_tasks(cursor=cursor)
        collected_place_ids = resume.restore_collected_place_ids()

    return unfinished_tasks, collected_place_ids


def main():

    started_prepare = time.time()

    # connection to prepare stuff, either for a clean start or restore prev session data
    # must be closed before writer thread starts
    conn = make_db_connection(config.DATABASE)      # creates the database if not exists
    cursor = conn.cursor()

    # queues
    tasks_q = mp.Queue()
    tasks_for_record_q = mp.Queue()
    database_q = mp.Queue()
    complete_q = mp.Queue()
    raw_json_q = mp.Queue()

    # locks
    writelock = mp.Lock()
    printlock = mp.Lock()

    # make writers, but don't launch yet
    db_writer = DatabaseWriter(db_file=config.DATABASE, poi_q=database_q, tasks_q=tasks_for_record_q,
                               complete_tasks_q=complete_q, printlock=printlock)
    raw_writer = RawResponseWriter(poi_q=raw_json_q, printlock=printlock)

    # define typing
    tasks: List[TaskDefinition]
    collected_place_ids: List[str]

    if config.RESUME:
        tasks, collected_place_ids = restore_tasks_and_places(cursor=cursor)    # unfinished tasks only
        db_writer.set_place_ids(place_ids=collected_place_ids)      # avoid writing duplicates, will check against these
        db_writer.set_success_ids(["dummy"])

    else:
        tables = resume.get_existing_tables(cursor=cursor)
        if config.POI_TABLE in tables:
            raise Exception(f"ERROR: table {config.POI_TABLE} already exists. "
                            f"You must remove it manually or use a different table name")
        tasks = make_initial_tasks()                        # initial_tasks
        prepare_database(cursor=cursor)

    # fill queue
    for t in tasks:
        tasks_q.put(t)

    # also include in all jobs table
    db_writer.set_initial_jobs(jobs=tasks)

    conn.close()    # close connection in this thread

    # get keys
    keys = get_api_keys()
    assert keys, "no keys can be used!"

    # print info
    _elapsed = time.time() - started_prepare
    print(f"MAIN: ready in {_elapsed:.1f} s. Starting {len(keys)} data collector workers, one API key per thread...")

    # start worker threads
    collectors = []
    for n, k in enumerate(keys):
        t = GoogleWorker(api_key=k, tasks_q=tasks_q, tasks_for_record_q=tasks_for_record_q, database_q=database_q,
                         complete_tasks_q=complete_q, rawfile_q=raw_json_q, printlock=printlock, writelock=writelock)
        t.start()
        time.sleep(1)   # wait between starts
        collectors.append(t)

    with printlock:
        print(f"MAIN: workers started. Starting writers...")

    # start writers
    db_writer.start()
    raw_writer.start()

    with printlock:
        print(f"MAIN: writer threads started")

    for t in collectors:
        t.join()

    with printlock:
        print(f"MAIN: collector threads joined")

    db_writer.join()
    raw_writer.join()

    with printlock:
        print(f"MAIN: writer threads joined. All jobs complete")



if __name__ == '__main__':

    print("MAIN: getting to work")
    started = time.time()

    main()  # does all the job

    elapsed = time.time() - started
    print(f"MAIN: done in {timing.format_delta(sec=elapsed, include_ms=False)}")
    sys.exit(0)
