import threading
import time
from queue import Queue
from typing import List

from db.writer import DatabaseWriter
from geometries.geomworks import make_grid, get_aoi_polygon, Densifier
from tasks import TaskDefinition
import config
from placetypes import get_search_types, get_valid_types
import resume
from api import get_api_keys
from workers import GoogleWorker


def make_initial_tasks():

    """ Gets AOI polygon from specified layer, makes initial grid and creates initial tasks."""

    assert config.AOI_LAYER_URI and isinstance(config.AOI_LAYER_URI), \
        f'invalid layer URI {config.AOI_LAYER_URI}, see config to fix'

    # derive spacing
    spacing = config.INITIAL_RADIUS * 2 / (2 ** 0.5)

    aoi_polygon = get_aoi_polygon(config.AOI_LAYER_URI)
    initial_points = make_grid(aoi_polygon, spacing=spacing)

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


def main():

    lock = threading.Lock()
    writer = DatabaseWriter(db_file=config.DATABASE, lock=lock)

    if config.RESUME:
        tasks: List[TaskDefinition] = resume.restore_unfinished(writer.cursor)    # restored tasks
    else:
        tasks: List[TaskDefinition] = make_initial_tasks()                        # initial_tasks

    # fill queue
    tasks_q = Queue()
    for t in tasks:
        tasks_q.put(t)

    # get keys
    keys = get_api_keys()
    assert keys, "no keys can be used!"

    # initialize worker threads
    collectors = []
    for n, k in enumerate(keys):
        t = GoogleWorker(api_key=k, writelock=lock, threadname=f"Google Places Collector - {n + 1}")
        t.start()
        time.sleep(1)   # wait between starts
        collectors.append(t)





if __name__ == '__main__':

    PROCESSED_TASKS