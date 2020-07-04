import sqlite3
from typing import List

from db.expressions import GET_UNFINISHED_FROM_PREVIOUS_SESSION, GET_POI_IDS_FROM_PREVIOUS_SESSIONS
from tasks import TaskDefinition


def get_existing_tables(cursor: sqlite3.Cursor):

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [rowtuple[0] for rowtuple in cursor.fetchall()]

    if len(tables) == 0:
        print(f"WARN: no tables found in the database!")

    return tables if tables else []


def restore_unfinished_tasks(cursor: sqlite3.Cursor) -> List[TaskDefinition]:

    cursor.execute(GET_UNFINISHED_FROM_PREVIOUS_SESSION)
    rows = cursor.fetchall()

    if len(rows) == 0:
        print(f"ERROR: no unfinished tasks fetched!")
        return []

    tasks = []
    for task_id, lon, lat, radius, place_type in rows:
        t = TaskDefinition(
            task_id=task_id,
            lon=lon,
            lat=lat,
            radius=radius,
            place_type=place_type
        )
        tasks.append(t)

    print(f"INFO: {len(tasks)} unfinished tasks restored from previous sessions")

    return tasks


def restore_collected_place_ids(cursor: sqlite3.Cursor) -> List[str]:

    cursor.execute(GET_POI_IDS_FROM_PREVIOUS_SESSIONS)
    rows = cursor.fetchall()

    if len(rows) == 0:
        print(f"ERROR: no places fetched from previous sessions!")
        return []

    place_ids: List[str] = [r[0] for r in rows]
    place_ids = list(set(place_ids))

    print(f"INFO: {len(place_ids)} places restored from previous sessions")

    return place_ids
