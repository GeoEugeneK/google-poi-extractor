from typing import List

import config
import sqlite3

from db.expressions import GET_UNFINISHED_FROM_PREVIOUS_SESSION
from tasks import TaskDefinition


def get_existing_tables(tablename: str, cursor: sqlite3.Cursor):

    assert tablename, f'invalid table name "{tablename}"'

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [rowtuple[0] for rowtuple in cursor.fetchall()]

    if len(tables) == 0:
        print(f"WARN: no tables found in the database!")

    return tables if tables else []


def restore_unfinished(cursor: sqlite3.Cursor) -> List[TaskDefinition]:
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


def __restore_prev_session_success(cursor: sqlite3.Cursor):
    cursor.execute('xx')
    rows = cursor.fetchall()
