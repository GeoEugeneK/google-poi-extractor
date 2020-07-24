import random
# from sqlite3 import Cursor
import time
from db.connect import make_db_connection


def insert_sample(into: str, row: list):

    global inserted_count, commits_count

    assert into, "no table to insert into!"
    assert len(row) >= 1, "no elements to insert"

    question_marks = ", ".join("?" for _ in row)
    statement = f"INSERT INTO {into} VALUES({question_marks});"
    cursor.execute(statement, row)

    inserted_count += 1

    global __uncommitted_inserts
    try:
        if __uncommitted_inserts:
            __uncommitted_inserts += 1
        else:
            __uncommitted_inserts = 1
    except NameError:
        __uncommitted_inserts = 1       # first run of the function

    # commit if needed
    if __uncommitted_inserts > commit_each:
        print(f"DEBUG: committed {commits_count} times")
        commits_count += 1
        __uncommitted_inserts = 0
        conn.commit()



if __name__ == '__main__':

    n_inserts = 23456
    table_name = "sampletable"
    commit_each = 100

    # global, will be updated within func
    inserted_count = 0
    commits_count = 0

    # creates new connection
    conn = make_db_connection("./__sample_db.sqlite3")
    cursor = conn.cursor()

    # make sample table
    drop_statement = f"DROP TABLE IF EXISTS {table_name};"
    create_statement = f"CREATE TABLE {table_name} (first_c TEXT, second_c INT, third_c TEXT, fourth_c FLOAT);"
    cursor.execute(drop_statement)
    cursor.execute(create_statement)
    conn.commit()

    started = time.time()   # measure timing

    # main loop
    for _ in range(n_inserts):
        row = ["short string", random.randint(-9999, 9999), "big string" * 152, random.uniform(-9999, 9999)]
        insert_sample(into=table_name, row=row)
    conn.commit()

    elapsed = time.time() - started
    per_row_ms = elapsed * 1000 / n_inserts
    per_commit_ms = elapsed * 1000 / commits_count

    print(f"Total {n_inserts} rows inserted in {elapsed:.1f} s, avg per row = {per_row_ms:.1f} ms. "
          f"Total {commits_count} commits, avg per commit = {per_commit_ms:.1f} ms")

    # show random row from the table
    statement_random_row = f"SELECT * FROM {table_name} ORDER BY RANDOM() LIMIT 1;"
    _ = cursor.fetchall()
    cursor.execute(statement_random_row)
    row = cursor.fetchall()[0]
    print(f"Selected random row: {row}")

    conn.close()
    print('DONE')
