import json
import sqlite3
from multiprocessing.pool import ThreadPool

import sys

from task_tracker_drone.src.tt_drone.api import TaskTrackerApi, Worker

TT_API_URL = "https://tt.simon987.net/api"
TT_PROJECT = 7

api = TaskTrackerApi(TT_API_URL)

worker = Worker.from_file(api)
if not worker:
    worker = api.make_worker("mm worker")
    worker.dump_to_file()
worker.request_access(TT_PROJECT, True, True)
input("Give permission to " + worker.alias)

spotids = set()

with sqlite3.connect(sys.argv[1]) as conn:

    cur = conn.cursor()
    cur.execute("SELECT data from artist")
    for row in cur.fetchall():
        j = json.loads(row[0])
        if j is None or "artists" not in j or "items" not in j["artists"]:
            continue
        for item in j["artists"]["items"]:
            spotids.add(item["id"])


    def mktask(lines):
        res = worker.submit_task(
            project=TT_PROJECT,
            recipe=json.dumps(
                [{"spotid": line} for line in lines]
            ),
            unique_str=lines[0],
            max_assign_time=60 * 5,
        )
        print(res.text)

    def ids():
        id_batch = list()

        for spotid in spotids:
            id_batch.append(spotid)
            if len(id_batch) >= 30:
                res = list(id_batch)
                id_batch.clear()
                yield res

    tasks = list(ids())

    pool = ThreadPool(processes=25)
    pool.map(func=mktask, iterable=tasks)

