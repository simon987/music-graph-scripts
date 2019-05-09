import json
from multiprocessing.pool import ThreadPool

from task_tracker_drone.src.tt_drone.api import TaskTrackerApi, Worker

TT_API_URL = "https://tt.simon987.net/api"
TT_PROJECT = 5


done = set()
# with sqlite3.connect(sys.argv[1]) as conn:
#     cur = conn.cursor()
#     cur.execute("SELECT id FROM covers")
#     for mbid in cur.fetchall():
#         done.add(mbid[0])

api = TaskTrackerApi(TT_API_URL)

worker = Worker.from_file(api)
if not worker:
    worker = api.make_worker("caa scraper")
    worker.dump_to_file()
worker.request_access(TT_PROJECT, True, True)
input("Give permission to " + worker.alias)


def mktask(mbids):
    res = worker.submit_task(
        project=TT_PROJECT,
        recipe=json.dumps(mbids),
        hash64=hash(mbids[0]),
        max_assign_time=60 * 30,
        priority=1,
        unique_str=None,
        verification_count=None,
        max_retries=5,
    )
    print(res.text)


def lines():
    with open("in/release") as f:
        buf = list()

        for line in f:
            cols = line.split("\t")

            buf.append(cols[1])
            if len(buf) == 75:
                a = list(buf)
                buf.clear()
                yield a


pool = ThreadPool(processes=20)
pool.map(func=mktask, iterable=lines())
