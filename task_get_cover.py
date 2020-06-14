from io import BytesIO
from multiprocessing.pool import ThreadPool

import psycopg2
import requests
from PIL import Image

import config


def should_download(image: dict):
    return image["front"] is True


def thumb(cover_blob):
    with Image.open(BytesIO(cover_blob)) as image:

        # https://stackoverflow.com/questions/43978819
        if image.mode == "I;16":
            image.mode = "I"
            image.point(lambda i: i * (1. / 256)).convert('L')

        image.thumbnail((256, 256), Image.BICUBIC)
        canvas = Image.new("RGB", image.size, 0x000000)

        if image.mode in ('RGBA', 'LA') or (image.mode == 'P' and 'transparency' in image.info):
            try:
                canvas.paste(image, mask=image.split()[-1])
            except ValueError:
                canvas.paste(image)
        else:
            canvas.paste(image)

        blob = BytesIO()
        canvas.save(blob, "JPEG", quality=85, optimize=True)
        canvas.close()

    return blob.getvalue()


def save(mbid, tn):
    with psycopg2.connect(config.connstr()) as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO mg.covers (mbid, tn) VALUES (%s,%s) ON CONFLICT (mbid) "
            "DO UPDATE SET tn = excluded.tn",
            (mbid, tn)
        )
        conn.commit()


def get_mbids(count=1):
    with psycopg2.connect(config.connstr()) as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT gid FROM release_group "
            "LEFT JOIN mg.covers ON gid = mbid "
            "WHERE tn IS NULL "
            "ORDER BY ts NULLS FIRST LIMIT %s",
            (count,)
        )
        for row in cur:
            yield row[0]


def download(mbid):
    r = requests.get("https://coverartarchive.org/release-group/%s/front-250.jpg" % mbid)

    if r.status_code == 200:
        return r.content
    if r.status_code != 404:
        print("<%d> %s" % (r.status_code, r.text))
    return None


def work(mbid):
    tn = download(mbid)
    save(mbid, tn)
    print(mbid)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--threads", type=int, default=3)
    args = parser.parse_args()

    pool = ThreadPool(processes=args.threads)
    pool.map(func=work, iterable=get_mbids(args.count))
