#!/usr/bin/env python
import json
import traceback
from itertools import repeat
from queue import Queue

import psycopg2
import spotipy
from hexlib.concurrency import queue_thread_exec, queue_iter
from hexlib.misc import silent_stdout
from spotipy.oauth2 import SpotifyClientCredentials

import config


def save_raw(conn, query, endpoint, data):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO mg.spotify_raw_data (query, endpoint, data) VALUES (%s,%s,%s) "
        "ON CONFLICT (query, endpoint) "
        "DO UPDATE SET ts=CURRENT_TIMESTAMP, data=excluded.data",
        (query, endpoint, json.dumps(data))
    )


def save_artist(conn, data, max_age_days=30):
    """Returns True if artist is new (and therefore, its albums, tracks etc. should be fetched)"""

    cur = conn.cursor()

    cur.execute("SELECT spotid FROM mg.spotify_artist_meta WHERE spotid=%s AND "
                "date_part('day', CURRENT_TIMESTAMP - ts) <= %s", (data["id"], max_age_days,))
    if cur.fetchone():
        return False

    cur.execute(
        "INSERT INTO mg.spotify_artist_meta (spotid, name, followers, popularity) "
        "VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (spotid) "
        "DO UPDATE SET name=excluded.name, followers=excluded.followers, popularity=excluded.popularity",
        (data["id"], data["name"], data["followers"]["total"], data["popularity"])
    )

    cur.execute("DELETE FROM mg.spotify_artist_tag WHERE spotid=%s", (data["id"],))
    if data["genres"]:
        cur.execute(
            "INSERT INTO mg.spotify_artist_tag VALUES %s" %
            ",".join("('%s', '%s')" % (n, t.replace("'", "''")) for (n, t) in zip(repeat(data["id"]), data["genres"]))
        )
    return True


def get_albums(conn, spotify, spotid):
    with silent_stdout:
        data = spotify.artist_albums(spotid, album_type="album,single,compilation")
    save_raw(conn, spotid, "artist_albums", data)

    cur = conn.cursor()
    cur.execute("DELETE FROM mg.spotify_artist_album WHERE spotid=%s", (spotid,))
    if data["items"]:
        cur.execute(
            "INSERT INTO mg.spotify_artist_album VALUES %s" %
            ",".join("('%s', '%s')" % (n, t.replace("'", "''"))
                     for (n, t) in zip(repeat(spotid), set(a["name"] for a in data["items"])))
        )
    return list()


def get_tracks(conn, spotify, spotid):
    with silent_stdout:
        data = spotify.artist_top_tracks(spotid)
    save_raw(conn, spotid, "artist_top_tracks", data)

    cur = conn.cursor()
    cur.execute("DELETE FROM mg.spotify_artist_track WHERE spotid=%s", (spotid,))

    unique_tracks = []
    done = set()
    for track in data["tracks"]:
        if track["name"] in done:
            continue
        unique_tracks.append((track["name"], track["preview_url"]))
        done.add(track["name"])

    if unique_tracks:
        cur.execute(
            "INSERT INTO mg.spotify_artist_track (spotid, track, url) VALUES %s" %
            ",".join("('%s', '%s', '%s')" % (i, t[0].replace("'", "''"), t[1])
                     for (i, t) in zip(repeat(spotid), unique_tracks))
        )


def related(conn, spotify, spotid):
    with silent_stdout:
        data = spotify.artist_related_artists(spotid)
    save_raw(conn, spotid, "artist_related_artists", data)
    return data["artists"]


def save_artist_artist(conn, id0, relations):
    if relations:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO mg.spotify_artist_artist (spotid0, spotid1, index) "
            "VALUES %s "
            "ON CONFLICT (spotid0, spotid1) "
            "DO NOTHING" %
            ",".join("('%s', '%s', '%d')" % (r[0], r[1]["id"], i) for (i, r) in enumerate(zip(repeat(id0), relations)))
        )


def get_mbids_with_matching_name(conn, name):
    cur = conn.cursor()
    cur.execute(
        "SELECT gid FROM artist "
        "WHERE asciifold_lower(name)=asciifold_lower(%s)",
        (name,)
    )
    rows = cur.fetchall()

    return [r[0] for r in rows]


def resolve_spotify_conflict(conn, mbid, existing_spotid, new_spotid):
    cur = conn.cursor()
    cur.execute(
        "SELECT asciifold_lower(album) FROM mg.spotify_artist_album WHERE spotid=%s",
        (new_spotid,)
    )
    new_albums = set(row[0] for row in cur.fetchall())

    if len(new_albums) == 0:
        return

    cur.execute(
        "SELECT asciifold_lower(album) FROM mg.spotify_artist_album WHERE spotid=%s",
        (existing_spotid,)
    )
    existing_albums = set(row[0] for row in cur.fetchall())

    if len(existing_albums) != 0:
        cur.execute(
            "SELECT DISTINCT asciifold_lower(release.name) FROM release "
            "INNER JOIN artist_credit_name cn ON cn.artist_credit = release.artist_credit "
            "INNER JOIN artist a on a.id = cn.artist "
            "WHERE a.gid=%s", (mbid,)
        )
        mb_albums = set(row[0] for row in cur.fetchall())
        if len(new_albums.intersection(mb_albums)) > len(existing_albums.intersection(mb_albums)):
            cur.execute("UPDATE mg.spotify_artist SET spotid = %s WHERE mbid=%s", (new_spotid, mbid))


def resolve_mb_conflict(conn, spotid, mbids):
    cur = conn.cursor()

    cur.execute(
        "SELECT asciifold_lower(album) FROM mg.spotify_artist_album WHERE spotid=%s",
        (spotid,)
    )
    spot_albums = set(row[0] for row in cur.fetchall())

    best_match_count = -1
    best_match = None

    if len(spot_albums) == 0:
        # We can't base our conflict resolution based on album names,
        # pick the one with the most releases
        for mbid in mbids:
            cur.execute(
                "SELECT count(release.name) FROM release "
                "INNER JOIN artist_credit_name cn ON cn.artist_credit = release.artist_credit "
                "INNER JOIN artist a on a.id = cn.artist "
                "WHERE a.gid = %s ",
                (mbid,)
            )
            match_count = cur.fetchone()[0]
            if match_count > best_match_count:
                best_match_count = match_count
                best_match = mbid
    else:
        for mbid in mbids:
            cur.execute(
                "SELECT asciifold_lower(release.name) FROM release "
                "INNER JOIN artist_credit_name cn ON cn.artist_credit = release.artist_credit "
                "INNER JOIN artist a on a.id = cn.artist "
                "WHERE a.gid = %s ",
                (mbid,)
            )
            match_count = len(set(row[0] for row in cur.fetchall()).intersection(spot_albums))
            if match_count > best_match_count:
                best_match_count = match_count
                best_match = mbid

    save_spotid_to_mbid(conn, spotid, best_match)


def save_spotid_to_mbid(conn, spotid, mbid):
    cur = conn.cursor()
    cur.execute(
        "SELECT spotid FROM mg.spotify_artist WHERE mbid=%s",
        (mbid,)
    )
    row = cur.fetchone()
    if row:
        resolve_spotify_conflict(conn, mbid, row[0], spotid)
    else:
        cur.execute(
            "INSERT INTO mg.spotify_artist (spotid, mbid) VALUES (%s,%s)",
            (spotid, mbid)
        )


def search_artist(conn, spotify, name):
    quoted_name = "\"%s\"" % name

    with silent_stdout:
        data = spotify.search(quoted_name, type="artist", limit=20)
    save_raw(conn, name, "search", data)

    for result in data["artists"]["items"]:
        if save_artist(conn, result):
            mbids = get_mbids_with_matching_name(conn, result["name"])

            get_albums(conn, spotify, result["id"])
            get_tracks(conn, spotify, result["id"])

            if len(mbids) > 1:
                resolve_mb_conflict(conn, result["id"], mbids)
            elif len(mbids) == 1:
                save_spotid_to_mbid(conn, result["id"], mbids[0])

            save_artist_artist(conn, result["id"], related(conn, spotify, result["id"]))


def get_tasks(conn, count=1):
    cur = conn.cursor()
    cur.execute(
        "SELECT artist.name FROM artist "
        "LEFT JOIN mg.spotify_artist sa ON sa.mbid=gid "
        "LEFT JOIN mg.spotify_raw_data srd ON srd.query=artist.name AND endpoint='search' "
        "LEFT JOIN mg.spotify_artist_meta sam ON sa.spotid=sam.spotid "
        "ORDER BY sam.ts NULLS FIRST, srd.ts NULLS FIRST LIMIT %s",
        (count,)
    )
    for row in cur:
        yield row[0]


def worker(q):
    conn = psycopg2.connect(config.connstr())
    spotify = spotipy.Spotify(client_credentials_manager=client_credentials_manager)

    for task in queue_iter(q, block=False):
        try:
            search_artist(conn, spotify, task)
            conn.commit()
            # print(task)
        except Exception as e:
            print("Error %s : %s" % (e, traceback.format_exc()))
    conn.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--threads", type=int, default=10)
    args = parser.parse_args()

    client_credentials_manager = SpotifyClientCredentials(
        client_id=config.config["SPOTIFY_CLIENTID"],
        client_secret=config.config["SPOTIFY_SECRET"]
    )
    queue = Queue()

    conn = psycopg2.connect(config.connstr())
    for task in get_tasks(conn, args.count):
        queue.put(task)
    conn.close()

    queue_thread_exec(queue, func=worker, thread_count=args.threads)
