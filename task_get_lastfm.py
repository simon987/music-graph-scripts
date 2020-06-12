#!/usr/bin/env python3

import json
from itertools import repeat

import config

import psycopg2
import requests


def get_mbid(lfm_name):
    cur = conn.cursor()
    cur.execute("SELECT mbid "
                "FROM mg.lastfm_artist WHERE name=%s", (lfm_name,))
    row = cur.fetchone()
    return row[0] if row else None


def set_mbid(lfm_name, mbid):
    cur = conn.cursor()
    cur.execute("INSERT INTO mg.lastfm_artist VALUES (%s,%s) ON CONFLICT (name) "
                "DO UPDATE SET mbid=excluded.mbid", (lfm_name, mbid))


def save_tags(lfm_name, tags):
    if not tags:
        return
    cur = conn.cursor()

    cur.execute("DELETE FROM mg.lastfm_artist_tag WHERE name=%s", (lfm_name,))
    cur.execute(
        "INSERT INTO mg.lastfm_artist_tag VALUES %s" %
        ",".join("('%s', '%s')" % (n, t.strip()) for (n, t) in zip(repeat(lfm_name), tags))
    )


def save_data(data):
    if data:
        disambiguate(data["name"], mbid=data["artist"])

        for similar in [s for s in data["similar"] if s["mbid"] is not None]:
            disambiguate(similar["name"], similar["mbid"])
            save_similar(data["name"], similar["name"], similar["match"])

        save_tags(data["name"], data["tags"])
        save_meta(data["name"], data["listeners"], data["playcount"])


def save_similar(lfm_name, similar, weight):
    cur = conn.cursor()

    cur.execute(
        "INSERT INTO mg.lastfm_artist_artist (name0, name1, weight) VALUES (%s,%s,%s) "
        "ON CONFLICT (name0, name1) DO UPDATE SET weight=excluded.weight, ts=CURRENT_TIMESTAMP",
        (lfm_name, similar, weight)
    )


def save_meta(lfm_name, listeners, playcount):
    cur = conn.cursor()
    cur.execute("INSERT INTO mg.lastfm_artist_meta VALUES (%s,%s,%s) ON CONFLICT (name) "
                "DO UPDATE SET listeners=excluded.listeners, playcount=excluded.playcount",
                (lfm_name, listeners, playcount))


def save_raw_data(name, mbid, data):
    cur = conn.cursor()
    cur.execute("INSERT INTO mg.lastfm_raw_data (name, mbid, data) VALUES (%s,%s,%s) "
                "ON CONFLICT (name, mbid) DO UPDATE SET ts=CURRENT_TIMESTAMP, data=excluded.data",
                (name, mbid, json.dumps(data)))


def get_release_count(mbid):
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) '
                'FROM l_artist_release '
                'INNER JOIN artist a ON entity0 = a.id '
                'WHERE a.gid = %s', (mbid,))
    row = cur.fetchone()
    return row[0] if row else 0


def disambiguate(name, mbid):
    """
    A lastfm artist name can refer to multiple MBIDs
    For RELATED_TO purposes, we assume that the MBID referring
    to the artist with the most official releases is the one
    """
    existing_mbid = get_mbid(name)

    if existing_mbid and mbid != existing_mbid:
        if get_release_count(existing_mbid) < get_release_count(mbid):
            set_mbid(name, mbid)
    else:
        set_mbid(name, mbid)


def get_cached_artist_data(name, mbid, max_age_days):
    cur = conn.cursor()
    cur.execute("SELECT data FROM mg.lastfm_raw_data WHERE name=%s AND mbid=%s "
                "AND date_part('day', CURRENT_TIMESTAMP - ts) <= %s ",
                (name, mbid, max_age_days))

    row = cur.fetchone()
    return row[0] if row else 0


def get_artist_data(name: str, mbid: str):
    cached_data = get_cached_artist_data(name, mbid, max_age_days=30)
    if cached_data:
        return cached_data

    raw = []
    url = "https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&mbid=%s&api_key=%s&format=json" % \
          (mbid, config.config["LASTFM_APIKEY"],)
    r = requests.get(url)
    raw.append((url, r.text))
    info_json = r.json()

    by_name = False

    if "artist" not in info_json:
        url1 = "https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=%s&api_key=%s&format=json" % \
               (name, config.config["LASTFM_APIKEY"],)
        r = requests.get(url1)
        raw.append((url1, r.text))
        info_json = r.json()
        if "artist" not in info_json:
            if "Rate Limit Exceeded" in r.text:
                raise Exception("Rate Limit Exceeded!")
            data = {
                "_raw": raw
            }
            save_raw_data(name, mbid, data)
            return
        by_name = True

    if by_name:
        url2 = "https://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&artist=%s&api_key=%s&format=json" % (
            name, config.config["LASTFM_APIKEY"],)
    else:
        url2 = "https://ws.audioscrobbler.com/2.0/?method=artist.getsimilar&mbid=%s&api_key=%s&format=json" % (
            mbid, config.config["LASTFM_APIKEY"],)
    r2 = requests.get(url2)
    raw.append((url2, r2.text))
    similar_json = r2.json()

    data = {
        "artist": mbid,
        "name": info_json["artist"]["name"],
        "mbid": info_json["artist"]["mbid"] if "mbid" in info_json["artist"] else None,
        "tags": [t["name"] for t in info_json["artist"]["tags"]["tag"]] if "tags" in info_json["artist"] and "tag" in
                                                                           info_json["artist"]["tags"] else [],
        "listeners": info_json["artist"]["stats"]["listeners"],
        "playcount": info_json["artist"]["stats"]["playcount"],
        "similar": [
            {
                "mbid": a["mbid"] if "mbid" in a else None,
                "match": a["match"],
                "name": a["name"]
            }
            for a in similar_json["similarartists"]["artist"]],
        "_raw": raw
    }

    save_raw_data(name, mbid, data)

    return data


def get_task(count=1):
    cur = conn.cursor()
    cur.execute(
        "SELECT artist.name, artist.gid FROM artist "
        "LEFT JOIN mg.lastfm_raw_data lfm ON lfm.mbid=gid AND lfm.name=artist.name "
        "ORDER BY lfm.ts NULLS FIRST LIMIT %s",
        (count,)
    )
    return cur.fetchone()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()

    conn = psycopg2.connect(config.connstr())

    for task in get_task(args.count):
        save_data(get_artist_data(*task))
        conn.commit()
        print(task[0])

    conn.close()
