import csv
import json
import sqlite3
from collections import defaultdict
import sys

artists = set()


def disambiguate(lfm_artist, artist_release_count, name, mbid):
    existing_mbid = lfm_artist.get(name, None)

    if existing_mbid and mbid != existing_mbid:
        if artist_release_count[existing_mbid] < artist_release_count[mbid]:

            lfm_artist[name] = mbid

            print("Replacing %s (%s) with %s (%d) for %s" %
                  (existing_mbid, artist_release_count[existing_mbid],
                   mbid, artist_release_count[mbid],
                   name))
    else:
        lfm_artist[name] = mbid


def patch(lastfm_data):

    artist_listeners = dict()
    lastfm_artist_to_mbid = dict()
    artist_release_count = defaultdict(int)
    related = list()

    with open("repo/artist_release.csv") as f:
        for line in f:
            cols = line.split(',')
            artist_release_count[cols[0]] += 1

    with sqlite3.connect(lastfm_data) as conn:
        cur = conn.cursor()
        cur.execute("SELECT data FROM lastfmdata", )
        data = list(cur.fetchall())

    # A lastfm artist name can refer to multiple MBIDs
    # For RELATED_TO purposes, we assume that the MBID referring
    # to the artist with the most official releases is the one

    for row in data:
        meta = json.loads(row[0])

        disambiguate(lastfm_artist_to_mbid, artist_release_count, meta["name"], meta["artist"])

        for similar in [s for s in meta["similar"] if s["mbid"] is not None]:
            disambiguate(lastfm_artist_to_mbid, artist_release_count, similar["name"], similar["mbid"])

    # Get related links & listener counts
    for row in data:
        meta = json.loads(row[0])

        artist_listeners[lastfm_artist_to_mbid[meta["name"]]] = \
            (meta["listeners"], meta["playcount"])

        for similar in [s for s in meta["similar"] if s["mbid"] is not None]:
            related.append((
                lastfm_artist_to_mbid[similar["name"]],
                lastfm_artist_to_mbid[meta["name"]],
                similar["match"]
            ))

    with open("repo/lastfm_artist.csv", "w") as out:
        writer = csv.writer(out)
        writer.writerow([
            "id:ID(Artist)", "name", "year:short", ":LABEL", "listeners:int", "playcount:int"
        ])

        with open("repo/artist.csv") as f:
            reader = csv.reader(f)

            reader.__next__()  # Skip header
            for row in reader:
                writer.writerow([
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    artist_listeners.get(row[0], (0, 0))[0],
                    artist_listeners.get(row[0], (0, 0))[1],
                ])
                artists.add(row[0])

    with open("repo/lastfm_artist_artist.csv", "w") as out:
        out.write(",".join((
            ":START_ID(Artist)", ":END_ID(Artist)", "weight:float"
        )) + "\n")

        for x in related:
            if x[0] in artists and x[1] in artists:
                out.write(",".join(x) + "\n")


patch(sys.argv[1])
