import sqlite3

release_to_release_group_map = dict()
release_groups = dict()

with open("in/release_group") as f:
    for line in f:
        cols = line.split("\t")
        release_groups[cols[0]] = cols[1]

with open("in/release") as f:
    for line in f:
        cols = line.split("\t")
        release_to_release_group_map[cols[1]] = release_groups[cols[4]]

with sqlite3.connect("mapdb.db") as conn:

    cursor = conn.cursor()
    cursor.execute("CREATE TABLE map (release TEXT PRIMARY KEY , release_group TEXT)")

    for k, v in release_to_release_group_map.items():
        cursor.execute("INSERT INTO map (release, release_group) VALUES (?,?)", (k, v))
    conn.commit()

"""
CREATE TABLE covers (id TEXT primary key, cover BLOB);
ATTACH 'mapdb.db' AS map;
ATTACH '/mnt/Data8/caa_tn_only.db' AS source;
INSERT OR IGNORE INTO covers SELECT release_group, cover FROM source.covers INNER JOIN map.map ON id = map.release;
"""

