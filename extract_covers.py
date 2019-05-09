import sqlite3

import sys

with sqlite3.connect(sys.argv[1]) as conn:

    cursor = conn.cursor()
    cursor.execute("SELECT id from covers")

    cursor = conn.cursor()
    cursor.execute("SELECT id from covers")

    def rows():
        buf = list()
        for row in cursor.fetchall():
            buf.append(row[0])
            if len(buf) > 30:
                yield buf
                buf.clear()

    for batch in rows():
        cursor.execute("SELECT cover from covers where id in (%s)" % (",".join(("'" + b + "'") for b in batch)))
        covers = cursor.fetchall()
        for i, cover in enumerate(covers):
            with open("./tmpcovers/" + batch[i] + ".jpg", "wb") as out:
                out.write(cover[0])
                print(batch[i])
