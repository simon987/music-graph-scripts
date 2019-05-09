import os
from collections import defaultdict

links = dict()
link_types = dict()
areas = dict()
labels = dict()
label_types = {
    "\\N": ""
}
release_groups = dict()
release_statuses = dict()
release_to_release_group_map = dict()
release_types = {
    "\\N": "",
}
artists = dict()
tags = dict()

release_release_rel_map = {
    "covers and versions": "",
    "remixes and compilations": "",
    "DJ-mix": "IS_DJ_MIX_OF",
    "live performance": "IS_LIVE_PERFORMANCE_OF",
    "cover": "IS_COVER_OF",
    "remix": "IS_REMIX_OF",
    "mashes up": "IS_MASHUP_OF",
    "included in": "INCLUDED_IN",
    "single from": "IS_SINGLE_FROM"
}

artist_release_rel_map = {
    "translator": "TRANSLATED",
    "liner notes": "WROTE_LINER_NOTES",
    "lyricist": "IS_LYRICIST_FOR",
    "lacquer cut": "DID_LACQUER_CUT_FOR",
    "samples from artist": "HAS_SAMPLES_IN",
    "remixes and compilations": "",
    "composition": "COMPOSED",
    "booking": "DID_BOOKING_FOR",
    "balance": "DID_BALANCE_FOR",
    "misc": "HAS_MISC_ROLE_IN",
    "conductor": "CONDUCTED",
    "legal representation": "PROVIDED_LEGAL_REPRESENTATION_FOR",
    "design/illustration": "DID_DESIGN_FOR",
    "performing orchestra": "PERFORMED_FOR",
    "producer": "PRODUCED",
    "instrument": "PERFORMED_INSTRUMENT_FOR",
    "writer": "WROTE_LYRICS_FOR",
    "production": "DID_PRODUCTION_FOR",
    "performance": "PERFORMED_FOR",
    "composer": "IS_COMPOSER_FOR",
    "sound": "DID_SOUND_FOR",
    "remixer": "DID_REMIXING_FOR",
    "orchestrator": "IS_ORCHESTRATOR_FOR",
    "compiler": "DID_COMPILATION_FOR",
    "vocal arranger": "IS_ARRANGER_FOR",
    "arranger": "IS_ARRENGER_FOR",
    "mix-DJ": "MIXED",
    "editor": "IS_EDITOR_FOR",
    "illustration": "DID_ILLUSTRATION_FOR",
    "audio": "DID_AUDIO_FOR",
    "publishing": "IS_PUBLISHER_FOR",
    "art direction": "DID_ART_DIRECTOR_FOR",
    "design": "DID_DESIGN_FOR",
    "instrument arranger": "IS_ARRANGER_FOR",
    "chorus master": "IS_CHORUS_MASTER_FOR",
    "photography": "DID_PHOTOGRAPHY_FOR",
    "performer": "PERFORMED_IN",
    "graphic design": "DID_GRAPHIC_DESIGN_FOR",
    "booklet editor": "IS_BOOKLET_EDITOR_FOR",
    "programming": "DID_PROGRAMING_FOR",
    "copyright": "IS_COPYRIGHT_HOLDER_OF",
    "piano technician": "IS_PIANO_TECNICIAN_FOR",
    "phonographic copyright": "IS_PHONOGRAPHIC_COPYRIGHT_HOLDER_OF",
    "mastering": "DID_MASTERING_FOR",
    "vocal": "PERFORED_VOCALS_FOR",
    "librettist": "IS_LIBRETTIST_FOR",
    "mix": "MIXED",
    "recording": "DID_RECORDING_FOR",
    "concertmaster": "IS_CONCERTMASTER_FOR",
    "engineer": "IS_ENGINEER_FOR",

    # release_group
    "tribute": "IS_TRIBUTE_TO",
    "dedicated to": "IS_DEDICATED_TO",
    "creative direction": "",
    "artists and repertoire": ""
}

artist_artist_rel_map = {
    "teacher": "TEACHER_OF",
    "composer-in-residence": "HAS_COMPOSER-IN-RESIDENCE_STATUS_IN",
    "member of band": "IS_MEMBER_OF",
    "voice actor": "IS_VOICE_ACTOR_OF",
    "tribute": "IS_TRIBUTE_TO",
    "supporting musician": "IS_SUPPORTING_MUSICIAN_OF",
    "instrumental supporting musician": "IS_INSTRUMENTAL_SUPPORTING_MUSICIAN_OF",
    "personal relationship": "HAS_PERSONAL_RELATIONSHIP_WITH",
    "musical relationships": "HAS_MUSICAL_RELATIONSHIP_WITH",
    "collaboration": "HAS_COLLABORATED_WITH",
    "married": "IS_MARRIED_WITH",
    "sibling": "IS_SIBLING_OF",
    "parent": "IS_PARENT_OF",
    "is person": "IS",
    "conductor position": "IS_CONDUCTOR_OF",
    "vocal supporting musician": "DOES_VOCAL_SUPPORT_FOR",
    "artistic director": "IS_ARTIST_DIRECTOR_OF",
    "subgroup": "IS_SUBGROUP_OF",
    "founder": "IS_FOUNDER_OF",
    "involved with": "IS_INVOLVED_WITH",
    "named after": "IS_NAMED_AFTER",
}

label_label_rel_map = {
    "label rename": "WAS_RENAMED_TO",
    "imprint": "DOES_IMPRINT_FOR",
    "label distribution": "DOES_DISTRIBUTION_FOR",
    "business association": "HAS_BUSINESS_ASSOCIATION_TO",
    "label ownership": "OWNS",
    "label reissue": "DOES_REISSUING_FOR"
}

if not os.path.exists("repo"):
    os.mkdir("repo")
else:
    os.system("rm repo/*")
if not os.path.exists("tmp"):
    os.mkdir("tmp")
else:
    os.system("rm tmp/*")

with open("in/link", "r") as f:
    for line in f:
        cols = line.split("\t")
        links[cols[0]] = cols

with open("in/release_status", "r") as f:
    for line in f:
        cols = line.split("\t")
        release_statuses[cols[0]] = cols

with open("in/link_type", "r") as f:
    for line in f:
        cols = line.split("\t")
        link_types[cols[0]] = cols

with open("in/area", "r") as f:
    for line in f:
        cols = line.split("\t")
        areas[cols[0]] = cols

with open("in/label_type") as f:
    for line in f:
        cols = line.split("\t")

        label_types[cols[0]] = ";" + cols[1].replace(" ", "")

        if cols[3] != "\\N" and cols[2] in label_types:
            label_types[cols[0]] += label_types[cols[2]].replace(" ", "")

with open("in/artist") as f:
    for line in f:
        cols = line.split("\t")
        artists[cols[0]] = cols

with open("repo/area_area.csv", "w") as out:
    out.write(":START_ID(Area),:END_ID(Area)\n")

    with open("in/l_area_area", "r") as f:
        for line in f:
            cols = line.split("\t")
            out.write(",".join((areas[cols[3]][1],
                                areas[cols[2]][1]
                                )) + "\n")

with open("repo/area.csv", "w") as out:
    out.write("id:ID(Area),name\n")

    for k, area in areas.items():
        out.write(",".join((area[1],
                            '"' + area[2] + '"'
                            )) + "\n")

# ------


out_artist = open("repo/artist.csv", "w")
out_artist_area = open("repo/artist_area.csv", "w")

out_artist.write("id:ID(Artist),name,year:int,:LABEL\n")
out_artist_area.write(":START_ID(Artist),:END_ID(Area)\n")

for _, artist in artists.items():
    out_artist.write(",".join((
        artist[1],
        '"' + artist[2].replace("\"", "\"\"") + '"',
        artist[4] if artist[4] != "\\N" else "0",
        "Artist" + (";Group\n" if artist[10] == "2" else "\n")
    )))

    if artist[11] != "\\N":
        out_artist_area.write(artist[1] + "," + areas[artist[11]][1] + "\n")

out_artist.close()
out_artist_area.close()

with open("repo/artist_artist.csv", "w") as out:
    out.write(":START_ID(Artist),:END_ID(Artist),:TYPE\n")

    with open("in/l_artist_artist", "r") as f:
        for line in f:
            cols = line.split("\t")
            out.write(",".join((
                artists[cols[2]][1],
                artists[cols[3]][1],
                artist_artist_rel_map[link_types[links[cols[1]][1]][6]] + "\n"
            )))

#  --------

with open("in/release_group_primary_type") as f:
    for line in f:
        cols = line.split("\t")
        release_types[cols[0]] = ";" + cols[1]

release_group_year = dict()
with open("in/release_group_meta") as f:
    for line in f:
        cols = line.split("\t")
        release_group_year[cols[0]] = cols[2] if cols[2] != "\\N" else "0"

with open("repo/release.csv", "w") as out:
    out.write("id:ID(Release),name,year:int,:LABEL\n")

    with open("in/release_group") as f:
        for line in f:
            cols = line.split("\t")
            out.write(",".join((
                cols[1],
                '"' + cols[2].replace("\"", "\"\"") + '"',
                release_group_year[cols[0]],
                "Release" + release_types[cols[4]],
            )) + "\n")

            release_groups[cols[0]] = cols

with open("in/release") as f:
    for line in f:
        cols = line.split("\t")
        if cols[5] != '\\N' and release_statuses[cols[5]][1] == "Official":
            release_to_release_group_map[cols[0]] = cols[4]

credit_names = defaultdict(list)

with open("in/artist_credit_name") as f:
    for line in f:
        cols = line.split("\t")
        credit_names[cols[0]].append(artists[cols[2]][1])

with open("tmp/tmp_artist_release.csv", "w") as out:
    out.write(":START_ID(Artist),:END_ID(Release),:TYPE\n")

    # Is this part really necessary?
    with open("in/l_artist_release") as f:
        for line in f:
            cols = line.split("\t")
            if cols[3] in release_to_release_group_map:
                out.write(",".join((
                    artists[cols[2]][1],
                    release_groups[release_to_release_group_map[cols[3]]][1],
                    artist_release_rel_map[link_types[links[cols[1]][1]][6]]
                )) + "\n")

    # Artist credits
    with open("in/release") as f:
        for line in f:
            cols = line.split("\t")
            if cols[0] in release_to_release_group_map:
                for credit in credit_names[cols[3]]:
                    out.write(",".join((
                        credit,
                        release_groups[release_to_release_group_map[cols[0]]][1],
                        "CREDITED_FOR"
                    )) + "\n")

# Remove dupes
os.system("(head -n 1 tmp/tmp_artist_release.csv && tail -n +2 tmp/tmp_artist_release.csv"
          " | sort) | uniq > repo/artist_release.csv && rm tmp/tmp_artist_release.csv")


with open("repo/release_release.csv", "w") as out:
    out.write(":START_ID(Release),:END_ID(Release),:TYPE\n")

    with open("in/l_release_group_release_group") as f:
        for line in f:
            cols = line.split("\t")
            out.write(",".join((
                release_groups[cols[2]][1],
                release_groups[cols[3]][1],
                release_release_rel_map[link_types[links[cols[1]][1]][6]]
            )) + "\n")

# ---

with open("in/tag") as f:
    with open("repo/tag.csv", "w") as out:
        out.write("id:ID(Tag),name\n")

        for line in f:
            cols = line.split("\t")
            tags[cols[0]] = cols
            out.write(cols[0] + ",\"" + cols[1].replace("\"", "\"\"") + "\"\n")

with open("repo/release_tag.csv", "w") as out:
    out.write(":START_ID(Release),:END_ID(Tag),weight:int\n")

    with open("in/release_group_tag") as f:
        for line in f:
            cols = line.split("\t")

            if int(cols[2]) <= 0:
                continue

            out.write(",".join((
                release_groups[cols[0]][1],
                cols[1],
                cols[2],
            )) + "\n")

with open("repo/artist_tag.csv", "w") as out:
    out.write(":START_ID(Artist),:END_ID(Tag),weight:int\n")

    with open("in/artist_tag") as f:
        for line in f:
            cols = line.split("\t")

            if int(cols[2]) <= 0:
                continue

            out.write(",".join((
                artists[cols[0]][1],
                cols[1],
                cols[2],
            )) + "\n")

with open("repo/tag_tag.csv", "w") as out:
    out.write(":START_ID(Tag),:END_ID(Tag),weight:int\n")

    with open("in/tag_relation") as f:
        for line in f:
            cols = line.split("\t")

            if int(cols[2]) <= 0:
                continue

            out.write(",".join((
                cols[0],
                cols[1],
                cols[2],
            )) + "\n")

# -----

with open("repo/labels.csv", "w") as out:
    out.write("id:ID(Label),name,code,:LABEL\n")

    with open("in/label") as f:
        for line in f:
            cols = line.split("\t")
            labels[cols[0]] = cols

            out.write(",".join((
                cols[1],
                "\"" + cols[2].replace("\"", "\"\"") + "\"",
                cols[9] if cols[9] != "\\N" else "",
                "Label" + label_types[cols[10]]
            )) + "\n")

with open("repo/label_label.csv", "w") as out:
    out.write(":START_ID(Label),:END_ID(Label),:TYPE\n")

    with open("in/l_label_label") as f:
        for line in f:
            cols = line.split("\t")

            out.write(",".join((
                labels[cols[2]][1],
                labels[cols[3]][1],
                label_label_rel_map[link_types[links[cols[1]][1]][6]]
            )) + "\n")

# ---
