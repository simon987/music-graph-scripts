#!/usr/bin/env bash

NEO4J_HOME="/home/simon/Documents/neo4j"
DATABASE="musicgraph"
PGPORT=5433
PGHOST=localhost

copy_csv() {
  PGPASSWORD=musicbrainz psql -p ${PGPORT} -h ${PGHOST} -U musicbrainz musicbrainz_db \
    -c "COPY (SELECT * FROM mg.${1}) TO STDOUT CSV HEADER" > "${1}.csv" && echo "${1}.csv"
}

mkdir workspace 2> /dev/null

${NEO4J_HOME}/bin/neo4j stop
rm -rf "${NEO4J_HOME}/data/databases/${DATABASE}"

(
  cd workspace

#  copy_csv "label"
#  copy_csv "artist_artist"
#  copy_csv "artist_release"
#  copy_csv "artist_tag"
#  copy_csv "release_tag"
#  copy_csv "release_label"
#  copy_csv "tag_tag"
#  copy_csv "label_label"
#  copy_csv "release"
#  copy_csv "artist"
#  copy_csv "tag"

  rm -rf "${NEO4J_HOME}/data/databases/${DATABASE}" 2>/dev/null
  . ${NEO4J_HOME}/bin/neo4j-admin import \
      --database "${DATABASE}"\
      --nodes=MusicBrainzEntity="artist.csv"\
      --nodes=MusicBrainzEntity="release.csv"\
      --nodes=Tag="tag.csv"\
      --nodes=MusicBrainzEntity="label.csv"\
      --relationships="artist_artist.csv"\
      --relationships="artist_release.csv"\
      --relationships=IS_TAGGED="artist_tag.csv"\
      --relationships=IS_TAGGED="release_tag.csv"\
      --relationships=RELEASE_UNDER="release_label.csv"\
      --relationships=IS_RELATED_TO="tag_tag.csv"\
      --relationships="label_label.csv"

#  rm ./*.csv
)


${NEO4J_HOME}/bin/neo4j start
sleep 15
${NEO4J_HOME}/bin/cypher-shell < seed.cypher
