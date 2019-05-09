#!/bin/bash

export NEO4J_HOME="/home/drone/Downloads/neo4j-community-3.5.3"
export REPOSITORY="http://localhost:9999"
export DATABASE="graph.db"

rm -rf "${NEO4J_HOME}/data/databases/${DATABASE}"

cp ${NEO4J_HOME}/conf/neo4j.conf ${NEO4J_HOME}/conf/neo4j.conf.bak
echo "dbms.security.auth_enabled=false" >> ${NEO4J_HOME}/conf/neo4j.conf

mkdir workspace 2> /dev/null
cd workspace
rm *.csv

wget ${REPOSITORY}/area.csv
wget ${REPOSITORY}/area_area.csv
wget ${REPOSITORY}/lastfm_artist.csv
wget ${REPOSITORY}/artist_area.csv
wget ${REPOSITORY}/artist_artist.csv
wget ${REPOSITORY}/artist_release.csv
wget ${REPOSITORY}/release.csv
wget ${REPOSITORY}/tag.csv
wget ${REPOSITORY}/tag_tag.csv
wget ${REPOSITORY}/release_tag.csv
wget ${REPOSITORY}/release_release.csv
wget ${REPOSITORY}/artist_tag.csv
wget ${REPOSITORY}/labels.csv
wget ${REPOSITORY}/label_label.csv
wget ${REPOSITORY}/lastfm_artist_artist.csv

. ${NEO4J_HOME}/bin/neo4j-admin import \
    --database ${DATABASE}\
    --high-io=true\
    --nodes:Area:MusicBrainzEntity "area.csv"\
    --nodes:MusicBrainzEntity "release.csv"\
    --nodes:MusicBrainzEntity "lastfm_artist.csv"\
    --nodes:Tag "tag.csv"\
    --nodes:MusicBrainzEntity "labels.csv"\
    --relationships:IS_PART_OF "area_area.csv"\
    --relationships:IS_BASED_IN "artist_area.csv"\
    --relationships "artist_artist.csv"\
    --relationships "artist_release.csv"\
    --relationships:IS_TAGGED "release_tag.csv"\
    --relationships:IS_TAGGED "artist_tag.csv"\
    --relationships:IS_RELATED_TO "tag_tag.csv"\
    --relationships "label_label.csv"\
    --relationships "release_release.csv"\
    --relationships:IS_RELATED_TO "lastfm_artist_artist.csv"

rm *.csv
cd ..

