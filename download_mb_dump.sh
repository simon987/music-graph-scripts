#!/usr/bin/env bash

latest=$(curl http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/LATEST)

mkdir in 2> /dev/null
cd in

wget -nc "http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/${latest}/mbdump.tar.bz2"
wget -nc "http://ftp.musicbrainz.org/pub/musicbrainz/data/fullexport/${latest}/mbdump-derived.tar.bz2"

tar -xjvf mbdump.tar.bz2 mbdump/area mbdump/artist mbdump/l_area_area mbdump/l_artist_artist \
mbdump/l_artist_release mbdump/l_artist_release_group mbdump/l_label_label mbdump/l_release_group_release_group \
mbdump/label mbdump/label_type mbdump/link mbdump/link_type mbdump/release mbdump/release_group \
mbdump/release_group_primary_type mbdump/artist_credit_name mbdump/release_status
tar -xjvf mbdump-derived.tar.bz2 mbdump/artist_tag mbdump/release_group_tag mbdump/tag mbdump/tag_relation \
mbdump/release_group_meta

mv mbdump/* .
rm -r mbdump
cd ..