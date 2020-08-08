CREATE OR REPLACE FUNCTION fn_sortname(name text, mb_sortname text) RETURNS text AS
$$
declare
    sn text;
BEGIN

    sn = regexp_replace(name, '[^a-zA-Z0-9.\-!?&çéàâäëïöü'' ]', '_', 'g');

    if length(replace(sn, '_', '')) = 0 then
        return upper(regexp_replace(mb_sortname, '[^\w.\-!?& ]', '_', 'g'));
    end if;

    return upper(sn);
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fn_sortname(name text) RETURNS text AS
$$
BEGIN
    return upper(regexp_replace(name, '[^a-zA-Z0-9.\-!?&çéàâäëïöü'' ]', '_'));
END
$$ LANGUAGE plpgsql;

CREATE TABLE mg.translate_artist_artist_rel
(
    mb_name TEXT PRIMARY KEY,
    mg_name TEXT
);
INSERT INTO mg.translate_artist_artist_rel
VALUES ('teacher', 'TEACHER_OF'),
       ('composer-in-residence', 'HAS_COMPOSER-IN-RESIDENCE_STATUS_IN'),
       ('member of band', 'IS_MEMBER_OF'),
       ('voice actor', 'IS_VOICE_ACTOR_OF'),
       ('tribute', 'IS_TRIBUTE_TO'),
       ('supporting musician', 'IS_SUPPORTING_MUSICIAN_OF'),
       ('instrumental supporting musician', 'IS_INSTRUMENTAL_SUPPORTING_MUSICIAN_OF'),
       ('personal relationship', 'HAS_PERSONAL_RELATIONSHIP_WITH'),
       ('musical relationships', 'HAS_MUSICAL_RELATIONSHIP_WITH'),
       ('collaboration', 'HAS_COLLABORATED_WITH'),
       ('married', 'IS_MARRIED_WITH'),
       ('sibling', 'IS_SIBLING_OF'),
       ('parent', 'IS_PARENT_OF'),
       ('is person', 'IS'),
       ('conductor position', 'IS_CONDUCTOR_OF'),
       ('vocal supporting musician', 'DOES_VOCAL_SUPPORT_FOR'),
       ('artistic director', 'IS_ARTIST_DIRECTOR_OF'),
       ('subgroup', 'IS_SUBGROUP_OF'),
       ('founder', 'IS_FOUNDER_OF'),
       ('involved with', 'IS_INVOLVED_WITH'),
       ('named after', 'IS_NAMED_AFTER');

CREATE TABLE mg.translate_artist_release_rel
(
    mb_name TEXT PRIMARY KEY,
    mg_name text
);
INSERT INTO mg.translate_artist_release_rel
VALUES ('translator', 'TRANSLATED'),
       ('liner notes', 'WROTE_LINER_NOTES'),
       ('lyricist', 'IS_LYRICIST_FOR'),
       ('lacquer cut', 'DID_LACQUER_CUT_FOR'),
       ('samples from artist', 'HAS_SAMPLES_IN'),
       ('remixes and compilations', NULL),
       ('composition', 'COMPOSED'),
       ('booking', 'DID_BOOKING_FOR'),
       ('balance', 'DID_BALANCE_FOR'),
       ('misc', 'HAS_MISC_ROLE_IN'),
       ('conductor', 'CONDUCTED'),
       ('legal representation', 'PROVIDED_LEGAL_REPRESENTATION_FOR'),
       ('design/illustration', 'DID_DESIGN_FOR'),
       ('performing orchestra', 'PERFORMED_FOR'),
       ('producer', 'PRODUCED'),
       ('instrument', 'PERFORMED_INSTRUMENT_FOR'),
       ('writer', 'WROTE_LYRICS_FOR'),
       ('production', 'DID_PRODUCTION_FOR'),
       ('performance', 'PERFORMED_FOR'),
       ('composer', 'IS_COMPOSER_FOR'),
       ('sound', 'DID_SOUND_FOR'),
       ('remixer', 'DID_REMIXING_FOR'),
       ('orchestrator', 'IS_ORCHESTRATOR_FOR'),
       ('compiler', 'DID_COMPILATION_FOR'),
       ('vocal arranger', 'IS_ARRANGER_FOR'),
       ('arranger', 'IS_ARRENGER_FOR'),
       ('mix-DJ', 'MIXED'),
       ('editor', 'IS_EDITOR_FOR'),
       ('illustration', 'DID_ILLUSTRATION_FOR'),
       ('audio', 'DID_AUDIO_FOR'),
       ('publishing', 'IS_PUBLISHER_FOR'),
       ('art direction', 'DID_ART_DIRECTION_FOR'),
       ('design', 'DID_DESIGN_FOR'),
       ('instrument arranger', 'IS_ARRANGER_FOR'),
       ('chorus master', 'IS_CHORUS_MASTER_FOR'),
       ('photography', 'DID_PHOTOGRAPHY_FOR'),
       ('performer', 'PERFORMED_IN'),
       ('graphic design', 'DID_GRAPHIC_DESIGN_FOR'),
       ('booklet editor', 'IS_BOOKLET_EDITOR_FOR'),
       ('programming', 'DID_PROGRAMING_FOR'),
       ('copyright', 'IS_COPYRIGHT_HOLDER_OF'),
       ('piano technician', 'IS_PIANO_TECNICIAN_FOR'),
       ('phonographic copyright', 'IS_PHONOGRAPHIC_COPYRIGHT_HOLDER_OF'),
       ('mastering', 'DID_MASTERING_FOR'),
       ('vocal', 'PERFORED_VOCALS_FOR'),
       ('librettist', 'IS_LIBRETTIST_FOR'),
       ('mix', 'MIXED'),
       ('recording', 'DID_RECORDING_FOR'),
       ('concertmaster', 'IS_CONCERTMASTER_FOR'),
       ('engineer', 'IS_ENGINEER_FOR'),
       ('tribute', 'IS_TRIBUTE_TO'),
       ('dedicated to', 'IS_DEDICATED_TO'),
       ('creative direction', NULL),
       ('artists and repertoire', NULL);


CREATE TABLE mg.translate_label_label_rel
(
    mb_name TEXT PRIMARY KEY,
    mg_name text
);
INSERT INTO mg.translate_label_label_rel
VALUES ('label rename', 'WAS_RENAMED_TO'),
       ('imprint', 'DOES_IMPRINT_FOR'),
       ('label distribution', 'DOES_DISTRIBUTION_FOR'),
       ('business association', 'HAS_BUSINESS_ASSOCIATION_TO'),
       ('label ownership', 'OWNS'),
       ('label reissue', 'DOES_REISSUING_FOR');


CREATE OR REPLACE VIEW mg.artist AS
SELECT gid                                                        as "id:ID(Artist)",
       artist.name,
       fn_sortname(artist.name, sort_name)                        as sortname,
       COALESCE(begin_date_year, 0)                               as "year:int",
       comment,
       COALESCE(lfm.listeners, 0)                                 as "listeners:int",
       COALESCE(lfm.playcount, 0)                                 as "playcount:int",
       (CASE WHEN type = 2 THEN 'Artist;Group' ELSE 'Artist' END) as ":LABEL"
FROM artist
         LEFT JOIN mg.lastfm_artist lfa ON lfa.mbid = artist.gid
         LEFT JOIN mg.lastfm_artist_meta lfm ON lfa.name = lfm.name;

CREATE OR REPLACE VIEW mg.artist_artist AS
SELECT a0.gid    as ":START_ID(Artist)",
       a1.gid    as ":END_ID(Artist)",
       0         as "weight:float",
       t.mg_name as ":TYPE"
FROM l_artist_artist
         INNER JOIN artist a0 ON entity0 = a0.id
         INNER JOIN artist a1 ON entity1 = a1.id
         INNER JOIN link l on l.id = l_artist_artist.link
         INNER JOIN link_type lt ON lt.id = l.link_type
         INNER JOIN mg.translate_artist_artist_rel t ON t.mb_name = lt.name
UNION ALL
SELECT lfa0.mbid,
       lfa1.mbid,
       weight,
       'IS_RELATED_TO'
FROM mg.lastfm_artist_artist
         INNER JOIN mg.lastfm_artist lfa0 ON lfa0.name = mg.lastfm_artist_artist.name0
         INNER JOIN mg.lastfm_artist lfa1 ON lfa1.name = mg.lastfm_artist_artist.name1
WHERE lfa0.mbid IN (SELECT gid FROM artist)
  AND lfa1.mbid IN (SELECT gid FROM artist)
UNION ALL
SELECT s0.mbid,
       s1.mbid,
       index::float / 20,
       'IS_RELATED_TO'
FROM mg.spotify_artist_artist
         INNER JOIN mg.spotify_artist s0 ON s0.spotid = mg.spotify_artist_artist.spotid0
         INNER JOIN mg.spotify_artist s1 ON s1.spotid = mg.spotify_artist_artist.spotid1
WHERE s0.mbid IN (SELECT gid FROM artist)
  AND s1.mbid IN (SELECT gid FROM artist);



CREATE OR REPLACE VIEW mg.release AS
SELECT release_group.gid                      as "id:ID(Release)",
       release_group.name,
       coalesce(m.first_release_date_year, 0) as "year:int",
       CONCAT('Release;', t.name)             as ":LABEL"
FROM release_group
         INNER JOIN release_group_meta m ON m.id = release_group.id
         LEFT JOIN release_group_primary_type t ON t.id = release_group.type;

CREATE OR REPLACE VIEW mg.artist_release AS
SELECT a.gid     as ":START_ID(Artist)",
       rg.gid    as ":END_ID(Release)",
       t.mg_name as ":TYPE"
FROM l_artist_release_group
         INNER JOIN artist a on a.id = l_artist_release_group.entity0
         INNER JOIN release_group rg on rg.id = l_artist_release_group.entity1
         INNER JOIN release r on r.release_group = l_artist_release_group.id
         INNER JOIN link l on l.id = l_artist_release_group.link
         INNER JOIN link_type lt ON lt.id = l.link_type
         INNER JOIN mg.translate_artist_release_rel t ON t.mb_name = lt.name
WHERE t.mg_name IS NOT NULL
  AND r.status = 1
UNION ALL
SELECT a.gid     as ":START_ID(Artist)",
       rg.gid    as ":END_ID(Release)",
       t.mg_name as ":TYPE"
FROM l_artist_release
         INNER JOIN artist a on a.id = l_artist_release.entity0
         INNER JOIN release r on r.id = l_artist_release.entity1
         INNER JOIN release_group rg on rg.id = r.release_group
         INNER JOIN link l on l.id = l_artist_release.link
         INNER JOIN link_type lt ON lt.id = l.link_type
         INNER JOIN mg.translate_artist_release_rel t ON t.mb_name = lt.name
WHERE t.mg_name IS NOT NULL
  AND r.status = 1
UNION ALL
SELECT a.gid          as ":START_ID(Artist)",
       rg.gid         as ":END_ID(Release)",
       'CREDITED_FOR' as ":TYPE"
FROM release
         INNER JOIN artist_credit_name cn ON cn.artist_credit = release.artist_credit
         INNER JOIN artist a on a.id = cn.artist
         INNER JOIN release_group rg on rg.id = release.release_group
WHERE status = 1;

CREATE OR REPLACE VIEW mg.track AS
SELECT row_number() over (ORDER BY sa.mbid) as "id:ID(Track)",
       mg.spotify_artist_track.album        as "album",
       mg.spotify_artist_track.track        as "track",
       mg.spotify_artist_track.url          as "url"
FROM mg.spotify_artist_track
         INNER JOIN mg.spotify_artist sa ON spotify_artist_track.spotid = sa.spotid
         INNER JOIN mg.artist ON "id:ID(Artist)" = sa.mbid
WHERE url != 'None';


CREATE OR REPLACE VIEW mg.artist_track AS
SELECT sa.mbid                              as ":START_ID(Artist)",
       row_number() over (ORDER BY sa.mbid) as ":END_ID(Track)"
FROM mg.spotify_artist_track
         INNER JOIN mg.spotify_artist sa ON spotify_artist_track.spotid = sa.spotid
            INNER JOIN mg.artist ON "id:ID(Artist)" = sa.mbid
WHERE url != 'None';


DROP VIEW mg.tag CASCADE ;
CREATE OR REPLACE VIEW mg.tag AS
WITH occurences AS (
    SELECT name,
           COUNT(*) as count
    FROM (
             SELECT lower(name) as name
             FROM release_group_tag
                      INNER JOIN tag t ON t.id = tag
             UNION ALL
             SELECT lower(name)
             FROM release_tag
                      INNER JOIN tag t ON t.id = tag
             UNION ALL
             SELECT lower(name)
             FROM mg.lastfm_artist_tag
             UNION ALL
             SELECT tag
             FROM mg.spotify_artist_tag
         ) as tags
    GROUP BY name
)
SELECT row_number() over (ORDER BY name) as "id:ID(Tag)",
       name,
       count                            as "occurences:int"
FROM occurences
WHERE count > 5;


CREATE OR REPLACE VIEW mg.release_tag AS
SELECT rg.gid                                                      as ":START_ID(Release)",
       t."id:ID(Tag)"                                              as ":END_ID(Tag)",
       greatest(least(release_group_tag.count::float / 6, 1), 0.2) as "weight:float"
FROM release_group_tag
         INNER JOIN release_group rg ON rg.id = release_group_tag.release_group
         INNER JOIN tag mbt ON mbt.id = release_group_tag.tag
         INNER JOIN mg.tag t ON t.name = mbt.name
WHERE release_group_tag.count > 0
UNION ALL
SELECT rg.gid                                                as ":START_ID(Release)",
       t."id:ID(Tag)"                                        as ":END_ID(Tag)",
       greatest(least(release_tag.count::float / 6, 1), 0.2) as "weight:float"
FROM release_tag
         INNER JOIN release r ON r.id = release_tag.release
         INNER JOIN release_group rg ON rg.id = r.release_group
         INNER JOIN tag mbt ON mbt.id = release_tag.tag
         INNER JOIN mg.tag t ON t.name = mbt.name
WHERE release_tag.count > 0
  AND r.status = 1;

CREATE OR REPLACE VIEW mg.artist_tag AS
SELECT a.gid                                                as ":START_ID(Artist)",
       t."id:ID(Tag)"                                       as ":END_ID(Tag)",
       greatest(least(artist_tag.count::float / 8, 1), 0.2) as "weight:float"
FROM artist_tag
         INNER JOIN artist a on artist_tag.artist = a.id
         INNER JOIN tag mbt ON mbt.id = artist_tag.tag
         INNER JOIN mg.tag t ON t.name = mbt.name
UNION ALL
SELECT DISTINCT a.gid          as ":START_ID(Artist)",
                t."id:ID(Tag)" as ":END_ID(Tag)",
                0.5            as "weight:float"
FROM mg.lastfm_artist_tag lfmt
         INNER JOIN mg.lastfm_artist lfma on lfmt.name = lfma.name
         INNER JOIN artist a on a.gid = lfma.mbid
         INNER JOIN mg.tag t ON t.name = lower(lfmt.tag)
UNION ALL
SELECT DISTINCT a.gid          as ":START_ID(Artist)",
                t."id:ID(Tag)" as ":END_ID(Tag)",
                0.5            as "weight:float"
FROM mg.spotify_artist_tag spott
         INNER JOIN mg.spotify_artist spota on spota.spotid = spott.spotid
         INNER JOIN artist a on a.gid = spota.mbid
         INNER JOIN mg.tag t ON t.name = lower(spott.tag);

CREATE OR REPLACE VIEW mg.tag_tag AS
SELECT t1."id:ID(Tag)"                                          as ":START_ID(Tag)",
       t2."id:ID(Tag)"                                          as ":END_ID(Tag)",
       greatest(least(tag_relation.weight::float / 12, 1), 0.2) as "weight:float"
FROM tag_relation
         INNER JOIN tag mbt1 ON mbt1.id = tag_relation.tag1
         INNER JOIN tag mbt2 ON mbt2.id = tag_relation.tag2
         INNER JOIN mg.tag t1 ON mbt1.name = t1.name
         INNER JOIN mg.tag t2 ON mbt1.name = t2.name;


CREATE OR REPLACE VIEW mg.label AS
SELECT label.gid               as "id:ID(Label)",
       label.name,
       fn_sortname(label.name) as sortname,
--        label_code                    as code,
       (CASE
            WHEN lt.name IS NULL
                THEN concat('Label;', lt.name)
            ELSE 'Label' END)  as ":LABEL"
FROM label
         LEFT JOIN label_type lt on label.type = lt.id;

CREATE OR REPLACE VIEW mg.release_label AS
SELECT rg.gid as ":START_ID(Release)",
       l.gid  as ":END_ID(Label)"
FROM l_label_release
         INNER JOIN label l on l_label_release.entity0 = l.id
         INNER JOIN release r on l_label_release.entity1 = r.id
         INNER JOIN release_group rg on r.release_group = rg.id
WHERE r.status = 1;
-- UNION
-- SELECT l.gid as ":START_ID(Release)",
--        r.gid as ":END_ID(Label)"
-- FROM l_label_release_group
--          INNER JOIN label l on l_label_release_group.entity0 = l.id
--          INNER JOIN release_group rg on l_label_release_group.entity1 = rg.id
--          INNER JOIN release r on r.release_group = rg.id


CREATE OR REPLACE VIEW mg.label_label AS
SELECT l0.gid    as ":START_ID(Label)",
       l1.gid    as ":END_ID(Label)",
       t.mg_name as ":TYPE"
FROM l_label_label
         INNER JOIN label l0 on l_label_label.entity0 = l0.id
         INNER JOIN label l1 on l_label_label.entity1 = l1.id
         INNER JOIN link l on l.id = l_label_label.link
         INNER JOIN link_type lt ON lt.id = l.link_type
         INNER JOIN mg.translate_label_label_rel t ON t.mb_name = lt.name;

--------------

CREATE TABLE mg.covers
(
    mbid uuid PRIMARY KEY,
    ts   timestamp DEFAULT CURRENT_TIMESTAMP,
    tn   bytea
);

CREATE TABLE mg.lastfm_artist
(
    name TEXT PRIMARY KEY,
    mbid uuid UNIQUE
);

CREATE TABLE mg.lastfm_raw_data
(
    name TEXT,
    mbid uuid,
    ts   timestamp DEFAULT CURRENT_TIMESTAMP,
    data jsonb,
    PRIMARY KEY (name, mbid)
);

CREATE TABLE mg.lastfm_artist_meta
(
    name      TEXT PRIMARY KEY,
    listeners int,
    playcount int
);

CREATE TABLE mg.lastfm_artist_tag
(
    name TEXT,
    tag  TEXT,
    PRIMARY KEY (name, tag)
);

CREATE TABLE mg.lastfm_artist_artist
(
    name0  TEXT,
    name1  TEXT,
    weight float,
    ts     timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (name0, name1)
);

--------------

CREATE TABLE mg.spotify_artist
(
    spotid TEXT PRIMARY KEY,
    mbid   UUID UNIQUE
);

CREATE TABLE mg.spotify_artist_meta
(
    spotid     TEXT PRIMARY KEY,
    name       TEXT,
    followers  int,
    popularity int,
    ts         timestamp DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE mg.spotify_artist_tag
(
    spotid TEXT,
    tag    TEXT,
    PRIMARY KEY (spotid, tag)
);

CREATE TABLE mg.spotify_artist_album
(
    spotid TEXT,
    album  TEXT,
    PRIMARY KEY (spotid, album)
);

CREATE TABLE mg.spotify_artist_track
(
    spotid TEXT,
    track  TEXT,
    album  TEXT,
    url    TEXT,
    PRIMARY KEY (spotid, track)
);

CREATE TABLE mg.spotify_artist_artist
(
    spotid0 TEXT,
    spotid1 TEXT,
    index   int,
    ts      timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (spotid0, spotid1)
);


CREATE TABLE mg.spotify_raw_data
(
    query    TEXT,
    endpoint TEXT,
    ts       timestamp DEFAULT CURRENT_TIMESTAMP,
    data     jsonb,
    PRIMARY KEY (query, endpoint)
);

--------
CREATE OR REPLACE FUNCTION asciifold(text) RETURNS text
AS
'/pglib/libasciifolding.so',
'asciifold' LANGUAGE C STRICT
                       PARALLEL SAFE;

CREATE OR REPLACE FUNCTION asciifold_lower(text) RETURNS text
AS
'/pglib/libasciifolding.so',
'asciifold_lower' LANGUAGE C STRICT
                             PARALLEL SAFE;
