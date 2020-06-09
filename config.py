import os

config = {
    "DB": "musicbrainz_db",
    "USER": "musicbrainz",
    "PASSWORD": "musicbrainz",
    "HOST": "127.0.0.1",
    "PORT": 5433,

    "LASTFM_APIKEY": os.environ.get("LASTFM_APIKEY"),
    "LASTFM_USER": os.environ.get("LASTFM_USER"),

    "SPOTIFY_CLIENTID": os.environ.get("SPOTIFY_CLIENTID"),
    "SPOTIFY_SECRET": os.environ.get("SPOTIFY_SECRET"),
}


def connstr():
    return " dbname=%s user=%s password=%s host=%s port=%d" % (
        config["DB"], config["USER"], config["PASSWORD"],
        config["HOST"], config["PORT"]
    )
