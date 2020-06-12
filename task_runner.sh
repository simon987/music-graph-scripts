#!/usr/bin/env bash

if [[ -z "${LASTFM_APIKEY}" ]]; then
  echo "LASTFM_APIKEY is not set"
  exit
fi

if [[ -z "${SPOTIFY_CLIENTID}" ]]; then
  echo "SPOTIFY_CLIENTID is not set"
  exit
fi

if [[ -z "${SPOTIFY_SECRET}" ]]; then
  echo "SPOTIFY_SECRET is not set"
  exit
fi

while true; do
  /usr/bin/time python "$1" --count 50 &>> "$1".log
done
