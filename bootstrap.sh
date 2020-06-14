#!/usr/bin/env bash

tmux new -d -s mgCover bash -c "./task_runner.sh task_get_cover.py --count 100 --threads 2"
tmux new -d -s mgLastfm bash -c "./task_runner.sh task_get_lastfm.py --count 500 --threads 5"
tmux new -d -s mgSpotify bash -c "./task_runner.sh task_get_spotify.py --count 500 --threads 10"
