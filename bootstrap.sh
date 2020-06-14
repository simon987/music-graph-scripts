#!/usr/bin/env bash

tmux kill-session -t mgSpotify
tmux kill-session -t mgCover
tmux kill-session -t mgLastfm

tmux new -d -s mgCover bash -c "./task_runner.sh task_get_cover.py --count 100 --threads 2"
tmux new -d -s mgLastfm bash -c "./task_runner.sh task_get_lastfm.py --count 200 --threads 4"
tmux new -d -s mgSpotify bash -c "./task_runner.sh task_get_spotify.py --count 500 --threads 10"
