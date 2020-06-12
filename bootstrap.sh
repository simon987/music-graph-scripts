#!/usr/bin/env bash

tmux new -d -s mgCover bash -c "./task_runner.sh task_get_cover.py"

tmux new -d -s mgLastfm bash -c "./task_runner.sh task_get_lastfm"
tmux new -d -s mgLastfm2 bash -c "./task_runner.sh task_get_lastfm"

tmux new -d -s mgSpotify bash -c "./task_runner.sh task_get_spotify.py"
tmux new -d -s mgSpotify2 bash -c "./task_runner.sh task_get_spotify.py"
