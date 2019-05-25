#!/usr/bin/env bash

export NEO4J_HOME="/home/drone/Documents/neo4j"

cat seed.cypher | ${NEO4J_HOME}/bin/cypher-shell
