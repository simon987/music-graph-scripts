#!/usr/bin/env bash

export NEO4J_HOME="/home/drone/Downloads/neo4j-community-3.5.3"

cat seed.cypher | ${NEO4J_HOME}/bin/cypher-shell
