#!/bin/sh
set -e

# Perform the following outside this script:
#  export PQ_OUTPUT_REMOTE=<remote-https-path-with-token>

PQ_DIR=/usr/local/src/pq/
TMP_DIR=/var/tmp/pq/
CONFIG_FILE=/usr/local/src/pq/config.yaml
OUTPUT_FILE=power_usage.tsv

if ! git -C "$TMP_DIR" rev-parse --git-dir > /dev/null 2>&1; then
  mkdir -p "$TMP_DIR"
  if [ -n "$(ls -A ${TMP_DIR} 2>/dev/null)" ]; then
    # directory is not empty; explode
    exit 1
  else
    # clone output repository
    git clone "$PQ_OUTPUT_REMOTE" "$TMP_DIR" --quiet > /dev/null
  fi
fi

# update the repository
git -C "$TMP_DIR" pull --ff-only

# going to the prom
"$PQ_DIR"/promqueen.py -c "$CONFIG_FILE" -o "$TMP_DIR"/"$OUTPUT_FILE"

# sharing our glory
git -C "$TMP_DIR" add "$OUTPUT_FILE" > /dev/null
git -C "$TMP_DIR" -c user.name='Her Majesty The Prom Queen' -c user.email='promqueen@inm7.de' commit -m "Update from new scrape cycle" --quiet > /dev/null
git -C "$TMP_DIR" push "$PQ_OUTPUT_REMOTE" --quiet > /dev/null

