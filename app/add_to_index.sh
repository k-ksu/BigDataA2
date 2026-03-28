#!/bin/bash

set -e

FILE="$1"

if [ -z "$FILE" ]; then
    echo "Usage: bash add_to_index.sh <local_file_path>"
    exit 1
fi

if [ ! -f "$FILE" ]; then
    echo "[ERROR] File not found: $FILE"
    exit 1
fi

cd /app
source .venv/bin/activate

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "================================================================"
echo " add_to_index.sh — indexing: $FILE"
echo "================================================================"

python3 add_document.py "$FILE"

echo "================================================================"
echo " Done."
echo "================================================================"
