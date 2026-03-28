#!/bin/bash

set -e

cd "$(dirname "$0")"

source .venv/bin/activate

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

echo "================================================================"
echo " store_index.sh — loading index from HDFS into Cassandra"
echo "================================================================"

echo "[INFO] Checking HDFS index paths …"

for PATH_TO_CHECK in /indexer/index /indexer/doc_stats; do
    if ! hdfs dfs -test -e "$PATH_TO_CHECK"; then
        echo "[ERROR] $PATH_TO_CHECK not found in HDFS."
        echo "        Run create_index.sh first."
        exit 1
    fi
    echo "  OK: $PATH_TO_CHECK"
done

echo ""

echo "[INFO] Starting Cassandra loader …"
python3 load_to_cassandra.py

echo ""
echo "================================================================"
echo " store_index.sh — done!"
echo "================================================================"
