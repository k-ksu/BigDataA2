#!/bin/bash

set -e

if [ -z "$*" ]; then
    echo "Usage: bash search.sh \"<query text>\""
    echo "Example: bash search.sh \"machine learning\""
    exit 1
fi

QUERY="$*"

cd /app

source .venv/bin/activate

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export HADOOP_USER_NAME=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

export PYSPARK_DRIVER_PYTHON=$(which python)

export PYSPARK_PYTHON=./.venv/bin/python

if ! jps | grep -q NodeManager; then
    echo "[INFO] NodeManager not detected — starting it..."
    export YARN_NODEMANAGER_USER=root
    yarn --daemon start nodemanager
    echo "[INFO] Waiting 15 s for NodeManager to register..."
    sleep 15
fi

if [ ! -f /app/.venv.tar.gz ]; then
    echo "[INFO] .venv.tar.gz not found — packing virtualenv now..."
    venv-pack -o /app/.venv.tar.gz
fi

echo "================================================================"
echo " search.sh — BM25 query on YARN"
echo " Query: \"$QUERY\""
echo "================================================================"
echo ""

spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    --conf "spark.yarn.archive=hdfs:///apps/spark/spark-jars.zip" \
    --conf "spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python" \
    --conf "spark.executorEnv.PYSPARK_PYTHON=./.venv/bin/python" \
    --conf "spark.yarn.submit.waitAppCompletion=true" \
    --conf "spark.ui.showConsoleProgress=false" \
    /app/query.py "$QUERY"
