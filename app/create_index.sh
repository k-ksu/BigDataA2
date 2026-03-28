#!/bin/bash

set -e

INPUT_PATH=${1:-/input/data}

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export HADOOP_USER_NAME=root
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root

echo "================================================================"
echo " create_index.sh  —  input: $INPUT_PATH"
echo "================================================================"

if ! jps | grep -q NodeManager; then
    echo "[INFO] NodeManager not running — starting it..."
    yarn --daemon start nodemanager
    echo "[INFO] Waiting 15 s for NodeManager to register with ResourceManager..."
    sleep 15
fi

echo "[INFO] Active JVM processes:"
jps -lm

STREAMING_JAR=$(find "$HADOOP_HOME/share/hadoop/tools/lib" \
                     -name 'hadoop-streaming-*.jar' 2>/dev/null | head -1)

if [ -z "$STREAMING_JAR" ]; then
    echo "[ERROR] hadoop-streaming jar not found under $HADOOP_HOME/share/hadoop/tools/lib"
    exit 1
fi
echo "[INFO] Streaming JAR: $STREAMING_JAR"

if ! hdfs dfs -test -e "$INPUT_PATH"; then
    echo "[ERROR] Input path $INPUT_PATH does not exist in HDFS."
    exit 1
fi
echo "[INFO] Input path $INPUT_PATH — OK"

echo "[INFO] Removing previous /indexer output (if any)..."
hdfs dfs -rm -r -f /indexer
hdfs dfs -mkdir -p /indexer

echo ""
echo "----------------------------------------------------------------"
echo " Pipeline 1: Inverted Index"
echo "----------------------------------------------------------------"

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="InvertedIndex" \
    -D mapreduce.job.reduces=1 \
    -input  "$INPUT_PATH" \
    -output /indexer/index \
    -mapper  "python3 mapper1.py" \
    -reducer "python3 reducer1.py" \
    -file /app/mapreduce/mapper1.py \
    -file /app/mapreduce/reducer1.py

echo "[INFO] Pipeline 1 complete."
echo "[INFO] /indexer/index contents:"
hdfs dfs -ls /indexer/index

echo ""
echo "----------------------------------------------------------------"
echo " Pipeline 2: Document & Corpus Statistics"
echo "----------------------------------------------------------------"

hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="DocStats" \
    -D mapreduce.job.reduces=1 \
    -input  "$INPUT_PATH" \
    -output /indexer/doc_stats \
    -mapper  "python3 mapper2.py" \
    -reducer "python3 reducer2.py" \
    -file /app/mapreduce/mapper2.py \
    -file /app/mapreduce/reducer2.py

echo "[INFO] Pipeline 2 complete."
echo "[INFO] /indexer/doc_stats contents:"
hdfs dfs -ls /indexer/doc_stats

echo ""
echo "================================================================"
echo " Index creation complete!"
echo "================================================================"
echo "[INFO] Full /indexer layout:"
hdfs dfs -ls -R /indexer

echo ""
echo "[INFO] Quick sanity check — first 3 lines of the inverted index:"
hdfs dfs -cat '/indexer/index/part-*' 2>/dev/null | head -3

echo ""
echo "[INFO] Quick sanity check — CORPUS line from doc_stats:"
hdfs dfs -cat '/indexer/doc_stats/part-*' 2>/dev/null | grep '^CORPUS'
