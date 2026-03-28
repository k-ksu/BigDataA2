#!/bin/bash

source .venv/bin/activate

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop

export PYSPARK_DRIVER_PYTHON=$(which python)

unset PYSPARK_PYTHON

hdfs dfs -put -f data/parquet/m.parquet / && \
    echo "Parquet file uploaded to HDFS"

spark-submit prepare_data.py && \
    echo "Local document files created"

hdfs dfs -rm -r -f /data
hdfs dfs -rm -r -f /input/data

hdfs dfs -put data / && \
    echo "Documents uploaded to HDFS /data"

hdfs dfs -ls /data

spark-submit prepare_input.py && \
    echo "Input data prepared in HDFS /input/data"

hdfs dfs -ls /input/data

echo "Done data preparation!"
