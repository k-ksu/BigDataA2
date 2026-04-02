#!/bin/bash
# app.sh
# Full pipeline entry point — runs automatically when the container starts.
#
# Flow:
#   1. Start SSH (required by Hadoop start scripts)
#   2. Start all Hadoop / YARN / Spark services
#   3. Create Python virtualenv and install dependencies
#   4. Upload the 1000 .txt documents to HDFS /data
#   5. Build /input/data (TSV: doc_id \t title \t text) with PySpark
#   6. Build the inverted index with Hadoop MapReduce (create_index.sh)
#   7. Load the index into Cassandra (store_index.sh)
#   8. Run a test search query
#   9. Keep the container alive so the professor can run more queries
#
# The professor can:
#   • Run extra queries  : docker exec cluster-master bash /app/search.sh "<query>"
#   • Re-index new data  : docker exec cluster-master bash /app/index.sh <hdfs_path>

set -e
cd /app

echo "================================================================"
echo " app.sh — Big Data Assignment 2"
echo "================================================================"

# ---------------------------------------------------------------------------
# 1. SSH — required by Hadoop sbin scripts to start daemons on worker nodes
# ---------------------------------------------------------------------------
echo ""
echo "[Step 1/8] Starting SSH server..."
service ssh restart

# ---------------------------------------------------------------------------
# 2. Hadoop / YARN / Spark services
#    start-services.sh is idempotent: safe to run on fresh or restarted
#    containers. It writes yarn-site.xml, spark-defaults.conf, sets the
#    workers file to localhost, formats HDFS if needed, starts all daemons,
#    and uploads the Spark JAR archive to HDFS on the first run.
# ---------------------------------------------------------------------------
echo ""
echo "[Step 2/8] Starting Hadoop / YARN / Spark services..."
bash start-services.sh

# ---------------------------------------------------------------------------
# 3. Python virtualenv
#    Created inside /app so it lives on the mounted volume and persists
#    across container restarts.  venv-pack archives it for YARN executors.
# ---------------------------------------------------------------------------
echo ""
echo "[Step 3/8] Setting up Python virtualenv..."

python3 -m venv .venv
source .venv/bin/activate

pip install --quiet -r requirements.txt

echo "[INFO] Packaging virtualenv for YARN executor distribution..."
venv-pack -o .venv.tar.gz
echo "[INFO] Virtualenv ready: .venv.tar.gz"

# ---------------------------------------------------------------------------
# 4. Upload documents to HDFS /data
#    The repository already contains 1000 pre-extracted .txt documents in
#    app/data/.  We upload them directly — no parquet file needed.
#    A Python helper is used so that filenames with Unicode characters
#    (e.g. dashes, diacritics) are handled correctly by the HDFS client.
# ---------------------------------------------------------------------------
echo ""
echo "[Step 4/8] Uploading documents to HDFS /data..."

export HADOOP_CONF_DIR=/usr/local/hadoop/etc/hadoop
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

hdfs dfs -rm -r -f /data
hdfs dfs -rm -r -f /input
hdfs dfs -mkdir -p /data

python3 - << 'PYEOF'
import os, subprocess, sys

env = {**os.environ}
ok = 0
fail = 0

for fname in sorted(os.listdir("data")):
    if not fname.endswith(".txt"):
        continue
    result = subprocess.run(
        ["hdfs", "dfs", "-put", "-f", "data/" + fname, "/data/"],
        capture_output=True,
        env=env,
    )
    if result.returncode == 0:
        ok += 1
    else:
        fail += 1
        print(f"  WARN: failed to upload {fname}: {result.stderr.decode()[:120]}",
              file=sys.stderr)

print(f"  Uploaded {ok} documents to HDFS /data  ({fail} failed)")
if ok == 0:
    print("ERROR: No documents uploaded. Check that app/data/*.txt files exist.",
          file=sys.stderr)
    sys.exit(1)
PYEOF

echo "[INFO] HDFS /data count:"
hdfs dfs -count /data

# ---------------------------------------------------------------------------
# 5. Build /input/data with PySpark RDD
#    Reads every .txt file from HDFS /data and produces a single-partition
#    TSV file: doc_id \t doc_title \t doc_text
#    This is the input format expected by the MapReduce indexer.
# ---------------------------------------------------------------------------
echo ""
echo "[Step 5/8] Building /input/data with PySpark..."
spark-submit prepare_input.py
echo "[INFO] /input/data ready:"
hdfs dfs -ls /input/data

# ---------------------------------------------------------------------------
# 6 + 7. Build index and load to Cassandra
#    index.sh calls create_index.sh (two MapReduce streaming jobs) and then
#    store_index.sh (loads HDFS output into four Cassandra tables).
#    Cassandra may still be starting up; load_to_cassandra.py retries for
#    up to 150 seconds automatically.
# ---------------------------------------------------------------------------
echo ""
echo "[Step 6-7/8] Building index and loading to Cassandra..."
bash index.sh

# ---------------------------------------------------------------------------
# 8. Test search query
# ---------------------------------------------------------------------------
echo ""
echo "[Step 8/8] Running test search query..."
bash search.sh "machine learning"

# ---------------------------------------------------------------------------
# Keep container alive
# The professor can now exec into the container to run additional queries
# or re-index new documents:
#
#   docker exec cluster-master bash /app/search.sh "your query here"
#   docker exec cluster-master bash /app/index.sh /input/data
# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo " Pipeline complete!  Container is running."
echo ""
echo " Run more queries:"
echo "   docker exec cluster-master bash /app/search.sh \"<query>\""
echo ""
echo " Re-index with different data:"
echo "   docker exec cluster-master bash /app/index.sh <hdfs_path>"
echo "================================================================"

tail -f /dev/null
