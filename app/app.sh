#!/bin/bash
set -e
cd /app

echo "================================================================"
echo " app.sh — Big Data Assignment 2"
echo "================================================================"

echo ""
echo "[Step 1/8] Starting SSH server..."
service ssh restart

echo ""
echo "[Step 2/8] Starting Hadoop / YARN / Spark services..."
bash start-services.sh

echo ""
echo "[Step 3/8] Setting up Python virtualenv..."

python3 -m venv .venv
source .venv/bin/activate

pip install --quiet -r requirements.txt

echo "[INFO] Packaging virtualenv for YARN executor distribution..."
venv-pack -f -o .venv.tar.gz
echo "[INFO] Virtualenv ready: .venv.tar.gz"

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

data_dir = "data"
txt_files = sorted(f for f in os.listdir(data_dir) if f.endswith(".txt"))

if not txt_files:
    print("ERROR: No .txt files found in app/data/.", file=sys.stderr)
    sys.exit(1)

safe_paths = []
skipped = []
for f in txt_files:
    try:
        f.encode("ascii")
        safe_paths.append(os.path.join(data_dir, f))
    except UnicodeEncodeError:
        skipped.append(f)

if skipped:
    print(f"[WARN] Skipping {len(skipped)} file(s) with non-ASCII characters in filename.",
          file=sys.stderr)

if not safe_paths:
    print("ERROR: No ASCII-safe files to upload.", file=sys.stderr)
    sys.exit(1)

print(f"[INFO] Uploading {len(safe_paths)} documents in a single hdfs put call...")

result = subprocess.run(["hdfs", "dfs", "-put", "-f"] + safe_paths + ["/data/"])

if result.returncode != 0:
    print(f"ERROR: hdfs put failed (exit code {result.returncode}).", file=sys.stderr)
    sys.exit(1)

print(f"[INFO] Upload complete: {len(safe_paths)} documents uploaded to HDFS /data.")
if skipped:
    print(f"[WARN] {len(skipped)} document(s) excluded (non-ASCII filename).")
PYEOF

echo "[INFO] HDFS /data count:"
hdfs dfs -count /data

echo ""
echo "[Step 5/8] Building /input/data with PySpark..."
spark-submit prepare_input.py
echo "[INFO] /input/data ready:"
hdfs dfs -ls /input/data

echo ""
echo "[Step 6-7/8] Building index and loading to Cassandra..."
bash index.sh

echo ""
echo "[Step 8/8] Running test search query..."
bash search.sh "machine learning"

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
