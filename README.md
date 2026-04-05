# Big Data Assignment 2 — Simple Search Engine

A distributed search engine built with **Hadoop MapReduce**, **PySpark**, and **Cassandra**.  
Documents are indexed using an inverted index and ranked with the **BM25** algorithm.

---

## Prerequisites

- Docker + Docker Compose

Nothing else — everything runs inside containers.

---

## Running

```bash
docker compose up
```

The `cluster-master` container runs the full pipeline automatically on startup.  
**First run takes ~25–35 minutes** (packages Spark JARs, runs MapReduce jobs, loads Cassandra).

You will see progress in the logs:

```
[Step 1/8] Starting SSH server...
[Step 2/8] Starting Hadoop / YARN / Spark services...
[Step 3/8] Setting up Python virtualenv...
[Step 4/8] Uploading documents to HDFS /data...
[Step 5/8] Building /input/data with PySpark...
[Step 6-7/8] Building index and loading to Cassandra...
[Step 8/8] Running test search query...
Pipeline complete! Container is running.
```

---

## Running Queries

Once the pipeline is complete, run queries from a separate terminal:

```bash
docker exec cluster-master bash /app/search.sh "machine learning"
docker exec cluster-master bash /app/search.sh "military aircraft"
docker exec cluster-master bash /app/search.sh "computer science algorithm"
```

Returns the **top-10 ranked documents** with BM25 scores.

---

## Architecture

```
app/data/*.txt  (1000 pre-extracted Wikipedia documents)
      │
      ▼
[PySpark RDD]  prepare_input.py
      │  builds one-partition TSV → HDFS /input/data
      │  format: doc_id \t title \t text
      │
      ▼
[Hadoop MapReduce Streaming]  create_index.sh
      ├── mapper1.py + reducer1.py → HDFS /indexer/index
      │       term \t df \t doc_id1:tf1 doc_id2:tf2 …
      └── mapper2.py + reducer2.py → HDFS /indexer/doc_stats
              DOC \t doc_id \t title \t length
              CORPUS \t total_docs \t avg_doc_length
      │
      ▼
[cassandra-driver]  load_to_cassandra.py
      │  keyspace: search_index
      │  tables: vocabulary, inverted_index, doc_stats, corpus_stats
      │
      ▼
[PySpark RDD on YARN]  query.py
      reads posting lists from Cassandra → BM25 scores → top-10
```

---

## File Structure

```
BigDataA2/
├── docker-compose.yml
└── app/
    ├── app.sh                  # pipeline entry point (runs on container start)
    ├── start-services.sh       # starts Hadoop / YARN / Spark
    ├── prepare_input.py        # PySpark RDD: /data → /input/data (TSV)
    ├── create_index.sh         # runs both MapReduce streaming jobs
    ├── store_index.sh          # loads HDFS index → Cassandra
    ├── index.sh                # create_index.sh + store_index.sh
    ├── load_to_cassandra.py    # Cassandra loader
    ├── search.sh               # submits query.py to YARN
    ├── query.py                # PySpark RDD BM25 ranker
    ├── requirements.txt
    ├── data/                   # 1000 pre-extracted .txt documents
    └── mapreduce/
        ├── mapper1.py          # tokenise → (term \t doc_id)
        ├── reducer1.py         # → term \t df \t posting_list
        ├── mapper2.py          # → doc length per document
        └── reducer2.py         # → DOC lines + CORPUS line
```
