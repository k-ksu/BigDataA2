# Big Data Assignment 2 — Search Engine

A distributed search engine built on **Hadoop MapReduce**, **Apache Spark (PySpark)**, and **Cassandra**.  
Documents are indexed using an inverted index and ranked at query time using the **BM25** algorithm.

---

## Architecture

```
Parquet file
    │
    ▼
[PySpark] prepare_data.py
    │  extracts 1000 documents → HDFS /data  (individual .txt files)
    │  builds one-partition TSV → HDFS /input/data
    │
    ▼
[Hadoop MapReduce] create_index.sh
    │
    ├── Pipeline 1  mapper1.py + reducer1.py
    │       input : /input/data
    │       output: /indexer/index   (term \t df \t doc_id1:tf1 doc_id2:tf2 …)
    │
    └── Pipeline 2  mapper2.py + reducer2.py
            input : /input/data
            output: /indexer/doc_stats  (DOC/CORPUS lines with lengths & averages)
    │
    ▼
[Python + cassandra-driver] store_index.sh
    │  loads /indexer/index and /indexer/doc_stats into Cassandra
    │
    │  Tables (keyspace: search_index)
    │    vocabulary      — term, df
    │    inverted_index  — term, doc_id, tf
    │    doc_stats       — doc_id, title, doc_length
    │    corpus_stats    — total_docs, avg_doc_length
    │
    ▼
[PySpark on YARN] search.sh "<query>"
    │  reads posting lists from Cassandra (driver)
    │  computes BM25 scores via RDD flatMap → reduceByKey
    └─ prints top-10 ranked documents
```

---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

No other local dependencies are needed — everything runs inside containers.

---

## Quick Start (fully automated)

### 1. Place the parquet file

Download a Wikipedia parquet file and copy it to:

```
app/data/parquet/m.parquet
```

### 2. Start the containers

```bash
docker compose up -d
```

This starts five services:
| Container | Role |
|---|---|
| `cluster-master` | Spark driver, Hadoop NameNode + DataNode + YARN |
| `cluster-slave-1` | Spark worker |
| `namenode` | HDFS NameNode (BDE image, for reference) |
| `datanode` | HDFS DataNode (BDE image, for reference) |
| `cassandra-server` | Cassandra database |

### 3. Run the full pipeline

```bash
docker exec -it cluster-master bash /app/app.sh
```

`app.sh` runs the complete pipeline automatically:

1. Starts SSH (needed by Hadoop start scripts)
2. Runs `start-services.sh` — configures and starts all Hadoop/YARN/Spark daemons
3. Creates the Python virtualenv and installs dependencies
4. Packs the virtualenv for YARN executor distribution
5. Runs `prepare_data.sh` — parquet → HDFS documents → `/input/data`
6. Runs `index.sh` — MapReduce indexing + Cassandra load
7. Runs a test search: `bash search.sh "machine learning"`

---

## Manual Step-by-Step

If you want to run each stage individually (useful for development or re-running a single step):

```bash
docker exec -it cluster-master bash
cd /app
```

### Start services

```bash
bash start-services.sh
```

Idempotent — safe to run multiple times. On first run it:
- Writes `yarn-site.xml` (localizer on port 8046)
- Writes `spark-defaults.conf` (`spark.yarn.archive` pointing to HDFS)
- Sets Hadoop workers to `localhost`
- Formats the HDFS NameNode if this is a fresh container
- Starts NameNode, DataNode, SecondaryNameNode, ResourceManager, NodeManager, HistoryServer
- Uploads the Spark JAR archive to HDFS (one-time, ~300 MB)

### Set up the Python environment

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
venv-pack -o .venv.tar.gz
```

### Data preparation

```bash
bash prepare_data.sh
```

- Uploads `data/parquet/m.parquet` to HDFS as `/m.parquet`
- Runs `prepare_data.py` (PySpark): samples 1 000 documents → writes `.txt` files locally
- Uploads the `.txt` files to HDFS `/data`
- Runs `prepare_input.py` (PySpark): reads `/data`, builds `/input/data` (one partition, TSV)

### Build the index

```bash
bash create_index.sh              # default input: /input/data
bash create_index.sh /input/data  # explicit path
```

Runs two Hadoop MapReduce streaming jobs:

| Pipeline | Mapper | Reducer | Output |
|---|---|---|---|
| 1 — Inverted index | `mapper1.py` | `reducer1.py` | `/indexer/index/part-00000` |
| 2 — Doc statistics | `mapper2.py` | `reducer2.py` | `/indexer/doc_stats/part-00000` |

### Load index into Cassandra

```bash
bash store_index.sh
```

Waits for Cassandra, creates the `search_index` keyspace and four tables, then batch-loads all data.

### Run both index steps together

```bash
bash index.sh
bash index.sh /input/data   # custom input path
```

### Search

```bash
bash search.sh "machine learning"
bash search.sh "military aircraft weapons"
bash search.sh "computer science algorithm"
```

Submits `query.py` to YARN (`--master yarn --deploy-mode client`).  
Prints the top-10 documents with their BM25 scores.

### Add a single document to the index (optional)

```bash
bash add_to_index.sh /path/to/12345_My_Article_Title.txt
```

The file name must follow the `<doc_id>_<doc_title>.txt` convention.  
Updates `inverted_index`, `vocabulary` (increments `df`), `doc_stats`, and `corpus_stats` in Cassandra.

---

## File Structure

```
BigDataA2/
├── docker-compose.yml
├── app/
│   ├── app.sh                  # full pipeline entry point
│   ├── start-services.sh       # starts Hadoop / YARN / Spark
│   ├── prepare_data.sh         # parquet → HDFS /data + /input/data
│   ├── prepare_data.py         # PySpark: parquet → local .txt docs
│   ├── prepare_input.py        # PySpark RDD: /data → /input/data (TSV)
│   ├── create_index.sh         # runs MapReduce pipelines
│   ├── store_index.sh          # HDFS index → Cassandra
│   ├── index.sh                # create_index.sh + store_index.sh
│   ├── search.sh               # BM25 query on YARN
│   ├── query.py                # PySpark RDD BM25 ranker
│   ├── load_to_cassandra.py    # loads HDFS index into Cassandra
│   ├── add_to_index.sh         # add a single document (optional)
│   ├── add_document.py         # single-doc indexer (optional)
│   ├── requirements.txt
│   ├── data/
│   │   └── parquet/
│   │       └── m.parquet       # ← place your parquet file here
│   └── mapreduce/
│       ├── mapper1.py          # tokenise → (term, doc_id)
│       ├── reducer1.py         # (term, doc_id)* → term \t df \t posting_list
│       ├── mapper2.py          # → (__STATS__, doc_id, title, length)
│       └── reducer2.py         # → DOC lines + CORPUS line
└── hdfs/
    ├── namenode/               # HDFS NameNode data (volume-mounted)
    └── datanode/               # HDFS DataNode data (volume-mounted)
```

---

## BM25 Formula

```
score(q, d) = Σ_{t ∈ q}  IDF(t) · tf(t,d)·(k1+1) / (tf(t,d) + k1·(1−b + b·|d|/avgdl))

IDF(t)  = log( (N − df(t) + 0.5) / (df(t) + 0.5) + 1 )

k1 = 1.5   (term-frequency saturation)
b  = 0.75  (document-length normalisation)
```

---

## Cassandra Schema

```sql
-- keyspace
CREATE KEYSPACE search_index
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- term document-frequency (for IDF)
CREATE TABLE vocabulary (
    term       TEXT PRIMARY KEY,
    df         INT
);

-- posting list (for TF lookup per document)
CREATE TABLE inverted_index (
    term    TEXT,
    doc_id  TEXT,
    tf      INT,
    PRIMARY KEY (term, doc_id)
);

-- per-document length and title (for BM25 normalisation)
CREATE TABLE doc_stats (
    doc_id      TEXT PRIMARY KEY,
    title       TEXT,
    doc_length  INT
);

-- corpus-wide statistics (for IDF and avgdl)
CREATE TABLE corpus_stats (
    id              INT PRIMARY KEY,   -- always 1
    total_docs      INT,
    avg_doc_length  DOUBLE
);
```

---

## Notes

- `trash.py` in the project root is a local exploration script that uses Pandas and is **not part of the assignment pipeline**.
- The `namenode` and `datanode` BDE containers are included in `docker-compose.yml` but the active HDFS runs inside `cluster-master` (pseudo-distributed mode). The BDE containers are kept for potential future use.
- On the first run, `start-services.sh` uploads `spark-jars.zip` (~300 MB) to HDFS. Subsequent runs skip this step.