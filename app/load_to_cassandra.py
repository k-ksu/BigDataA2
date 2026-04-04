#!/usr/bin/env python3
from __future__ import annotations

import os
import subprocess
import sys
import time
from typing import List, Tuple

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_index"
HADOOP_CONF_DIR = "/usr/local/hadoop/etc/hadoop"

CONCURRENCY = 64

CASSANDRA_MAX_RETRIES = 30
CASSANDRA_RETRY_SLEEP = 5


def wait_for_cassandra() -> Cluster:
    print("Waiting for Cassandra …", flush=True)
    for attempt in range(1, CASSANDRA_MAX_RETRIES + 1):
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                protocol_version=4,
            )

            cluster.connect()
            print(f"  Cassandra is ready (attempt {attempt}).", flush=True)
            return cluster
        except Exception as exc:
            print(
                f"  Attempt {attempt}/{CASSANDRA_MAX_RETRIES} failed: {exc}",
                flush=True,
            )
            time.sleep(CASSANDRA_RETRY_SLEEP)

    print("ERROR: Cassandra did not become ready in time.", file=sys.stderr)
    sys.exit(1)


def hdfs_read_glob(hdfs_glob: str) -> List[str]:
    env = {**os.environ, "HADOOP_CONF_DIR": HADOOP_CONF_DIR}

    ls = subprocess.run(
        ["hdfs", "dfs", "-ls", hdfs_glob],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )

    paths: List[str] = []
    for ls_line in ls.stdout.splitlines():
        parts = ls_line.split()
        if parts and parts[-1].startswith("/"):
            paths.append(parts[-1])

    if not paths:
        print(
            f"  WARNING: no files found matching {hdfs_glob!r}",
            file=sys.stderr,
        )
        return []

    lines: List[str] = []
    for path in paths:
        cat = subprocess.run(
            ["hdfs", "dfs", "-cat", path],
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
        )
        if cat.returncode != 0:
            print(
                f"  WARNING: could not read {path}: {cat.stderr.strip()}",
                file=sys.stderr,
            )
        else:
            lines.extend(cat.stdout.splitlines())

    return lines


def setup_schema(session) -> None:

    session.execute(
        f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """
    )
    session.set_keyspace(KEYSPACE)

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS vocabulary (
            term        TEXT PRIMARY KEY,
            df          INT
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS inverted_index (
            term    TEXT,
            doc_id  TEXT,
            tf      INT,
            PRIMARY KEY (term, doc_id)
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS doc_stats (
            doc_id      TEXT PRIMARY KEY,
            title       TEXT,
            doc_length  INT
        )
        """
    )

    session.execute(
        """
        CREATE TABLE IF NOT EXISTS corpus_stats (
            id              INT PRIMARY KEY,
            total_docs      INT,
            avg_doc_length  DOUBLE
        )
        """
    )

    print("  Schema OK.  Truncating tables for a clean load …", flush=True)
    for table in ("vocabulary", "inverted_index", "doc_stats", "corpus_stats"):
        session.execute(f"TRUNCATE {table}")


def load_index(session) -> None:
    print("Loading inverted index …", flush=True)

    lines = hdfs_read_glob("/indexer/index/part-*")
    if not lines:
        print("  No index data found.", file=sys.stderr)
        return

    vocab_stmt = session.prepare("INSERT INTO vocabulary (term, df) VALUES (?, ?)")
    index_stmt = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
    )

    vocab_params: List[Tuple] = []
    index_params: List[Tuple] = []

    for line in lines:
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue

        term, df_str, postings_str = parts

        try:
            df = int(df_str)
        except ValueError:
            continue

        vocab_params.append((term, df))

        for token in postings_str.split():
            if ":" not in token:
                continue
            doc_id, tf_str = token.rsplit(":", 1)
            try:
                index_params.append((term, doc_id, int(tf_str)))
            except ValueError:
                continue

    print(
        f"  {len(vocab_params):,} vocabulary entries, "
        f"{len(index_params):,} index entries — writing …",
        flush=True,
    )

    execute_concurrent_with_args(
        session, vocab_stmt, vocab_params, concurrency=CONCURRENCY
    )
    execute_concurrent_with_args(
        session, index_stmt, index_params, concurrency=CONCURRENCY
    )

    print("  Inverted index loaded.", flush=True)


def load_doc_stats(session) -> None:

    print("Loading document statistics …", flush=True)

    lines = hdfs_read_glob("/indexer/doc_stats/part-*")
    if not lines:
        print("  No doc_stats data found.", file=sys.stderr)
        return

    doc_stmt = session.prepare(
        "INSERT INTO doc_stats (doc_id, title, doc_length) VALUES (?, ?, ?)"
    )
    corpus_stmt = session.prepare(
        "INSERT INTO corpus_stats (id, total_docs, avg_doc_length) VALUES (?, ?, ?)"
    )

    doc_params: List[Tuple] = []

    for line in lines:
        parts = line.split("\t")

        if parts[0] == "DOC" and len(parts) >= 4:
            _, doc_id, title, doc_length_str = parts[0], parts[1], parts[2], parts[3]
            try:
                doc_params.append((doc_id, title, int(doc_length_str)))
            except ValueError:
                continue

        elif parts[0] == "CORPUS" and len(parts) >= 3:
            try:
                total_docs = int(parts[1])
                avg_doc_length = float(parts[2])
            except ValueError:
                continue
            session.execute(corpus_stmt, (1, total_docs, avg_doc_length))
            print(
                f"  Corpus stats: total_docs={total_docs}, "
                f"avg_doc_length={avg_doc_length:.2f}",
                flush=True,
            )

    print(
        f"  {len(doc_params):,} document stat rows — writing …",
        flush=True,
    )
    execute_concurrent_with_args(session, doc_stmt, doc_params, concurrency=CONCURRENCY)
    print("  Document statistics loaded.", flush=True)


def main() -> None:
    cluster = wait_for_cassandra()

    cluster.shutdown()
    cluster = Cluster(
        [CASSANDRA_HOST],
        load_balancing_policy=DCAwareRoundRobinPolicy(),
        protocol_version=4,
    )
    session = cluster.connect()

    print("Setting up Cassandra schema …", flush=True)
    setup_schema(session)

    load_index(session)
    load_doc_stats(session)

    cluster.shutdown()
    print("\nAll done!", flush=True)


if __name__ == "__main__":
    main()
