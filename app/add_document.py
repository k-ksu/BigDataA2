#!/usr/bin/env python3

import os
import re
import sys
import time

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

CASSANDRA_HOST = "cassandra-server"
KEYSPACE = "search_index"

CASSANDRA_MAX_RETRIES = 20
CASSANDRA_RETRY_SLEEP = 5


def tokenize(text: str) -> list:
    return re.findall(r"[a-z]{2,}", text.lower())


def compute_tf(tokens: list) -> dict:
    tf: dict = {}
    for token in tokens:
        tf[token] = tf.get(token, 0) + 1
    return tf


def parse_filename(filepath: str) -> tuple:
    basename = os.path.splitext(os.path.basename(filepath))[0]
    parts = basename.split("_", 1)
    if len(parts) == 2:
        doc_id, raw_title = parts
        doc_title = raw_title.replace("_", " ")
    else:
        doc_id = basename
        doc_title = basename
    return doc_id, doc_title


def connect_cassandra() -> tuple:
    print("Connecting to Cassandra …", flush=True)
    for attempt in range(1, CASSANDRA_MAX_RETRIES + 1):
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                protocol_version=4,
            )
            session = cluster.connect(KEYSPACE)
            print(f"  Connected (attempt {attempt}).", flush=True)
            return cluster, session
        except Exception as exc:
            print(
                f"  Attempt {attempt}/{CASSANDRA_MAX_RETRIES} failed: {exc}",
                flush=True,
            )
            time.sleep(CASSANDRA_RETRY_SLEEP)

    print("ERROR: Could not connect to Cassandra.", file=sys.stderr)
    sys.exit(1)


def doc_already_indexed(session, doc_id: str) -> bool:
    row = session.execute(
        "SELECT doc_id FROM doc_stats WHERE doc_id = %s", (doc_id,)
    ).one()
    return row is not None


def index_document(
    session,
    doc_id: str,
    doc_title: str,
    tf_map: dict,
    doc_length: int,
) -> None:

    session.execute(
        "INSERT INTO doc_stats (doc_id, title, doc_length) VALUES (%s, %s, %s)",
        (doc_id, doc_title, doc_length),
    )
    print(
        f"  doc_stats row inserted (doc_id={doc_id}, doc_length={doc_length}).",
        flush=True,
    )

    index_stmt = session.prepare(
        "INSERT INTO inverted_index (term, doc_id, tf) VALUES (?, ?, ?)"
    )
    vocab_insert_stmt = session.prepare(
        "INSERT INTO vocabulary (term, df) VALUES (?, ?)"
    )
    vocab_update_stmt = session.prepare("UPDATE vocabulary SET df = ? WHERE term = ?")

    new_terms = 0
    updated_terms = 0

    for term, tf in tf_map.items():
        session.execute(index_stmt, (term, doc_id, tf))

        row = session.execute(
            "SELECT df FROM vocabulary WHERE term = %s", (term,)
        ).one()

        if row is None:
            session.execute(vocab_insert_stmt, (term, 1))
            new_terms += 1
        else:
            session.execute(vocab_update_stmt, (row.df + 1, term))
            updated_terms += 1

    print(
        f"  Vocabulary: {new_terms} new terms, {updated_terms} existing terms updated.",
        flush=True,
    )

    corpus_row = session.execute(
        "SELECT total_docs, avg_doc_length FROM corpus_stats WHERE id = 1"
    ).one()

    if corpus_row is None:
        new_total_docs = 1
        new_avg = float(doc_length)
    else:
        old_total = corpus_row.total_docs
        old_avg = corpus_row.avg_doc_length
        new_total_docs = old_total + 1
        new_avg = (old_avg * old_total + doc_length) / new_total_docs

    session.execute(
        "INSERT INTO corpus_stats (id, total_docs, avg_doc_length) VALUES (1, %s, %s)",
        (new_total_docs, new_avg),
    )
    print(
        f"  corpus_stats updated: total_docs={new_total_docs}, "
        f"avg_doc_length={new_avg:.4f}.",
        flush=True,
    )


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python3 add_document.py <path_to_local_file>", file=sys.stderr)
        sys.exit(1)

    filepath = sys.argv[1]

    if not os.path.isfile(filepath):
        print(f"ERROR: File not found: {filepath}", file=sys.stderr)
        sys.exit(1)

    doc_id, doc_title = parse_filename(filepath)
    print(f"Document  id='{doc_id}'  title='{doc_title}'")
    print(f"File path: {filepath}")

    with open(filepath, "r", encoding="utf-8") as fh:
        text = fh.read()

    if not text.strip():
        print("ERROR: The file is empty — nothing to index.", file=sys.stderr)
        sys.exit(1)

    tokens = tokenize(text)
    tf_map = compute_tf(tokens)
    doc_length = len(tokens)

    print(
        f"Tokenisation: {doc_length} tokens, {len(tf_map)} unique terms.",
        flush=True,
    )

    cluster, session = connect_cassandra()

    try:
        if doc_already_indexed(session, doc_id):
            print(
                f"WARNING: doc_id='{doc_id}' is already present in the index.\n"
                "Proceeding will overwrite the document's own rows and "
                "double-count df values in the vocabulary.\n"
                "Aborting.  Use a unique doc_id or remove the existing entry first.",
                file=sys.stderr,
            )
            sys.exit(1)

        index_document(session, doc_id, doc_title, tf_map, doc_length)

    finally:
        cluster.shutdown()

    print(f"\nDocument '{doc_title}' (id={doc_id}) successfully added to the index.")


if __name__ == "__main__":
    main()
