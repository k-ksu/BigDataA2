#!/usr/bin/env python3

from __future__ import annotations

import math
import re
import sys
import time
from typing import Dict, List, Optional, Tuple

K1: float = 1.5
B: float = 0.75

CASSANDRA_HOST: str = "cassandra-server"
KEYSPACE: str = "search_index"


def tokenize(text: str) -> List[str]:
    return list(set(re.findall(r"[a-z]{2,}", text.lower())))


def _connect_cassandra():
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    max_retries = 30
    for attempt in range(1, max_retries + 1):
        try:
            cluster = Cluster(
                [CASSANDRA_HOST],
                load_balancing_policy=DCAwareRoundRobinPolicy(),
                protocol_version=4,
            )
            session = cluster.connect(KEYSPACE)
            return cluster, session
        except Exception as exc:  # noqa: BLE001
            if attempt == max_retries:
                print(
                    f"ERROR: Cannot connect to Cassandra after {max_retries} attempts: {exc}",
                    file=sys.stderr,
                )
                sys.exit(1)
            print(
                f"  Cassandra not ready (attempt {attempt}/{max_retries}): {exc}",
                flush=True,
            )
            time.sleep(5)


def fetch_index_data(
    query_terms: List[str],
) -> Tuple[
    int, float, List[Tuple[float, List[Tuple[str, int]]]], Dict[str, Tuple[int, str]]
]:

    cluster, session = _connect_cassandra()

    corpus_row = session.execute(
        "SELECT total_docs, avg_doc_length FROM corpus_stats WHERE id = 1"
    ).one()
    if corpus_row is None:
        print(
            "ERROR: corpus_stats is empty. Run index.sh before searching.",
            file=sys.stderr,
        )
        cluster.shutdown()
        sys.exit(1)

    N: int = corpus_row.total_docs
    avgdl: float = corpus_row.avg_doc_length

    posting_data: List[Tuple[float, List[Tuple[str, int]]]] = []
    all_doc_ids: set = set()

    for term in query_terms:
        vocab_row = session.execute(
            "SELECT df FROM vocabulary WHERE term = %s", (term,)
        ).one()
        if vocab_row is None:
            continue

        df: int = vocab_row.df

        idf: float = math.log((N - df + 0.5) / (df + 0.5) + 1.0)

        rows = session.execute(
            "SELECT doc_id, tf FROM inverted_index WHERE term = %s", (term,)
        )
        postings: List[Tuple[str, int]] = [(r.doc_id, r.tf) for r in rows]

        if postings:
            posting_data.append((idf, postings))
            all_doc_ids.update(doc_id for doc_id, _ in postings)

    doc_info: Dict[str, Tuple[int, str]] = {}
    for doc_id in all_doc_ids:
        row = session.execute(
            "SELECT doc_length, title FROM doc_stats WHERE doc_id = %s", (doc_id,)
        ).one()
        if row:
            doc_info[doc_id] = (row.doc_length, row.title)

    cluster.shutdown()
    return N, avgdl, posting_data, doc_info


def bm25_term_scores(
    entry: Tuple[float, List[Tuple[str, int]]],
    avgdl_val: float,
    doc_info_val: Dict[str, Tuple[int, str]],
) -> List[Tuple[str, float]]:
    idf, postings = entry
    results: List[Tuple[str, float]] = []

    for doc_id, tf in postings:
        dl = doc_info_val.get(doc_id, (avgdl_val, ""))[0]

        numerator = tf * (K1 + 1.0)
        denominator = tf + K1 * (1.0 - B + B * dl / avgdl_val)
        score = idf * (numerator / denominator)

        results.append((doc_id, score))

    return results


def main() -> None:
    if len(sys.argv) > 1:
        query_text = " ".join(sys.argv[1:])
    else:
        print("Enter query: ", end="", flush=True)
        query_text = sys.stdin.readline().strip()

    if not query_text:
        print("ERROR: Empty query.", file=sys.stderr)
        sys.exit(1)

    query_terms = tokenize(query_text)
    if not query_terms:
        print("ERROR: No valid tokens in query.", file=sys.stderr)
        sys.exit(1)

    print(f"\nQuery : '{query_text}'")
    print(f"Terms : {sorted(query_terms)}\n", flush=True)

    N, avgdl, posting_data, doc_info = fetch_index_data(query_terms)

    matched_terms = len(posting_data)
    print(
        f"Index : {matched_terms}/{len(query_terms)} query terms found, "
        f"{len(doc_info)} candidate documents, "
        f"N={N}, avgdl={avgdl:.1f}",
        flush=True,
    )

    if not posting_data:
        print("\nNo matching documents found for the query.")
        return

    from pyspark import SparkConf, SparkContext

    conf = SparkConf().setAppName("BM25Query")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")

    avgdl_bc = sc.broadcast(avgdl)
    doc_info_bc = sc.broadcast(doc_info)

    posting_rdd = sc.parallelize(posting_data, numSlices=max(1, len(posting_data)))

    scores_rdd = posting_rdd.flatMap(
        lambda entry: bm25_term_scores(entry, avgdl_bc.value, doc_info_bc.value)
    ).reduceByKey(lambda a, b: a + b)

    top10: List[Tuple[str, float]] = scores_rdd.sortBy(lambda x: -x[1]).take(10)

    sc.stop()

    sep = "=" * 62
    print(f"\n{sep}")
    print(f'  Top {len(top10)} results for: "{query_text}"')
    print(sep)

    if not top10:
        print("  (no results — all candidate documents scored 0)")
    else:
        info_map = doc_info
        for rank, (doc_id, score) in enumerate(top10, start=1):
            title = info_map.get(doc_id, (0, "Unknown Title"))[1]
            print(f"  {rank:2d}.  [{doc_id:>12}]  {title:<35}  BM25={score:.4f}")

    print(sep)


if __name__ == "__main__":
    main()
