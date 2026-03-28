#!/usr/bin/env python3

import sys


def main() -> None:
    docs: list[tuple[str, str, int]] = []
    total_length: int = 0

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        parts = line.split("\t", 3)
        if len(parts) < 4:
            continue

        _key, doc_id, title, doc_length_str = parts

        try:
            doc_length = int(doc_length_str)
        except ValueError:
            continue

        docs.append((doc_id, title, doc_length))
        total_length += doc_length

    total_docs = len(docs)

    if total_docs == 0:
        print("CORPUS\t0\t0.000000")
        return

    avg_doc_length: float = total_length / total_docs

    for doc_id, title, doc_length in docs:
        print(f"DOC\t{doc_id}\t{title}\t{doc_length}")

    print(f"CORPUS\t{total_docs}\t{avg_doc_length:.6f}")


if __name__ == "__main__":
    main()
