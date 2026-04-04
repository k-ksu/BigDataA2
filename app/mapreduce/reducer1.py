#!/usr/bin/env python3

import sys
from collections import defaultdict
from typing import Dict, Optional


def emit(term: str, postings: dict) -> None:
    df = len(postings)
    posting_str = " ".join(f"{did}:{tf}" for did, tf in postings.items())
    print(f"{term}\t{df}\t{posting_str}")


def main() -> None:
    current_term: Optional[str] = None
    postings: Dict[str, int] = defaultdict(int)

    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        parts = line.split("\t", 1)
        if len(parts) < 2:
            continue

        term, doc_id = parts

        if term != current_term:
            if current_term is not None:
                emit(current_term, postings)
            current_term = term
            postings = defaultdict(int)

        postings[doc_id] += 1

    if current_term is not None:
        emit(current_term, postings)


if __name__ == "__main__":
    main()
