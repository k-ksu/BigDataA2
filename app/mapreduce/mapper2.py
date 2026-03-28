#!/usr/bin/env python3
from __future__ import annotations

import re
import sys
from typing import List


def tokenize(text: str) -> List[str]:
    return re.findall(r"[a-z]{2,}", text.lower())


def main() -> None:
    for raw_line in sys.stdin:
        line = raw_line.rstrip("\n")
        if not line:
            continue

        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue

        doc_id, title, text = parts

        doc_length = len(tokenize(text))

        if doc_length == 0:
            continue

        print(f"__STATS__\t{doc_id}\t{title}\t{doc_length}")


if __name__ == "__main__":
    main()
