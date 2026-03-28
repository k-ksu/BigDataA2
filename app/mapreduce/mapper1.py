#!/usr/bin/env python3
import re
import sys


def tokenize(text: str):
    return re.findall(r"[a-z]{2,}", text.lower())


for raw_line in sys.stdin:
    line = raw_line.rstrip("\n")
    if not line:
        continue

    parts = line.split("\t", 2)
    if len(parts) < 3:
        continue

    doc_id, _title, text = parts

    for token in tokenize(text):
        sys.stdout.write(f"{token}\t{doc_id}\n")
