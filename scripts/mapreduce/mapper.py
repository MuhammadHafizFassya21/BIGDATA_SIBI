#!/usr/bin/env python3
"""
Hadoop Streaming Mapper for Project BIGDATA_SIBI.
Reads 'book_id\\ttoken' from stdin.
Emits 'book_id\\ttoken\\t1' to stdout.
Validates input format and logs invalid records to stderr.
"""

import sys


def process_mapper():
    """Read lines from standard input and emit key-value pairs."""
    for line_num, line in enumerate(sys.stdin, start=1):
        line = line.rstrip("\r\n")
        if not line:
            # Skip empty lines silently
            continue

        parts = line.split("\t")
        if len(parts) != 2:
            sys.stderr.write(
                f"[MAPPER ERROR] Line {line_num}: Expected 2 tab-separated fields, "
                f"got {len(parts)} -> '{line[:50]}'\n"
            )
            continue

        book_id, token = parts[0].strip(), parts[1].strip()

        # Validation: book_id must be a non-empty digit/identifier (1, 2, 3, 4)
        if not book_id:
            sys.stderr.write(f"[MAPPER ERROR] Line {line_num}: Empty book_id\n")
            continue

        # Validation: token must not be empty
        if not token:
            sys.stderr.write(f"[MAPPER ERROR] Line {line_num}: Empty token for book {book_id}\n")
            continue

        # Emit standard 3-field intermediate tuple: book_id \t token \t 1
        sys.stdout.write(f"{book_id}\t{token}\t1\n")


if __name__ == "__main__":
    process_mapper()

